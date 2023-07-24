/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "VeloxShuffleWriter.h"
#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "utils/ArrowTypeUtils.h"
#include "velox/vector/arrow/Bridge.h"

#include "utils/compression.h"
#include "utils/macros.h"

#include "arrow/c/bridge.h"
#include "utils/VeloxArrowUtils.h"

#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <iostream>

using namespace facebook;
using namespace facebook::velox;

namespace gluten {

// macro to rotate left an 8-bit value 'x' given the shift 's' is a 32-bit integer
// (x is left shifted by 's' modulo 8) OR (x right shifted by (8 - 's' modulo 8))
#if !defined(__x86_64__)
#define rotateLeft(x, s) (x << (s - ((s >> 3) << 3)) | x >> (8 - (s - ((s >> 3) << 3))))
#endif

// on x86 machines, _MM_HINT_T0,T1,T2 are defined as 1, 2, 3
// equivalent mapping to __builtin_prefetch hints is 3, 2, 1
#if defined(__x86_64__)
#define PREFETCHT0(ptr) _mm_prefetch(ptr, _MM_HINT_T0)
#define PREFETCHT1(ptr) _mm_prefetch(ptr, _MM_HINT_T1)
#define PREFETCHT2(ptr) _mm_prefetch(ptr, _MM_HINT_T2)
#else
#define PREFETCHT0(ptr) __builtin_prefetch(ptr, 0, 3)
#define PREFETCHT1(ptr) __builtin_prefetch(ptr, 0, 2)
#define PREFETCHT2(ptr) __builtin_prefetch(ptr, 0, 1)
#endif
// #define SKIPWRITE

#if defined(__x86_64__)
template <typename T>
std::string m128iToString(const __m128i var) {
  std::stringstream sstr;
  T values[16 / sizeof(T)];
  std::memcpy(values, &var, sizeof(values)); // See discussion below
  if (sizeof(T) == 1) {
    for (unsigned int i = 0; i < sizeof(__m128i); i++) { // C++11: Range for also possible
      sstr << std::hex << (int)values[i] << " " << std::dec;
    }
  } else {
    for (unsigned int i = 0; i < sizeof(__m128i) / sizeof(T); i++) {
      sstr << std::hex << values[i] << " " << std::dec;
    }
  }
  return sstr.str();
}
#endif

namespace {

bool vectorHasNull(const velox::VectorPtr& vp) {
  if (!vp->mayHaveNulls()) {
    return false;
  }
  return vp->countNulls(vp->nulls(), vp->size()) != 0;
}

velox::RowVectorPtr getStrippedRowVector(const velox::RowVector& rv) {
  // get new row type
  auto rowType = rv.type()->asRow();
  auto typeChildren = rowType.children();
  typeChildren.erase(typeChildren.begin());
  auto newRowType = velox::ROW(std::move(typeChildren));

  // get length
  auto length = rv.size();

  // get children
  auto children = rv.children();
  children.erase(children.begin());

  return std::make_shared<velox::RowVector>(rv.pool(), newRowType, BufferPtr(nullptr), length, std::move(children));
}

const int32_t* getFirstColumn(const velox::RowVector& rv) {
  VELOX_DCHECK(rv.childrenSize() > 0, "RowVector missing partition id column.");

  auto& firstChild = rv.childAt(0);
  VELOX_DCHECK(firstChild->type()->isInteger(), "RecordBatch field 0 should be integer");

  // first column is partition key hash value or pid
  return firstChild->asFlatVector<int32_t>()->rawValues();
}

int64_t getBatchNbytes(const arrow::RecordBatch& rb) {
  int64_t accumulated = 0L;

  for (const auto& array : rb.columns()) {
    if (array == nullptr || array->data() == nullptr) {
      continue;
    }
    for (const auto& buf : array->data()->buffers) {
      if (buf == nullptr) {
        continue;
      }
      accumulated += buf->size();
    }
  }
  return accumulated;
}

std::shared_ptr<arrow::Array> makeNullBinaryArray(std::shared_ptr<arrow::DataType> type, ShuffleBufferPool* pool) {
  std::shared_ptr<arrow::Buffer> offsetBuffer;
  size_t sizeofBinaryOffset = sizeof(arrow::LargeStringType::offset_type);
  GLUTEN_THROW_NOT_OK(pool->allocate(offsetBuffer, sizeofBinaryOffset * 2));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, sizeofBinaryOffset);
  // second value offset 0
  memset(offsetaddr + sizeofBinaryOffset, 0, sizeofBinaryOffset);
  // If it is not compressed array, null valueBuffer
  // worked, but if compress, will core dump at buffer::size(), so replace by kNullBuffer
  static std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, offsetBuffer, kNullBuffer}));
}

std::shared_ptr<arrow::Array> makeBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    std::shared_ptr<arrow::Buffer> valueBuffer,
    ShuffleBufferPool* pool) {
  if (valueBuffer == nullptr) {
    return makeNullBinaryArray(type, pool);
  }

  std::shared_ptr<arrow::Buffer> offsetBuffer;
  size_t sizeofBinaryOffset = sizeof(arrow::LargeStringType::offset_type);
  GLUTEN_THROW_NOT_OK(pool->allocate(offsetBuffer, sizeofBinaryOffset * 2));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, sizeofBinaryOffset);
  int64_t length = valueBuffer->size();
  memcpy(offsetaddr + sizeofBinaryOffset, reinterpret_cast<uint8_t*>(&length), sizeofBinaryOffset);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, offsetBuffer, valueBuffer}));
}

inline void writeInt64(std::shared_ptr<arrow::Buffer> buffer, int64_t& offset, int64_t value) {
  memcpy(buffer->mutable_data() + offset, &value, sizeof(int64_t));
  offset += sizeof(int64_t);
}

int64_t getMaxCompressedBufferSize(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::util::Codec* codec) {
  int64_t totalSize = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      totalSize += codec->MaxCompressedLen(buffer->size(), nullptr);
    }
  }
  return totalSize;
}

std::shared_ptr<arrow::RecordBatch> makeCompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> compressWriteSchema,
    ShuffleBufferPool* pool,
    arrow::util::Codec* codec,
    int32_t bufferCompressThreshold) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    std::shared_ptr<arrow::ResizableBuffer> headerBuffer;
    GLUTEN_THROW_NOT_OK(pool->allocateDirectly(headerBuffer, sizeof(uint32_t)));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(codec->compression_type());
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(0)->type(), headerBuffer, pool));
  }

  // Length buffer layout |buffers.size()|buffer1 unCompressedLength|buffer1 compressedLength| buffer2...
  std::shared_ptr<arrow::ResizableBuffer> lengthBuffer;
  GLUTEN_THROW_NOT_OK(pool->allocateDirectly(lengthBuffer, (buffers.size() * 2 + 1) * sizeof(int64_t)));
  int64_t offset = 0;
  writeInt64(lengthBuffer, offset, buffers.size());

  int64_t compressedBufferMaxSize = getMaxCompressedBufferSize(buffers, codec);
  std::shared_ptr<arrow::ResizableBuffer> valueBuffer;
  GLUTEN_THROW_NOT_OK(pool->allocateDirectly(valueBuffer, compressedBufferMaxSize));
  int64_t compressValueOffset = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      int64_t actualLength;
      if (buffer->size() >= bufferCompressThreshold) {
        writeInt64(lengthBuffer, offset, buffer->size());
        int64_t maxLength = codec->MaxCompressedLen(buffer->size(), nullptr);
        GLUTEN_ASSIGN_OR_THROW(
            actualLength,
            codec->Compress(
                buffer->size(), buffer->data(), maxLength, valueBuffer->mutable_data() + compressValueOffset));
      } else {
        // Will not compress small buffer, mark uncompressed length as -1 to indicate it is original buffer
        writeInt64(lengthBuffer, offset, -1);
        memcpy(valueBuffer->mutable_data() + compressValueOffset, buffer->data(), buffer->size());
        actualLength = buffer->size();
      }
      compressValueOffset += actualLength;
      writeInt64(lengthBuffer, offset, actualLength);
    } else {
      writeInt64(lengthBuffer, offset, 0);
      writeInt64(lengthBuffer, offset, 0);
    }
  }
  GLUTEN_THROW_NOT_OK(valueBuffer->Resize(compressValueOffset, /*shrink*/ true));
  arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(1)->type(), lengthBuffer, pool));
  arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(2)->type(), valueBuffer, pool));
  return arrow::RecordBatch::Make(compressWriteSchema, 1, {arrays});
}

// generate the new big one row several columns binary recordbatch
std::shared_ptr<arrow::RecordBatch> makeUncompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> writeSchema,
    ShuffleBufferPool* pool) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    std::shared_ptr<arrow::ResizableBuffer> headerBuffer;
    GLUTEN_THROW_NOT_OK(pool->allocateDirectly(headerBuffer, sizeof(uint32_t) * 2));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(arrow::Compression::type::UNCOMPRESSED);
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back(makeBinaryArray(writeSchema->field(0)->type(), headerBuffer, pool));
  }

  int32_t bufferNum = writeSchema->num_fields() - 1;
  for (int32_t i = 0; i < bufferNum; i++) {
    arrays.emplace_back(makeBinaryArray(writeSchema->field(i + 1)->type(), buffers[i], pool));
  }
  return arrow::RecordBatch::Make(writeSchema, 1, {arrays});
}
} // namespace

// VeloxShuffleWriter
arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxShuffleWriter::create(
    uint32_t numPartitions,
    std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
    ShuffleWriterOptions options) {
  std::shared_ptr<VeloxShuffleWriter> res(
      new VeloxShuffleWriter(numPartitions, std::move(partitionWriterCreator), std::move(options)));
  RETURN_NOT_OK(res->init());
  return res;
}

arrow::Status VeloxShuffleWriter::init() {
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#else
  supportAvx512_ = false;
#endif

  // partition number should be less than 64k
  VELOX_CHECK_LE(numPartitions_, 64 * 1024);

  // split record batch size should be less than 32k
  VELOX_CHECK_LE(options_.buffer_size, 32 * 1024);

  ARROW_ASSIGN_OR_RAISE(partitionWriter_, partitionWriterCreator_->make(this));

  ARROW_ASSIGN_OR_RAISE(partitioner_, Partitioner::make(options_.partitioning_name, numPartitions_));

  // pre-allocated buffer size for each partition, unit is row count
  // when partitioner is SinglePart, partial variables don`t need init
  if (options_.partitioning_name != "single") {
    partition2RowCount_.resize(numPartitions_);
    partition2BufferSize_.resize(numPartitions_);
    partitionBufferIdxOffset_.resize(numPartitions_);
    partition2RowOffset_.resize(numPartitions_ + 1);
  }

  partitionBufferIdxBase_.resize(numPartitions_);

  partitionCachedRecordbatch_.resize(numPartitions_);
  partitionCachedRecordbatchSize_.resize(numPartitions_);

  partitionLengths_.resize(numPartitions_);
  rawPartitionLengths_.resize(numPartitions_);

  RETURN_NOT_OK(setCompressType(options_.compression_type));

  RETURN_NOT_OK(pool_->init());
  RETURN_NOT_OK(initIpcWriteOptions());

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initIpcWriteOptions() {
  auto& ipcWriteOptions = options_.ipc_write_options;
  if (options_.prefer_evict) {
    ipcWriteOptions.memory_pool = options_.memory_pool.get();
  } else {
    if (!options_.ipc_memory_pool) {
      auto ipcMemoryPool = std::make_shared<LargeMemoryPool>(options_.memory_pool.get());
      options_.ipc_memory_pool = std::move(ipcMemoryPool);
    }
    ipcWriteOptions.memory_pool = options_.ipc_memory_pool.get();
  }
  ipcWriteOptions.use_threads = false;

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initPartitions() {
  auto simpleColumnCount = simpleColumnIndices_.size();

  partitionValidityAddrs_.resize(simpleColumnCount);
  std::for_each(partitionValidityAddrs_.begin(), partitionValidityAddrs_.end(), [this](std::vector<uint8_t*>& v) {
    v.resize(numPartitions_, nullptr);
  });

  partitionFixedWidthValueAddrs_.resize(fixedWidthColumnCount_);
  std::for_each(
      partitionFixedWidthValueAddrs_.begin(), partitionFixedWidthValueAddrs_.end(), [this](std::vector<uint8_t*>& v) {
        v.resize(numPartitions_, nullptr);
      });

  partitionBuffers_.resize(simpleColumnCount);
  std::for_each(partitionBuffers_.begin(), partitionBuffers_.end(), [this](std::vector<arrow::BufferVector>& v) {
    v.resize(numPartitions_);
  });

  partitionBinaryAddrs_.resize(binaryColumnIndices_.size());
  std::for_each(partitionBinaryAddrs_.begin(), partitionBinaryAddrs_.end(), [this](std::vector<BinaryBuf>& v) {
    v.resize(numPartitions_);
  });

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::setCompressType(arrow::Compression::type compressedType) {
  options_.codec = createArrowIpcCodec(compressedType);
  return arrow::Status::OK();
}

namespace {

std::shared_ptr<arrow::Buffer> convertToArrowBuffer(velox::BufferPtr buffer, ShuffleBufferPool* pool) {
  if (buffer == nullptr) {
    return nullptr;
  }

  std::shared_ptr<arrow::ResizableBuffer> arrowBuffer;
  GLUTEN_THROW_NOT_OK(pool->allocateDirectly(arrowBuffer, buffer->size()));
  memcpy(arrowBuffer->mutable_data(), buffer->asMutable<void>(), buffer->size());
  return arrowBuffer;
}

template <velox::TypeKind kind>
void collectFlatVectorBuffer(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    ShuffleBufferPool* pool) {
  using T = typename velox::TypeTraits<kind>::NativeType;
  auto flatVector = dynamic_cast<const velox::FlatVector<T>*>(vector);
  buffers.emplace_back(convertToArrowBuffer(flatVector->nulls(), pool));
  buffers.emplace_back(convertToArrowBuffer(flatVector->values(), pool));
}

void collectFlatVectorBufferStringView(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    ShuffleBufferPool* pool) {
  auto flatVector = dynamic_cast<const velox::FlatVector<StringView>*>(vector);
  buffers.emplace_back(convertToArrowBuffer(flatVector->nulls(), pool));

  auto rawValues = flatVector->rawValues();
  // last offset is the totalStringSize
  std::shared_ptr<arrow::ResizableBuffer> offsetBuffer;
  GLUTEN_THROW_NOT_OK(pool->allocateDirectly(offsetBuffer, sizeof(int32_t) * (flatVector->size() + 1)));
  int32_t* rawOffset = reinterpret_cast<int32_t*>(offsetBuffer->mutable_data());
  // first offset is 0
  *rawOffset++ = 0;
  int32_t offset = 0;
  for (int32_t i = 0; i < flatVector->size(); i++) {
    offset += rawValues[i].size();
    *rawOffset++ = offset;
  }
  buffers.emplace_back(offsetBuffer);
  std::shared_ptr<arrow::ResizableBuffer> valueBuffer;
  GLUTEN_THROW_NOT_OK(pool->allocateDirectly(valueBuffer, offset));
  auto raw = reinterpret_cast<char*>(valueBuffer->mutable_data());
  for (int32_t i = 0; i < flatVector->size(); i++) {
    memcpy(raw, rawValues[i].data(), rawValues[i].size());
    raw += rawValues[i].size();
  }
  buffers.emplace_back(valueBuffer);
}

template <>
void collectFlatVectorBuffer<velox::TypeKind::VARCHAR>(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    ShuffleBufferPool* pool) {
  collectFlatVectorBufferStringView(vector, buffers, pool);
}

template <>
void collectFlatVectorBuffer<velox::TypeKind::VARBINARY>(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    ShuffleBufferPool* pool) {
  collectFlatVectorBufferStringView(vector, buffers, pool);
}

} // namespace

std::shared_ptr<arrow::Buffer> VeloxShuffleWriter::generateComplexTypeBuffers(velox::RowVectorPtr vector) {
  auto arena = std::make_unique<StreamArena>(veloxPool_.get());
  auto serializer =
      serde_->createSerializer(asRowType(vector->type()), vector->size(), arena.get(), /* serdeOptions */ nullptr);
  const IndexRange allRows{0, vector->size()};
  serializer->append(vector, folly::Range(&allRows, 1));
  auto serializedSize = serializer->serializedSize();
  auto flushBuffer = complexTypeFlushBuffer_[0];
  if (flushBuffer == nullptr) {
    GLUTEN_ASSIGN_OR_THROW(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, options_.memory_pool.get()));
  } else if (serializedSize > flushBuffer->capacity()) {
    GLUTEN_THROW_NOT_OK(flushBuffer->Reserve(serializedSize));
  }

  auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 0, serializedSize);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  return valueBuffer;
}

arrow::Status VeloxShuffleWriter::split(std::shared_ptr<ColumnarBatch> cb) {
  if (options_.partitioning_name == "single") {
    auto veloxColumnBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
    VELOX_DCHECK_NOT_NULL(veloxColumnBatch);
    auto& rv = *veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    std::vector<VectorPtr> complexChildren;
    for (auto& child : rv.children()) {
      if (child->encoding() == VectorEncoding::Simple::FLAT) {
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            collectFlatVectorBuffer, child->typeKind(), child.get(), buffers, pool_.get());
      } else {
        complexChildren.emplace_back(child);
      }
    }
    if (complexChildren.size() > 0) {
      auto rowVector = std::make_shared<RowVector>(
          veloxPool_.get(), complexWriteType_, BufferPtr(nullptr), rv.size(), std::move(complexChildren));
      buffers.emplace_back(generateComplexTypeBuffers(rowVector));
    }

    auto rb = makeRecordBatch(rv.size(), buffers);
    RETURN_NOT_OK(cacheRecordBatch(0, *rb, false));
  } else if (options_.partitioning_name == "range") {
    auto compositeBatch = std::dynamic_pointer_cast<CompositeColumnarBatch>(cb);
    VELOX_DCHECK_NOT_NULL(compositeBatch);
    auto batches = compositeBatch->getBatches();
    VELOX_DCHECK_EQ(batches.size(), 2);
    auto pidBatch = VeloxColumnarBatch::from(defaultLeafVeloxMemoryPool().get(), batches[0]);
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_, partition2RowCount_));
    auto rvBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(batches[1]);
    auto& rv = *rvBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    RETURN_NOT_OK(doSplit(rv));
  } else {
    auto veloxColumnBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
    VELOX_DCHECK_NOT_NULL(veloxColumnBatch);
    auto& rv = *veloxColumnBatch->getFlattenedRowVector();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(rv);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv.size(), row2Partition_, partition2RowCount_));
      auto strippedRv = getStrippedRowVector(rv);
      RETURN_NOT_OK(initFromRowVector(*strippedRv));
      RETURN_NOT_OK(doSplit(*strippedRv));
    } else {
      RETURN_NOT_OK(initFromRowVector(rv));
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv.size(), row2Partition_, partition2RowCount_));
      RETURN_NOT_OK(doSplit(rv));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::stop() {
  EVAL_START("write", options_.thread_id)
  RETURN_NOT_OK(partitionWriter_->stop());
  if (options_.ipc_memory_pool != options_.memory_pool) {
    options_.ipc_memory_pool.reset();
  }
  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::createPartition2Row(uint32_t rowNum) {
  // calc partition2RowOffset_
  partition2RowOffset_[0] = 0;
  for (auto pid = 1; pid <= numPartitions_; ++pid) {
    partition2RowOffset_[pid] = partition2RowOffset_[pid - 1] + partition2RowCount_[pid - 1];
  }

  // calc rowOffset2RowId_
  rowOffset2RowId_.resize(rowNum);
  for (auto row = 0; row < rowNum; ++row) {
    auto pid = row2Partition_[row];
    PREFETCHT0((rowOffset2RowId_.data() + partition2RowOffset_[pid] + 32));
    rowOffset2RowId_[partition2RowOffset_[pid]++] = row;
  }
  std::transform(
      partition2RowOffset_.begin(),
      std::prev(partition2RowOffset_.end()),
      partition2RowCount_.begin(),
      partition2RowOffset_.begin(),
      [](row_offset_type x, row_offset_type y) { return x - y; });

  printPartition2Row();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::updateInputHasNull(const velox::RowVector& rv) {
  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    // check input_has_null_[col] is cheaper than GetNullCount()
    // once input_has_null_ is set to true, we didn't reset it after evict
    if (!inputHasNull_[col]) {
      auto colIdx = simpleColumnIndices_[col];
      if (vectorHasNull(rv.childAt(colIdx))) {
        inputHasNull_[col] = true;
      }
    }
  }

  printInputHasNull();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::doSplit(const velox::RowVector& rv) {
  auto rowNum = rv.size();

  RETURN_NOT_OK(createPartition2Row(rowNum));

  RETURN_NOT_OK(updateInputHasNull(rv));

  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partition2RowCount_[pid] > 0) {
      // make sure the size to be allocated is larger than the size to be filled
      // partitionBufferManager[pid]->prepareNextSplit();
      if (partition2BufferSize_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto newSize = std::max(calculatePartitionBufferSize(rv), partition2RowCount_[pid]);
        RETURN_NOT_OK(allocatePartitionBuffersWithRetry(pid, newSize));
      } else if (partitionBufferIdxBase_[pid] + partition2RowCount_[pid] > partition2BufferSize_[pid]) {
        auto newSize = std::max(calculatePartitionBufferSize(rv), partition2RowCount_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to free current buffers and allocate new
        // buffer
        if (newSize > partition2BufferSize_[pid]) {
          // if the partition size after split is already larger than
          // allocated buffer size, need reallocate
          {
            bool reuseBuffers = false;
            ARROW_ASSIGN_OR_RAISE(auto rb, createArrowRecordBatchFromBuffer(pid, /*resetBuffers = */ !reuseBuffers));
            if (rb) {
              RETURN_NOT_OK(cacheRecordBatch(pid, *rb, reuseBuffers));
            }
          } // rb destructed
          if (options_.prefer_evict) {
            // if prefer_evict is set, evict current RowVector
            RETURN_NOT_OK(evictPartition(pid));
          }
          RETURN_NOT_OK(allocatePartitionBuffersWithRetry(pid, newSize));
        } else {
          // partition size after split is smaller than buffer size.
          // reuse the buffers.
          {
            bool reuseBuffers = true;
            ARROW_ASSIGN_OR_RAISE(auto rb, createArrowRecordBatchFromBuffer(pid, /*resetBuffers = */ !reuseBuffers));
            if (rb) {
              RETURN_NOT_OK(cacheRecordBatch(pid, *rb, reuseBuffers));
            }
          } // rb destructed
          if (options_.prefer_evict) {
            // if prefer_evict is set, evict current RowVector
            RETURN_NOT_OK(evictPartition(pid));
          } else {
            RETURN_NOT_OK(resetValidityBuffers(pid));
          }
        }
      }
    }
  }

  printPartitionBuffer();

  RETURN_NOT_OK(splitRowVector(rv));

  // update partition buffer base after split
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partitionBufferIdxBase_[pid] += partition2RowCount_[pid];
  }

  printPartitionBuffer();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::splitRowVector(const velox::RowVector& rv) {
  // now start to split the RowVector
  RETURN_NOT_OK(splitFixedWidthValueBuffer(rv));
  RETURN_NOT_OK(splitValidityBuffer(rv));
  RETURN_NOT_OK(splitBinaryArray(rv));
  RETURN_NOT_OK(splitComplexType(rv));
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::splitFixedWidthValueBuffer(const velox::RowVector& rv) {
  for (auto col = 0; col < fixedWidthColumnCount_; ++col) {
    auto colIdx = simpleColumnIndices_[col];
    auto column = rv.childAt(colIdx);
    assert(column->isFlatEncoding());

    const uint8_t* srcAddr = (const uint8_t*)column->valuesAsVoid();
    const auto& dstAddrs = partitionFixedWidthValueAddrs_[col];

    switch (arrow::bit_width(arrowColumnTypes_[colIdx]->id())) {
      case 1: // arrow::BooleanType::type_id:
        RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
        break;
      case 8:
        RETURN_NOT_OK(splitFixedType<uint8_t>(srcAddr, dstAddrs));
        break;
      case 16:
        RETURN_NOT_OK(splitFixedType<uint16_t>(srcAddr, dstAddrs));
        break;
      case 32:
        RETURN_NOT_OK(splitFixedType<uint32_t>(srcAddr, dstAddrs));
        break;
      case 64: {
        if (column->type()->kind() == velox::TypeKind::TIMESTAMP) {
          RETURN_NOT_OK(splitFixedType<int128_t>(srcAddr, dstAddrs));
          break;
        } else {
#ifdef PROCESSAVX
          std::transform(
              dstAddrs.begin(),
              dstAddrs.end(),
              partitionBufferIdxBase_.begin(),
              partitionBufferIdxOffset_.begin(),
              [](uint8_t* x, row_offset_type y) { return x + y * sizeof(uint64_t); });
          for (auto pid = 0; pid < numPartitions_; pid++) {
            auto dstPidBase = reinterpret_cast<uint64_t*>(partitionBufferIdxOffset_[pid]); /*32k*/
            auto r = partition2RowOffset_[pid]; /*8k*/
            auto size = partition2RowOffset_[pid + 1];
#if 1
            for (r; r < size && (((uint64_t)dstPidBase & 0x1f) > 0); r++) {
              auto srcOffset = rowOffset2RowId_[r]; /*16k*/
              *dstPidBase = reinterpret_cast<uint64_t*>(srcAddr)[srcOffset]; /*64k*/
              _mm_prefetch(&(srcAddr)[srcOffset * sizeof(uint64_t) + 64], _MM_HINT_T2);
              dstPidBase += 1;
            }
#if 0
          for (r; r+4<size; r+=4)                              
          {                                                                                    
            auto srcOffset = rowOffset2RowId_[r];                                 /*16k*/
            __m128i srcLd = _mm_loadl_epi64((__m128i*)(&rowOffset2RowId_[r]));
            __m128i srcOffset4x = _mm_cvtepu16_epi32(srcLd);
            
            __m256i src4x = _mm256_i32gather_epi64((const long long int*)srcAddr,srcOffset4x,8);
            //_mm256_store_si256((__m256i*)dstPidBase,src4x);
            _mm_stream_si128((__m128i*)dstPidBase,src2x);
                                                         
            _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r+1]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r+2]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r+3]*sizeof(uint64_t)+64], _MM_HINT_T2);
            dstPidBase+=4;
          }
#endif
            for (r; r + 2 < size; r += 2) {
              __m128i srcOffset2x = _mm_cvtsi32_si128(*((int32_t*)(rowOffset2RowId_.data() + r)));
              srcOffset2x = _mm_shufflelo_epi16(srcOffset2x, 0x98);

              __m128i src2x = _mm_i32gather_epi64((const long long int*)srcAddr, srcOffset2x, 8);
              _mm_store_si128((__m128i*)dstPidBase, src2x);
              //_mm_stream_si128((__m128i*)dstPidBase,src2x);

              _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r] * sizeof(uint64_t) + 64], _MM_HINT_T2);
              _mm_prefetch(&(srcAddr)[(uint32_t)rowOffset2RowId_[r + 1] * sizeof(uint64_t) + 64], _MM_HINT_T2);
              dstPidBase += 2;
            }
#endif
            for (r; r < size; r++) {
              auto srcOffset = rowOffset2RowId_[r]; /*16k*/
              *dstPidBase = reinterpret_cast<const uint64_t*>(srcAddr)[srcOffset]; /*64k*/
              _mm_prefetch(&(srcAddr)[srcOffset * sizeof(uint64_t) + 64], _MM_HINT_T2);
              dstPidBase += 1;
            }
            break;
#else
          RETURN_NOT_OK(splitFixedType<uint64_t>(srcAddr, dstAddrs));
#endif
            break;
          }
        }

        case 128: // arrow::Decimal128Type::type_id
          // too bad gcc generates movdqa even we use __m128i_u data type.
          // splitFixedType<__m128i_u>(srcAddr, dstAddrs);
          {
            if (column->type()->isShortDecimal()) {
              RETURN_NOT_OK(splitFixedType<int64_t>(srcAddr, dstAddrs));
            } else if (column->type()->isLongDecimal()) {
              // assume batch size = 32k; reducer# = 4K; row/reducer = 8
              RETURN_NOT_OK(splitFixedType<int128_t>(srcAddr, dstAddrs));
            } else {
              return arrow::Status::Invalid(
                  "Column type " + schema_->field(colIdx)->type()->ToString() + " is not supported.");
            }
          }
          break;
        default:
          return arrow::Status::Invalid(
              "Column type " + schema_->field(colIdx)->type()->ToString() + " is not fixed width");
      }
    }

    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    // assume batch size = 32k; reducer# = 4K; row/reducer = 8
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      // set the last byte
      auto dstaddr = dstAddrs[pid];
      if (partition2RowCount_[pid] > 0 && dstaddr != nullptr) {
        auto r = partition2RowOffset_[pid]; /*8k*/
        auto size = partition2RowOffset_[pid + 1];
        uint32_t dstOffset = partitionBufferIdxBase_[pid];
        uint32_t dstOffsetInByte = (8 - (dstOffset & 0x7)) & 0x7;
        uint32_t dstIdxByte = dstOffsetInByte;
        uint8_t dst = dstaddr[dstOffset >> 3];
        if (pid + 1 < numPartitions_) {
          PREFETCHT1((&dstaddr[partitionBufferIdxBase_[pid + 1] >> 3]));
        }
        for (; r < size && dstIdxByte > 0; r++, dstIdxByte--) {
          auto srcOffset = rowOffset2RowId_[r]; /*16k*/
          uint8_t src = srcAddr[srcOffset >> 3];
          src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
          src = __rolb(src, 8 - dstIdxByte);
#else
        src = rotateLeft(src, (8 - dstIdxByte));
#endif
          dst = dst & src; // only take the useful bit.
        }
        dstaddr[dstOffset >> 3] = dst;
        if (r == size) {
          continue;
        }
        dstOffset += dstOffsetInByte;
        // now dst_offset is 8 aligned
        for (; r + 8 < size; r += 8) {
          uint8_t src = 0;
          auto srcOffset = rowOffset2RowId_[r]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          // PREFETCHT0((&(srcAddr)[(srcOffset >> 3) + 64]));
          dst = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 1]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 1 | 0xfd; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 2]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 2 | 0xfb; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 3]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 3 | 0xf7; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 4]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 4 | 0xef; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 5]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 5 | 0xdf; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 6]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 6 | 0xbf; // get the bit in bit 0, other bits set to 1

          srcOffset = rowOffset2RowId_[r + 7]; /*16k*/
          src = srcAddr[srcOffset >> 3];
          dst &= src >> (srcOffset & 7) << 7 | 0x7f; // get the bit in bit 0, other bits set to 1

          dstaddr[dstOffset >> 3] = dst;
          dstOffset += 8;
          //_mm_prefetch(dstaddr + (dst_offset >> 3) + 64, _MM_HINT_T0);
        }
        // last byte, set it to 0xff is ok
        dst = 0xff;
        dstIdxByte = 0;
        for (; r < size; r++, dstIdxByte++) {
          auto srcOffset = rowOffset2RowId_[r]; /*16k*/
          uint8_t src = srcAddr[srcOffset >> 3];
          src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
          src = __rolb(src, dstIdxByte);
#else
        src = rotateLeft(src, dstIdxByte);
#endif
          dst = dst & src; // only take the useful bit.
        }
        dstaddr[dstOffset >> 3] = dst;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::splitValidityBuffer(const velox::RowVector& rv) {
    for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
      auto colIdx = simpleColumnIndices_[col];
      auto column = rv.childAt(colIdx);
      if (vectorHasNull(column)) {
        auto& dstAddrs = partitionValidityAddrs_[col];
        for (auto pid = 0; pid < numPartitions_; ++pid) {
          if (partition2RowCount_[pid] > 0 && dstAddrs[pid] == nullptr) {
            // init bitmap if it's null, initialize the buffer as true
            auto newSize = std::max(partition2RowCount_[pid], (uint32_t)options_.buffer_size);
            std::shared_ptr<arrow::Buffer> validityBuffer;
            auto status = pool_->allocate(validityBuffer, arrow::bit_util::BytesForBits(newSize));
            ARROW_RETURN_NOT_OK(status);
            dstAddrs[pid] = const_cast<uint8_t*>(validityBuffer->data());
            memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
            partitionBuffers_[col][pid][kValidityBufferIndex] = std::move(validityBuffer);
          }
        }

        auto srcAddr = (const uint8_t*)(column->mutableRawNulls());
        RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
      } else {
        VsPrintLF(colIdx, " column hasn't null");
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::splitBinaryType(
      uint32_t binaryIdx, const velox::FlatVector<velox::StringView>& src, std::vector<BinaryBuf>& dst) {
    auto rawValues = src.rawValues();

    for (auto pid = 0; pid < numPartitions_; ++pid) {
      auto& binaryBuf = dst[pid];

      // use 32bit offset
      using offset_type = arrow::BinaryType::offset_type;
      auto dstOffsetBase = (offset_type*)(binaryBuf.offsetPtr) + partitionBufferIdxBase_[pid];

      auto valueOffset = binaryBuf.valueOffset;
      auto dstValuePtr = binaryBuf.valuePtr + valueOffset;
      auto capacity = binaryBuf.valueCapacity;

      auto r = partition2RowOffset_[pid];
      auto size = partition2RowOffset_[pid + 1] - r;
      auto multiply = 1;

      for (uint32_t x = 0; x < size; x++) {
        auto rowId = rowOffset2RowId_[x + r];
        auto& stringView = rawValues[rowId];
        auto stringLen = stringView.size();

        // 1. copy offset
        valueOffset = dstOffsetBase[x + 1] = valueOffset + stringLen;

        if (valueOffset >= capacity) {
          auto oldCapacity = capacity;
          (void)oldCapacity; // suppress warning
          capacity = capacity + std::max((capacity >> multiply), (uint64_t)stringLen);
          multiply = std::min(3, multiply + 1);

          auto valueBuffer = std::static_pointer_cast<arrow::ResizableBuffer>(
              partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][pid][kValueBufferIndex]);

          RETURN_NOT_OK(valueBuffer->Reserve(capacity));

          binaryBuf.valuePtr = valueBuffer->mutable_data();
          binaryBuf.valueCapacity = capacity;
          dstValuePtr = binaryBuf.valuePtr + valueOffset - stringLen;

          std::cout << "Split value buffer resized colIdx" << binaryIdx << std::endl;
          VsPrintSplit(" dst_start", dstOffsetBase[x]);
          VsPrintSplit(" dst_end", dstOffsetBase[x + 1]);
          VsPrintSplit(" old size", oldCapacity);
          VsPrintSplit(" new size", capacity);
          VsPrintSplit(" row", partitionBufferIdxBase_[pid]);
          VsPrintSplitLF(" string len", stringLen);
        }

        // 2. copy value
        memcpy(dstValuePtr, stringView.data(), stringLen);

        dstValuePtr += stringLen;
      }

      binaryBuf.valueOffset = valueOffset;
    }

    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::splitBinaryArray(const velox::RowVector& rv) {
    for (auto col = fixedWidthColumnCount_; col < simpleColumnIndices_.size(); ++col) {
      auto binaryIdx = col - fixedWidthColumnCount_;
      auto& dstAddrs = partitionBinaryAddrs_[binaryIdx];
      auto colIdx = simpleColumnIndices_[col];
      auto column = rv.childAt(colIdx);
      auto stringColumn = column->asFlatVector<velox::StringView>();
      assert(stringColumn);
      RETURN_NOT_OK(splitBinaryType(binaryIdx, *stringColumn, dstAddrs));
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::splitComplexType(const velox::RowVector& rv) {
    if (complexColumnIndices_.size() == 0) {
      return arrow::Status::OK();
    }
    auto numRows = rv.size();
    std::vector<std::vector<facebook::velox::IndexRange>> rowIndexs;
    rowIndexs.resize(numPartitions_);
    // TODO: maybe an estimated row is more reasonable
    for (auto row = 0; row < numRows; ++row) {
      auto partition = row2Partition_[row];
      if (complexTypeData_[partition] == nullptr) {
        // TODO: maybe memory issue, copy many times
        complexTypeData_[partition] = std::move(serde_->createSerializer(
            complexWriteType_, partition2RowCount_[partition], arena_.get(), /* serdeOptions */ nullptr));
      }
      rowIndexs[partition].emplace_back(IndexRange{row, 1});
    }

    std::vector<VectorPtr> childrens;
    for (size_t i = 0; i < complexColumnIndices_.size(); ++i) {
      auto colIdx = complexColumnIndices_[i];
      auto column = rv.childAt(colIdx);
      childrens.emplace_back(column);
    }
    auto rowVector = std::make_shared<RowVector>(
        veloxPool_.get(), complexWriteType_, BufferPtr(nullptr), rv.size(), std::move(childrens));
    for (auto pid = 0; pid < numPartitions_; pid++) {
      if (rowIndexs[pid].size() != 0) {
        complexTypeData_[pid]->append(rowVector, folly::Range(rowIndexs[pid].data(), rowIndexs[pid].size()));
      }
    }

    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::initColumnTypes(const velox::RowVector& rv) {
    schema_ = toArrowSchema(rv.type());

    for (size_t i = 0; i < rv.childrenSize(); ++i) {
      veloxColumnTypes_.push_back(rv.childAt(i)->type());
    }

    VsPrintSplitLF("schema_", schema_->ToString());

    // get arrow_column_types_ from schema
    ARROW_ASSIGN_OR_RAISE(arrowColumnTypes_, toShuffleWriterTypeId(schema_->fields()));

    std::vector<std::string> complexNames;
    std::vector<TypePtr> complexChildrens;

    for (size_t i = 0; i < arrowColumnTypes_.size(); ++i) {
      switch (arrowColumnTypes_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id:
          binaryColumnIndices_.push_back(i);
          break;
        case arrow::StructType::type_id:
        case arrow::MapType::type_id:
        case arrow::ListType::type_id: {
          complexColumnIndices_.push_back(i);
          complexNames.emplace_back(veloxColumnTypes_[i]->name());
          complexChildrens.emplace_back(veloxColumnTypes_[i]);
        } break;
        default:
          simpleColumnIndices_.push_back(i);
          break;
      }
    }

    fixedWidthColumnCount_ = simpleColumnIndices_.size();

    simpleColumnIndices_.insert(simpleColumnIndices_.end(), binaryColumnIndices_.begin(), binaryColumnIndices_.end());

    printColumnsInfo();

    binaryArrayEmpiricalSize_.resize(binaryColumnIndices_.size(), 0);

    inputHasNull_.resize(simpleColumnIndices_.size(), false);

    complexTypeData_.resize(numPartitions_);
    complexTypeFlushBuffer_.resize(numPartitions_);

    complexWriteType_ = std::make_shared<RowType>(std::move(complexNames), std::move(complexChildrens));

    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::initFromRowVector(const velox::RowVector& rv) {
    if (veloxColumnTypes_.empty()) {
      RETURN_NOT_OK(initColumnTypes(rv));
      RETURN_NOT_OK(initPartitions());
    }
    return arrow::Status::OK();
  }

  uint32_t VeloxShuffleWriter::calculatePartitionBufferSize(const velox::RowVector& rv) {
    uint32_t sizePerRow = 0;
    auto numRows = rv.size();
    for (size_t i = fixedWidthColumnCount_; i < simpleColumnIndices_.size(); ++i) {
      auto index = i - fixedWidthColumnCount_;
      if (binaryArrayEmpiricalSize_[index] == 0) {
        auto column = rv.childAt(simpleColumnIndices_[i]);
        auto stringViewColumn = column->asFlatVector<velox::StringView>();
        assert(stringViewColumn);

        // accumulate length
        uint64_t length = stringViewColumn->values()->size();
        for (auto& buffer : stringViewColumn->stringBuffers()) {
          length += buffer->size();
        }

        binaryArrayEmpiricalSize_[index] = length % numRows == 0 ? length / numRows : length / numRows + 1;
      }
    }

    VS_PRINT_VECTOR_MAPPING(binaryArrayEmpiricalSize_);

    sizePerRow = std::accumulate(binaryArrayEmpiricalSize_.begin(), binaryArrayEmpiricalSize_.end(), 0);

    for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
      auto colIdx = simpleColumnIndices_[col];
      // `bool(1) >> 3` gets 0, so +7
      sizePerRow += ((arrow::bit_width(arrowColumnTypes_[colIdx]->id()) + 7) >> 3);
    }

    VS_PRINTLF(sizePerRow);

    uint64_t preAllocRowCnt = options_.offheap_per_task > 0 && sizePerRow > 0
        ? options_.offheap_per_task / sizePerRow / numPartitions_ >> 2
        : options_.buffer_size;
    preAllocRowCnt = std::min(preAllocRowCnt, (uint64_t)options_.buffer_size);

    VS_PRINTLF(preallocRowCnt);

    return preAllocRowCnt;
  }

  arrow::Status VeloxShuffleWriter::allocatePartitionBuffers(uint32_t partitionId, uint32_t newSize) {
    // try to allocate new
    auto numFields = schema_->num_fields();
    assert(numFields == arrowColumnTypes_.size());

    auto fixedWidthIdx = 0;
    auto binaryIdx = 0;
    for (auto i = 0; i < numFields; ++i) {
      switch (arrowColumnTypes_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id: {
          std::shared_ptr<arrow::Buffer> offsetBuffer;
          std::shared_ptr<arrow::Buffer> validityBuffer = nullptr;
          auto valueBufSize = binaryArrayEmpiricalSize_[binaryIdx] * newSize + 1024;
          ARROW_ASSIGN_OR_RAISE(
              std::shared_ptr<arrow::Buffer> valueBuffer,
              arrow::AllocateResizableBuffer(valueBufSize, options_.memory_pool.get()));
          ARROW_RETURN_NOT_OK(pool_->allocate(offsetBuffer, newSize * sizeof(arrow::StringType::offset_type) + 1));

          // set the first offset to 0
          uint8_t* offsetaddr = offsetBuffer->mutable_data();
          memset(offsetaddr, 0, 8);

          partitionBinaryAddrs_[binaryIdx][partitionId] =
              BinaryBuf(valueBuffer->mutable_data(), offsetBuffer->mutable_data(), valueBufSize);

          auto index = fixedWidthColumnCount_ + binaryIdx;
          if (inputHasNull_[index]) {
            ARROW_RETURN_NOT_OK(pool_->allocate(validityBuffer, arrow::bit_util::BytesForBits(newSize)));
            // initialize all true once allocated
            memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
            partitionValidityAddrs_[index][partitionId] = validityBuffer->mutable_data();
          } else {
            partitionValidityAddrs_[index][partitionId] = nullptr;
          }
          partitionBuffers_[index][partitionId] = {
              std::move(validityBuffer), std::move(offsetBuffer), std::move(valueBuffer)};
          binaryIdx++;
          break;
        }
        case arrow::StructType::type_id:
        case arrow::MapType::type_id:
        case arrow::ListType::type_id:
          break;
        default: {
          std::shared_ptr<arrow::Buffer> valueBuffer;
          std::shared_ptr<arrow::Buffer> validityBuffer = nullptr;
          if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
            ARROW_RETURN_NOT_OK(pool_->allocate(valueBuffer, arrow::bit_util::BytesForBits(newSize)));
          } else if (veloxColumnTypes_[i]->isShortDecimal()) {
            ARROW_RETURN_NOT_OK(
                pool_->allocate(valueBuffer, newSize * (arrow::bit_width(arrow::Int64Type::type_id) >> 3)));
          } else if (veloxColumnTypes_[i]->kind() == TypeKind::TIMESTAMP) {
            ARROW_RETURN_NOT_OK(pool_->allocate(valueBuffer, BaseVector::byteSize<Timestamp>(newSize)));
          } else {
            ARROW_RETURN_NOT_OK(
                pool_->allocate(valueBuffer, newSize * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3)));
          }
          partitionFixedWidthValueAddrs_[fixedWidthIdx][partitionId] = valueBuffer->mutable_data();

          if (inputHasNull_[fixedWidthIdx]) {
            ARROW_RETURN_NOT_OK(pool_->allocate(validityBuffer, arrow::bit_util::BytesForBits(newSize)));
            // initialize all true once allocated
            memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
            partitionValidityAddrs_[fixedWidthIdx][partitionId] = validityBuffer->mutable_data();
          } else {
            partitionValidityAddrs_[fixedWidthIdx][partitionId] = nullptr;
          }
          partitionBuffers_[fixedWidthIdx][partitionId] = {std::move(validityBuffer), std::move(valueBuffer)};
          fixedWidthIdx++;
          break;
        }
      }
    }

    partition2BufferSize_[partitionId] = newSize;
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::allocatePartitionBuffersWithRetry(uint32_t partitionId, uint32_t newSize) {
    auto retry = 0;
    auto status = allocatePartitionBuffers(partitionId, newSize);
    while (status.IsOutOfMemory() && retry < 3) {
      // retry allocate
      ++retry;
      std::cout << status.ToString() << std::endl
                << std::to_string(retry) << " retry to allocate new buffer for partition "
                << std::to_string(partitionId) << std::endl;

      int64_t evictedSize = 0;
      RETURN_NOT_OK(evictPartitionsOnDemand(&evictedSize));
      if (evictedSize <= 0) {
        std::cout << "Failed to allocate new buffer for partition " << std::to_string(partitionId)
                  << ". No partition buffer to evict." << std::endl;
        return status;
      }

      status = allocatePartitionBuffers(partitionId, newSize);
    }

    if (status.IsOutOfMemory()) {
      std::cout << "Failed to allocate new buffer for partition " << std::to_string(partitionId) << ". Out of memory."
                << std::endl;
    }

    return status;
  }

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> VeloxShuffleWriter::createArrowIpcPayload(
      const arrow::RecordBatch& rb, bool reuseBuffers) {
    auto payload = std::make_shared<arrow::ipc::IpcPayload>();
    // Extract numRows from header column
    GLUTEN_THROW_NOT_OK(arrow::ipc::GetRecordBatchPayload(rb, options_.ipc_write_options, payload.get()));
    if (options_.codec == nullptr) {
      // Without compression, we need to perform a manual copy of the original buffers
      // so that we can reuse them for next split.
      if (reuseBuffers) {
        for (auto i = 0; i < payload->body_buffers.size(); ++i) {
          auto& buffer = payload->body_buffers[i];
          if (buffer) {
            auto memoryPool = options_.ipc_write_options.memory_pool;
            ARROW_ASSIGN_OR_RAISE(auto copy, ::arrow::AllocateResizableBuffer(buffer->size(), memoryPool));
            if (buffer->size() > 0) {
              memcpy(copy->mutable_data(), buffer->data(), static_cast<size_t>(buffer->size()));
            }
            buffer = std::move(copy);
          }
        }
      }
    }
    return payload;
  }

  arrow::Status VeloxShuffleWriter::createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) {
    ARROW_ASSIGN_OR_RAISE(auto rb, createArrowRecordBatchFromBuffer(partitionId, resetBuffers));
    if (rb) {
      RETURN_NOT_OK(cacheRecordBatch(partitionId, *rb, false));
    }
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> VeloxShuffleWriter::createArrowRecordBatchFromBuffer(
      uint32_t partitionId, bool resetBuffers) {
    if (partitionBufferIdxBase_[partitionId] <= 0) {
      return nullptr;
    }

    auto numRows = partitionBufferIdxBase_[partitionId];

    // already filled
    auto fixedWidthIdx = 0;
    auto binaryIdx = 0;
    auto numFields = schema_->num_fields();

    std::vector<std::shared_ptr<arrow::Array>> arrays(numFields);
    std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
    // one column should have 2 buffers at least, string column has 3 column buffers
    allBuffers.reserve(fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3);
    bool hasComplexType = false;
    for (int i = 0; i < numFields; ++i) {
      switch (arrowColumnTypes_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id: {
          auto buffers = partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId];
          // validity buffer
          if (buffers[kValidityBufferIndex] != nullptr) {
            buffers[kValidityBufferIndex] =
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows));
          }
          // offset buffer
          if (buffers[kOffsetBufferIndex] != nullptr) {
            buffers[kOffsetBufferIndex] =
                arrow::SliceBuffer(buffers[kOffsetBufferIndex], 0, (numRows + 1) * sizeof(int32_t));
          }
          // value buffer
          if (buffers[kValueBufferIndex] != nullptr) {
            VELOX_DCHECK_NE(buffers[kOffsetBufferIndex], nullptr);
            buffers[kValueBufferIndex] = arrow::SliceBuffer(
                buffers[kValueBufferIndex],
                0,
                reinterpret_cast<const int32_t*>(buffers[kOffsetBufferIndex]->data())[numRows]);
          }

          allBuffers.emplace_back(buffers[kValidityBufferIndex]);
          allBuffers.emplace_back(buffers[kOffsetBufferIndex]);
          allBuffers.emplace_back(buffers[kValueBufferIndex]);

          if (resetBuffers) {
            partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx][partitionId] = nullptr;
            partitionBinaryAddrs_[binaryIdx][partitionId] = BinaryBuf();
            partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId].clear();
          } else {
            // reset the offset
            partitionBinaryAddrs_[binaryIdx][partitionId].valueOffset = 0;
          }
          binaryIdx++;
          break;
        }
        case arrow::StructType::type_id:
        case arrow::MapType::type_id:
        case arrow::ListType::type_id: {
          hasComplexType = true;
        } break;
        default: {
          auto buffers = partitionBuffers_[fixedWidthIdx][partitionId];
          if (buffers[kValidityBufferIndex] != nullptr) {
            buffers[kValidityBufferIndex] =
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows));
          }
          if (buffers[1] != nullptr) {
            if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
              buffers[1] = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(numRows));
            } else if (veloxColumnTypes_[i]->isShortDecimal()) {
              buffers[1] =
                  arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(arrow::Int64Type::type_id) >> 3));
            } else if (veloxColumnTypes_[i]->kind() == TypeKind::TIMESTAMP) {
              buffers[1] = arrow::SliceBuffer(buffers[1], 0, BaseVector::byteSize<Timestamp>(numRows));
            } else {
              buffers[1] =
                  arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3));
            }
          }
          allBuffers.emplace_back(buffers[kValidityBufferIndex]);
          allBuffers.emplace_back(buffers[1]);
          if (resetBuffers) {
            partitionValidityAddrs_[fixedWidthIdx][partitionId] = nullptr;
            partitionFixedWidthValueAddrs_[fixedWidthIdx][partitionId] = nullptr;
            partitionBuffers_[fixedWidthIdx][partitionId].clear();
          }
          fixedWidthIdx++;
          break;
        }
      }
    }
    if (hasComplexType && complexTypeData_[partitionId] != nullptr) {
      auto flushBuffer = complexTypeFlushBuffer_[partitionId];
      auto serializedSize = complexTypeData_[partitionId]->serializedSize();
      if (flushBuffer == nullptr) {
        GLUTEN_ASSIGN_OR_THROW(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, options_.memory_pool.get()));
      } else if (serializedSize > flushBuffer->capacity()) {
        GLUTEN_THROW_NOT_OK(flushBuffer->Reserve(serializedSize));
      }
      auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 0, serializedSize);
      auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
      serializer::presto::PrestoOutputStreamListener listener;
      ArrowFixedSizeBufferOutputStream out(output, &listener);
      complexTypeData_[partitionId]->flush(&out);
      allBuffers.emplace_back(valueBuffer);
      complexTypeData_[partitionId] = nullptr;
    }

    return makeRecordBatch(numRows, allBuffers);
  }

  std::shared_ptr<arrow::RecordBatch> VeloxShuffleWriter::makeRecordBatch(
      uint32_t numRows, const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
    if (options_.codec == nullptr) {
      return makeUncompressedRecordBatch(numRows, buffers, writeSchema(), pool_.get());
    } else {
      TIME_NANO_START(totalCompressTime_);
      auto rb = makeCompressedRecordBatch(
          numRows,
          buffers,
          compressWriteSchema(),
          pool_.get(),
          options_.codec.get(),
          options_.buffer_compress_threshold);
      TIME_NANO_END(totalCompressTime_);
      return rb;
    }
  }

  arrow::Status VeloxShuffleWriter::cacheRecordBatch(
      uint32_t partitionId, const arrow::RecordBatch& rb, bool reuseBuffers) {
    rawPartitionLengths_[partitionId] += getBatchNbytes(rb);
    ARROW_ASSIGN_OR_RAISE(auto payload, createArrowIpcPayload(rb, reuseBuffers));
    partitionCachedRecordbatchSize_[partitionId] += payload->body_length;
    partitionCachedRecordbatch_[partitionId].push_back(std::move(payload));
    partitionBufferIdxBase_[partitionId] = 0;
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::evictFixedSize(int64_t size, int64_t * actual) {
    int64_t currentEvicted = 0L;
    auto tryCount = 0;
    while (currentEvicted < size && tryCount < 5) {
      tryCount++;
      int64_t singleCallEvicted = 0;
      RETURN_NOT_OK(evictPartitionsOnDemand(&singleCallEvicted));
      if (singleCallEvicted <= 0) {
        break;
      }
      currentEvicted += singleCallEvicted;
    }
    *actual = currentEvicted;
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::evictPartitionsOnDemand(int64_t * size) {
    if (options_.prefer_evict) {
      // evict the largest partition
      auto maxSize = 0;
      int32_t partitionToEvict = -1;
      for (auto i = 0; i < numPartitions_; ++i) {
        if (partitionCachedRecordbatchSize_[i] > maxSize) {
          maxSize = partitionCachedRecordbatchSize_[i];
          partitionToEvict = i;
        }
      }
      if (partitionToEvict != -1) {
        RETURN_NOT_OK(evictPartition(partitionToEvict));
#ifdef GLUTEN_PRINT_DEBUG
        std::cout << "Evicted partition " << std::to_string(partitionToEvict) << ", " << std::to_string(maxSize)
                  << " bytes released" << std::endl;
#endif
        *size = maxSize;
      } else {
        *size = 0;
      }
    } else {
      // Evict all cached partitions
      int64_t totalCachedSize = totalCachedPayloadSize();
      if (totalCachedPayloadSize() <= 0) {
        *size = 0;
      } else {
        RETURN_NOT_OK(evictPartition(-1));
#ifdef GLUTEN_PRINT_DEBUG
        std::cout << "Evicted all partition. " << std::to_string(totalCachedSize) << " bytes released" << std::endl;
#endif
        *size = totalCachedSize;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::resetValidityBuffers(uint32_t partitionId) {
    std::for_each(
        partitionBuffers_.begin(), partitionBuffers_.end(), [partitionId](std::vector<arrow::BufferVector>& bufs) {
          if (bufs[partitionId].size() != 0 && bufs[partitionId][0] != nullptr) {
            // initialize all true once allocated
            auto addr = bufs[partitionId][0]->mutable_data();
            memset(addr, 0xff, bufs[partitionId][0]->capacity());
          }
        });
    return arrow::Status::OK();
  }

  // TODO: Move into PartitionWriter
  arrow::Status VeloxShuffleWriter::evictPartition(int32_t partitionId) {
    RETURN_NOT_OK(partitionWriter_->evictPartition(partitionId));
    // reset validity buffer after evict
    if (partitionId == -1) {
      // Reset for all partitions
      for (auto i = 0; i < numPartitions_; ++i) {
        RETURN_NOT_OK(resetValidityBuffers(i));
      }
    } else {
      RETURN_NOT_OK(resetValidityBuffers(partitionId));
    }
    return arrow::Status::OK();
  }

} // namespace gluten
