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
#include "memory/VeloxMemoryManager.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/arrow/Bridge.h"

#include "utils/Common.h"
#include "utils/compression.h"
#include "utils/macros.h"

#include "arrow/c/bridge.h"

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

#define VELOX_SHUFFLE_WRITER_LOG_FLAG 0

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

using BinaryArrayOffsetType = arrow::BinaryType::offset_type;

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

std::shared_ptr<arrow::Array> makeNullBinaryArray(std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool) {
  size_t sizeofBinaryOffset = sizeof(arrow::LargeStringType::offset_type);
  GLUTEN_ASSIGN_OR_THROW(auto offsetBuffer, arrow::AllocateResizableBuffer(sizeofBinaryOffset << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, sizeofBinaryOffset);
  // second value offset 0
  memset(offsetaddr + sizeofBinaryOffset, 0, sizeofBinaryOffset);
  // If it is not compressed array, null valueBuffer
  // worked, but if compress, will core dump at buffer::size(), so replace by kNullBuffer
  static std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, std::move(offsetBuffer), kNullBuffer}));
}

std::shared_ptr<arrow::Array> makeBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    std::shared_ptr<arrow::Buffer> valueBuffer,
    arrow::MemoryPool* pool) {
  if (valueBuffer == nullptr) {
    return makeNullBinaryArray(type, pool);
  }

  size_t sizeofBinaryOffset = sizeof(arrow::LargeStringType::offset_type);
  GLUTEN_ASSIGN_OR_THROW(auto offsetBuffer, arrow::AllocateResizableBuffer(sizeofBinaryOffset << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, sizeofBinaryOffset);
  int64_t length = valueBuffer->size();
  memcpy(offsetaddr + sizeofBinaryOffset, reinterpret_cast<uint8_t*>(&length), sizeofBinaryOffset);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, std::move(offsetBuffer), valueBuffer}));
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

// Length buffer layout |buffers.size()|buffer1 unCompressedLength|buffer1 compressedLength| buffer2...
void getLengthBufferAndValueBufferOneByOne(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    int32_t bufferCompressThreshold,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& valueBuffer) {
  GLUTEN_ASSIGN_OR_THROW(
      lengthBuffer, arrow::AllocateResizableBuffer((buffers.size() * 2 + 1) * sizeof(int64_t), pool));
  int64_t offset = 0;
  writeInt64(lengthBuffer, offset, buffers.size());

  int64_t compressedBufferMaxSize = getMaxCompressedBufferSize(buffers, codec);
  GLUTEN_ASSIGN_OR_THROW(valueBuffer, arrow::AllocateResizableBuffer(compressedBufferMaxSize, pool));
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
}

int64_t getBuffersSize(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  int64_t totalSize = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr) {
      totalSize += buffer->size();
    }
  }
  return totalSize;
}

// Length buffer layout |buffer unCompressedLength|buffer compressedLength|buffers.size()| buffer1 size | buffer2 size
void getLengthBufferAndValueBufferStream(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    int32_t bufferCompressThreshold,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& compressedBuffer) {
  GLUTEN_ASSIGN_OR_THROW(lengthBuffer, arrow::AllocateResizableBuffer((buffers.size() + 3) * sizeof(int64_t), pool));

  auto originalBufferSize = getBuffersSize(buffers);

  if (originalBufferSize >= bufferCompressThreshold) {
    // because 64B align, uncompressedBuffer size maybe bigger than unCompressedBufferSize which is
    // getBuffersSize(buffers), then cannot use this size
    GLUTEN_ASSIGN_OR_THROW(auto uncompressedBuffer, arrow::AllocateResizableBuffer(originalBufferSize, pool));
    int64_t uncompressedSize = uncompressedBuffer->size();

    // Write metadata.
    auto lengthBufferPtr = (int64_t*)lengthBuffer->mutable_data();
    int64_t pos = 0;
    lengthBufferPtr[pos++] = uncompressedSize; // uncompressedLength
    lengthBufferPtr[pos++] = 0; // 0 for compressedLength
    lengthBufferPtr[pos++] = buffers.size();

    int64_t compressValueOffset = 0;
    for (auto& buffer : buffers) {
      // Copy all buffers into one big buffer.
      if (buffer != nullptr && buffer->size() != 0) {
        lengthBufferPtr[pos++] = buffer->size();
        memcpy(uncompressedBuffer->mutable_data() + compressValueOffset, buffer->data(), buffer->size());
        compressValueOffset += buffer->size();
      } else {
        lengthBufferPtr[pos++] = 0;
      }
    }

    // Compress the big buffer.
    int64_t maxLength = codec->MaxCompressedLen(uncompressedSize, nullptr);
    GLUTEN_ASSIGN_OR_THROW(compressedBuffer, arrow::AllocateResizableBuffer(maxLength, pool));
    GLUTEN_ASSIGN_OR_THROW(
        int64_t actualLength,
        codec->Compress(uncompressedSize, uncompressedBuffer->data(), maxLength, compressedBuffer->mutable_data()));
    GLUTEN_THROW_NOT_OK(compressedBuffer->Resize(actualLength, /*shrink*/ true));

    // Update compressedLength.
    lengthBufferPtr[1] = actualLength;
  } else {
    int64_t offset = 0;
    // mark uncompress size as -1 to mark it is uncompressed buffer
    writeInt64(lengthBuffer, offset, -1); // unCompressedBufferSize
    GLUTEN_ASSIGN_OR_THROW(compressedBuffer, arrow::AllocateResizableBuffer(originalBufferSize, pool));
    writeInt64(lengthBuffer, offset, compressedBuffer->size()); // 0 for compressLength
    writeInt64(lengthBuffer, offset, buffers.size());
    int64_t compressValueOffset = 0;
    for (auto& buffer : buffers) {
      if (buffer != nullptr && buffer->size() != 0) {
        writeInt64(lengthBuffer, offset, buffer->size());
        memcpy(compressedBuffer->mutable_data() + compressValueOffset, buffer->data(), buffer->size());
        compressValueOffset += buffer->size();
      } else {
        writeInt64(lengthBuffer, offset, 0);
      }
    }
  }
}

std::shared_ptr<arrow::RecordBatch> makeCompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> compressWriteSchema,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    int32_t bufferCompressThreshold,
    CompressionMode compressionMode) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    GLUTEN_ASSIGN_OR_THROW(auto headerBuffer, arrow::AllocateResizableBuffer(sizeof(uint32_t) + sizeof(int32_t), pool));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(codec->compression_type());
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(0)->type(), std::move(headerBuffer), pool));
  }
  std::shared_ptr<arrow::ResizableBuffer> lengthBuffer;
  std::shared_ptr<arrow::ResizableBuffer> valueBuffer;
  if (compressionMode == CompressionMode::BUFFER) {
    getLengthBufferAndValueBufferOneByOne(buffers, pool, codec, bufferCompressThreshold, lengthBuffer, valueBuffer);
  } else {
    getLengthBufferAndValueBufferStream(buffers, pool, codec, bufferCompressThreshold, lengthBuffer, valueBuffer);
  }

  arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(1)->type(), lengthBuffer, pool));
  arrays.emplace_back(makeBinaryArray(compressWriteSchema->field(2)->type(), valueBuffer, pool));
  return arrow::RecordBatch::Make(compressWriteSchema, 1, {arrays});
}

// generate the new big one row several columns binary recordbatch
std::shared_ptr<arrow::RecordBatch> makeUncompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> writeSchema,
    arrow::MemoryPool* pool) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    GLUTEN_ASSIGN_OR_THROW(auto headerBuffer, arrow::AllocateResizableBuffer(sizeof(uint32_t) + sizeof(int32_t), pool));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(arrow::Compression::type::UNCOMPRESSED);
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back(makeBinaryArray(writeSchema->field(0)->type(), std::move(headerBuffer), pool));
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
    const ShuffleWriterOptions& options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool) {
#if VELOX_SHUFFLE_WRITER_LOG_FLAG
  std::ostringstream oss;
  oss << "Velox shuffle writer created,";
  oss << " partitionNum:" << numPartitions;
  oss << " partitionWriterCreator:" << typeid(*partitionWriterCreator.get()).name();
  oss << " partitioning_name:" << options.partitioning_name;
  oss << " buffer_size:" << options.buffer_size;
  oss << " compression_type:" << (int)options.compression_type;
  oss << " codec_backend:" << (int)options.codec_backend;
  oss << " compression_mode:" << (int)options.compression_mode;
  oss << " buffered_write:" << options.buffered_write;
  oss << " write_eos:" << options.write_eos;
  oss << " partition_writer_type:" << options.partition_writer_type;
  oss << " thread_id:" << options.thread_id;
  oss << " offheap_per_task:" << options.offheap_per_task;
  LOG(INFO) << oss.str();
#endif
  std::shared_ptr<VeloxShuffleWriter> res(
      new VeloxShuffleWriter(numPartitions, partitionWriterCreator, options, veloxPool));
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
    partition2RowOffset_.resize(numPartitions_ + 1);
  }

  partitionBufferIdxBase_.resize(numPartitions_);

  partitionLengths_.resize(numPartitions_);
  rawPartitionLengths_.resize(numPartitions_);

  RETURN_NOT_OK(initIpcWriteOptions());

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initIpcWriteOptions() {
  auto& ipcWriteOptions = options_.ipc_write_options;
  ipcWriteOptions.memory_pool = payloadPool_.get();
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
  std::for_each(partitionBuffers_.begin(), partitionBuffers_.end(), [this](auto& v) { v.resize(numPartitions_); });

  partitionBinaryAddrs_.resize(binaryColumnIndices_.size());
  std::for_each(partitionBinaryAddrs_.begin(), partitionBinaryAddrs_.end(), [this](std::vector<BinaryBuf>& v) {
    v.resize(numPartitions_);
  });

  return arrow::Status::OK();
}

namespace {

std::shared_ptr<arrow::Buffer> convertToArrowBuffer(velox::BufferPtr buffer, arrow::MemoryPool* pool) {
  if (buffer == nullptr) {
    return nullptr;
  }

  GLUTEN_ASSIGN_OR_THROW(auto arrowBuffer, arrow::AllocateResizableBuffer(buffer->size(), pool));
  memcpy(arrowBuffer->mutable_data(), buffer->asMutable<void>(), buffer->size());
  return arrowBuffer;
}

template <velox::TypeKind kind>
void collectFlatVectorBuffer(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  using T = typename velox::TypeTraits<kind>::NativeType;
  auto flatVector = dynamic_cast<const velox::FlatVector<T>*>(vector);
  buffers.emplace_back(convertToArrowBuffer(flatVector->nulls(), pool));
  buffers.emplace_back(convertToArrowBuffer(flatVector->values(), pool));
}

void collectFlatVectorBufferStringView(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  auto flatVector = dynamic_cast<const velox::FlatVector<StringView>*>(vector);
  buffers.emplace_back(convertToArrowBuffer(flatVector->nulls(), pool));

  auto rawValues = flatVector->rawValues();
  // last offset is the totalStringSize
  auto offsetBufferSize = sizeof(int32_t) * (flatVector->size() + 1);
  GLUTEN_ASSIGN_OR_THROW(auto offsetBuffer, arrow::AllocateResizableBuffer(offsetBufferSize, pool));
  int32_t* rawOffset = reinterpret_cast<int32_t*>(offsetBuffer->mutable_data());
  // first offset is 0
  *rawOffset++ = 0;
  int32_t offset = 0;
  for (int32_t i = 0; i < flatVector->size(); i++) {
    offset += rawValues[i].size();
    *rawOffset++ = offset;
  }
  buffers.push_back(std::move(offsetBuffer));

  GLUTEN_ASSIGN_OR_THROW(auto valueBuffer, arrow::AllocateResizableBuffer(offset, pool));
  auto raw = reinterpret_cast<char*>(valueBuffer->mutable_data());
  for (int32_t i = 0; i < flatVector->size(); i++) {
    memcpy(raw, rawValues[i].data(), rawValues[i].size());
    raw += rawValues[i].size();
  }
  buffers.push_back(std::move(valueBuffer));
}

template <>
void collectFlatVectorBuffer<velox::TypeKind::VARCHAR>(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  collectFlatVectorBufferStringView(vector, buffers, pool);
}

template <>
void collectFlatVectorBuffer<velox::TypeKind::VARBINARY>(
    BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  collectFlatVectorBufferStringView(vector, buffers, pool);
}

} // namespace

std::shared_ptr<arrow::Buffer> VeloxShuffleWriter::generateComplexTypeBuffers(velox::RowVectorPtr vector) {
  auto arena = std::make_unique<StreamArena>(veloxPool_.get());
  auto serializer =
      serde_.createSerializer(asRowType(vector->type()), vector->size(), arena.get(), /* serdeOptions */ nullptr);
  const IndexRange allRows{0, vector->size()};
  serializer->append(vector, folly::Range(&allRows, 1));
  auto serializedSize = serializer->maxSerializedSize();
  auto flushBuffer = complexTypeFlushBuffer_[0];
  if (flushBuffer == nullptr) {
    GLUTEN_ASSIGN_OR_THROW(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, payloadPool_.get()));
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

arrow::Status VeloxShuffleWriter::split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
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
            collectFlatVectorBuffer, child->typeKind(), child.get(), buffers, payloadPool_.get());
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
    ARROW_ASSIGN_OR_RAISE(auto payload, createArrowIpcPayload(*rb, false));
    rawPartitionLengths_[0] += payload->raw_body_length;
    RETURN_NOT_OK(partitionWriter_->processPayload(0, std::move(payload)));
  } else if (options_.partitioning_name == "range") {
    auto compositeBatch = std::dynamic_pointer_cast<CompositeColumnarBatch>(cb);
    VELOX_DCHECK_NOT_NULL(compositeBatch);
    auto batches = compositeBatch->getBatches();
    VELOX_DCHECK_EQ(batches.size(), 2);
    auto pidBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[0]);
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_, partition2RowCount_));
    END_TIMING();
    auto rvBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(batches[1]);
    auto& rv = *rvBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    RETURN_NOT_OK(doSplit(rv, memLimit));
  } else {
    auto veloxColumnBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
    VELOX_DCHECK_NOT_NULL(veloxColumnBatch);
    velox::RowVectorPtr rv;
    START_TIMING(cpuWallTimingList_[CpuWallTimingFlattenRV]);
    rv = veloxColumnBatch->getFlattenedRowVector();
    END_TIMING();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), row2Partition_, partition2RowCount_));
      END_TIMING();
      auto strippedRv = getStrippedRowVector(*rv);
      RETURN_NOT_OK(initFromRowVector(*strippedRv));
      RETURN_NOT_OK(doSplit(*strippedRv, memLimit));
    } else {
      RETURN_NOT_OK(initFromRowVector(*rv));
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_, partition2RowCount_));
      END_TIMING();
      RETURN_NOT_OK(doSplit(*rv, memLimit));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::stop() {
  {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
    setSplitState(SplitState::kStop);
    RETURN_NOT_OK(partitionWriter_->stop());
    partitionBuffers_.clear();
  }

  stat();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::buildPartition2Row(uint32_t rowNum) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingBuildPartition]);

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
      [](uint32_t x, uint32_t y) { return x - y; });

  printPartition2Row();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::updateInputHasNull(const velox::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingHasNull]);

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

arrow::Status VeloxShuffleWriter::doSplit(const velox::RowVector& rv, int64_t memLimit) {
  auto rowNum = rv.size();
  RETURN_NOT_OK(buildPartition2Row(rowNum));
  RETURN_NOT_OK(updateInputHasNull(rv));

  START_TIMING(cpuWallTimingList_[CpuWallTimingIteratePartitions]);
  setSplitState(SplitState::kPreAlloc);
  // Calculate buffer size based on available offheap memory, history average bytes per row and options_.buffer_size.
  auto preAllocBufferSize = calculatePartitionBufferSize(rv, memLimit);
  RETURN_NOT_OK(preAllocPartitionBuffers(preAllocBufferSize));
  END_TIMING();

  printPartitionBuffer();

  setSplitState(SplitState::kSplit);
  RETURN_NOT_OK(splitRowVector(rv));

  printPartitionBuffer();

  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::splitRowVector(const velox::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingSplitRV]);
  // now start to split the RowVector
  RETURN_NOT_OK(splitFixedWidthValueBuffer(rv));
  RETURN_NOT_OK(splitValidityBuffer(rv));
  RETURN_NOT_OK(splitBinaryArray(rv));
  RETURN_NOT_OK(splitComplexType(rv));

  // update partition buffer base after split
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partitionBufferIdxBase_[pid] += partition2RowCount_[pid];
  }

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
          std::vector<uint8_t*> partitionBufferIdxOffset;
          partitionBufferIdxOffset.resize(numPartitions_);

          std::transform(
              dstAddrs.begin(),
              dstAddrs.end(),
              partitionBufferIdxBase_.begin(),
              partitionBufferIdxOffset.begin(),
              [](uint8_t* x, uint32_t y) { return x + y * sizeof(uint64_t); });
          for (auto pid = 0; pid < numPartitions_; pid++) {
            auto dstPidBase = reinterpret_cast<uint64_t*>(partitionBufferIdxOffset[pid]); /*32k*/
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
      auto dstOffsetBase = (BinaryArrayOffsetType*)(binaryBuf.offsetPtr) + partitionBufferIdxBase_[pid];

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
        if (arenas_[partition] == nullptr) {
          arenas_[partition] = std::make_unique<facebook::velox::StreamArena>(veloxPool_.get());
        }
        complexTypeData_[partition] = serde_.createSerializer(
            complexWriteType_, partition2RowCount_[partition], arenas_[partition].get(), /* serdeOptions */ nullptr);
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
    schema_ = toArrowSchema(rv.type(), veloxPool_.get());

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

    binaryArrayTotalSizeBytes_.resize(binaryColumnIndices_.size(), 0);

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
      calculateSimpleColumnBytes();
    }
    return arrow::Status::OK();
  }

  inline bool VeloxShuffleWriter::beyondThreshold(uint32_t partitionId, uint64_t newSize) {
    auto currentBufferSize = partition2BufferSize_[partitionId];
    return newSize > (1 + options_.buffer_realloc_threshold) * currentBufferSize ||
        newSize < (1 - options_.buffer_realloc_threshold) * currentBufferSize;
  }

  void VeloxShuffleWriter::calculateSimpleColumnBytes() {
    simpleColumnBytes_ = 0;
    for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
      auto colIdx = simpleColumnIndices_[col];
      // `bool(1) >> 3` gets 0, so +7
      simpleColumnBytes_ += ((arrow::bit_width(arrowColumnTypes_[colIdx]->id()) + 7) >> 3);
    }
  }

  uint32_t VeloxShuffleWriter::calculatePartitionBufferSize(const velox::RowVector& rv, int64_t memLimit) {
    uint32_t bytesPerRow = simpleColumnBytes_;

    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCalculateBufferSize]);
    auto numRows = rv.size();
    // Calculate average size bytes (bytes per row) for each binary array.
    std::vector<uint64_t> binaryArrayAvgBytesPerRow(binaryColumnIndices_.size());
    for (size_t i = 0; i < binaryColumnIndices_.size(); ++i) {
      auto column = rv.childAt(binaryColumnIndices_[i]);
      auto stringViewColumn = column->asFlatVector<velox::StringView>();
      assert(stringViewColumn);

      uint64_t binarySizeBytes = stringViewColumn->values()->size();
      for (auto& buffer : stringViewColumn->stringBuffers()) {
        binarySizeBytes += buffer->size();
      }

      binaryArrayTotalSizeBytes_[i] += binarySizeBytes;
      binaryArrayAvgBytesPerRow[i] = binaryArrayTotalSizeBytes_[i] / (totalInputNumRows_ + numRows);
      bytesPerRow += binaryArrayAvgBytesPerRow[i];
    }

    VS_PRINT_VECTOR_MAPPING(binaryArrayAvgBytesPerRow);

    VS_PRINTLF(bytesPerRow);

    memLimit += cachedPayloadSize();
    // make sure split buffer uses 128M memory at least, let's hardcode it here for now
    if (memLimit < kMinMemLimit)
      memLimit = kMinMemLimit;

    uint64_t preAllocRowCnt =
        memLimit > 0 && bytesPerRow > 0 ? memLimit / bytesPerRow / numPartitions_ >> 2 : options_.buffer_size;
    preAllocRowCnt = std::min(preAllocRowCnt, (uint64_t)options_.buffer_size);

    VS_PRINTLF(preAllocRowCnt);

    totalInputNumRows_ += numRows;

    return preAllocRowCnt;
  }

  arrow::Result<std::shared_ptr<arrow::ResizableBuffer>> VeloxShuffleWriter::allocateValidityBuffer(
      uint32_t col, uint32_t partitionId, uint32_t newSize) {
    if (inputHasNull_[col]) {
      ARROW_ASSIGN_OR_RAISE(
          auto validityBuffer,
          arrow::AllocateResizableBuffer(arrow::bit_util::BytesForBits(newSize), partitionBufferPool_.get()));
      // initialize all true once allocated
      memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
      partitionValidityAddrs_[col][partitionId] = validityBuffer->mutable_data();
      return validityBuffer;
    }
    partitionValidityAddrs_[col][partitionId] = nullptr;
    return nullptr;
  }

  arrow::Status VeloxShuffleWriter::updateValidityBuffers(uint32_t partitionId, uint32_t newSize, bool reset) {
    for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
      // If the validity buffer is not yet allocated, allocate and fill 0xff based on inputHasNull_.
      if (partitionValidityAddrs_[i][partitionId] == nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            partitionBuffers_[i][partitionId][kValidityBufferIndex], allocateValidityBuffer(i, partitionId, newSize));
      } else if (reset) {
        // If reset, fill 0xff to the current buffer.
        auto validityBuffer = partitionBuffers_[i][partitionId][kValidityBufferIndex];
        memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize, bool reuseBuffers) {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingAllocateBuffer]);

    // try to allocate new
    auto numFields = schema_->num_fields();
    assert(numFields == arrowColumnTypes_.size());

    auto fixedWidthIdx = 0;
    auto binaryIdx = 0;
    for (auto i = 0; i < numFields; ++i) {
      switch (arrowColumnTypes_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id: {
          std::shared_ptr<arrow::ResizableBuffer> validityBuffer{};
          std::shared_ptr<arrow::ResizableBuffer> offsetBuffer{};
          std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};

          auto columnIdx = fixedWidthColumnCount_ + binaryIdx;
          ARROW_ASSIGN_OR_RAISE(validityBuffer, allocateValidityBuffer(columnIdx, partitionId, newSize));

          auto valueBufSize = calculateValueBufferSizeForBinaryArray(binaryIdx, newSize);
          auto offsetBufSize = (newSize + 1) * sizeof(BinaryArrayOffsetType);

          auto& buffers = partitionBuffers_[columnIdx][partitionId];
          if (reuseBuffers) {
            valueBuffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffers[kValueBufferIndex]);
            RETURN_NOT_OK(valueBuffer->Resize(valueBufSize, /*shrink_to_fit=*/true));
            offsetBuffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffers[kOffsetBufferIndex]);
            RETURN_NOT_OK(offsetBuffer->Resize(offsetBufSize, /*shrink_to_fit=*/true));
          } else {
            ARROW_ASSIGN_OR_RAISE(
                valueBuffer, arrow::AllocateResizableBuffer(valueBufSize, partitionBufferPool_.get()));
            ARROW_ASSIGN_OR_RAISE(
                offsetBuffer, arrow::AllocateResizableBuffer(offsetBufSize, partitionBufferPool_.get()));
          }
          // Set the first offset to 0.
          memset(offsetBuffer->mutable_data(), 0, sizeof(BinaryArrayOffsetType));
          partitionBinaryAddrs_[binaryIdx][partitionId] =
              BinaryBuf(valueBuffer->mutable_data(), offsetBuffer->mutable_data(), valueBufSize);
          buffers = {std::move(validityBuffer), std::move(offsetBuffer), std::move(valueBuffer)};

          binaryIdx++;
          break;
        }
        case arrow::StructType::type_id:
        case arrow::MapType::type_id:
        case arrow::ListType::type_id:
          break;
        default: {
          std::shared_ptr<arrow::ResizableBuffer> validityBuffer{};
          std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};

          ARROW_ASSIGN_OR_RAISE(validityBuffer, allocateValidityBuffer(fixedWidthIdx, partitionId, newSize));

          int64_t valueBufSize = 0;
          if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
            valueBufSize = arrow::bit_util::BytesForBits(newSize);
          } else if (veloxColumnTypes_[i]->isShortDecimal()) {
            valueBufSize = newSize * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
          } else if (veloxColumnTypes_[i]->kind() == TypeKind::TIMESTAMP) {
            valueBufSize = BaseVector::byteSize<Timestamp>(newSize);
          } else {
            valueBufSize = newSize * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3);
          }

          auto& buffers = partitionBuffers_[fixedWidthIdx][partitionId];
          if (reuseBuffers) {
            valueBuffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffers[1]);
            RETURN_NOT_OK(valueBuffer->Resize(valueBufSize, /*shrink_to_fit=*/true));
          } else {
            ARROW_ASSIGN_OR_RAISE(
                valueBuffer, arrow::AllocateResizableBuffer(valueBufSize, partitionBufferPool_.get()));
          }
          partitionFixedWidthValueAddrs_[fixedWidthIdx][partitionId] = valueBuffer->mutable_data();
          buffers = {std::move(validityBuffer), std::move(valueBuffer)};

          fixedWidthIdx++;
          break;
        }
      }
    }

    partition2BufferSize_[partitionId] = newSize;
    return arrow::Status::OK();
  }

  arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> VeloxShuffleWriter::createPayloadFromBuffer(
      uint32_t partitionId, bool reuseBuffers) {
    ARROW_ASSIGN_OR_RAISE(auto rb, createArrowRecordBatchFromBuffer(partitionId, reuseBuffers));
    if (rb) {
      ARROW_ASSIGN_OR_RAISE(auto payload, createArrowIpcPayload(*rb, reuseBuffers));
      rawPartitionLengths_[partitionId] += payload->raw_body_length;
      return payload;
    }
    return nullptr;
  }

  arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> VeloxShuffleWriter::createArrowIpcPayload(
      const arrow::RecordBatch& rb, bool reuseBuffers) {
    auto payload = std::make_unique<arrow::ipc::IpcPayload>();
    // Extract numRows from header column
    GLUTEN_THROW_NOT_OK(arrow::ipc::GetRecordBatchPayload(rb, options_.ipc_write_options, payload.get()));
    if (codec_ == nullptr) {
      // Without compression, we need to perform a manual copy of the original buffers
      // so that we can reuse them for next split.
      if (reuseBuffers) {
        for (auto i = 0; i < payload->body_buffers.size(); ++i) {
          auto& buffer = payload->body_buffers[i];
          if (buffer) {
            ARROW_ASSIGN_OR_RAISE(auto copy, ::arrow::AllocateResizableBuffer(buffer->size(), payloadPool_.get()));
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

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> VeloxShuffleWriter::createArrowRecordBatchFromBuffer(
      uint32_t partitionId, bool reuseBuffers) {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);

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
          const auto& buffers = partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId];
          // validity buffer
          if (buffers[kValidityBufferIndex] != nullptr) {
            allBuffers.push_back(
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows)));
          } else {
            allBuffers.push_back(nullptr);
          }
          // offset buffer
          ARROW_RETURN_IF(
              !buffers[kOffsetBufferIndex], arrow::Status::Invalid("Offset buffer of binary array is null."));
          allBuffers.push_back(arrow::SliceBuffer(buffers[kOffsetBufferIndex], 0, (numRows + 1) * sizeof(int32_t)));
          ARROW_RETURN_IF(!buffers[kValueBufferIndex], arrow::Status::Invalid("Value buffer of binary array is null."));
          // value buffer
          allBuffers.push_back(arrow::SliceBuffer(
              buffers[kValueBufferIndex],
              0,
              reinterpret_cast<const int32_t*>(buffers[kOffsetBufferIndex]->data())[numRows]));

          if (reuseBuffers) {
            // Set the first value offset to 0.
            partitionBinaryAddrs_[binaryIdx][partitionId].valueOffset = 0;
          } else {
            // Reset all buffers.
            partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx][partitionId] = nullptr;
            partitionBinaryAddrs_[binaryIdx][partitionId] = BinaryBuf();
            partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId].clear();
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
          auto& buffers = partitionBuffers_[fixedWidthIdx][partitionId];
          // validity buffer
          if (buffers[kValidityBufferIndex] != nullptr) {
            allBuffers.push_back(
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows)));
          } else {
            allBuffers.push_back(nullptr);
          }
          // value buffer
          ARROW_RETURN_IF(!buffers[1], arrow::Status::Invalid("Value buffer of fixed-width array is null."));
          std::shared_ptr<arrow::Buffer> valueBuffer;
          if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
            valueBuffer = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(numRows));
          } else if (veloxColumnTypes_[i]->isShortDecimal()) {
            valueBuffer =
                arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(arrow::Int64Type::type_id) >> 3));
          } else if (veloxColumnTypes_[i]->kind() == TypeKind::TIMESTAMP) {
            valueBuffer = arrow::SliceBuffer(buffers[1], 0, BaseVector::byteSize<Timestamp>(numRows));
          } else {
            valueBuffer =
                arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3));
          }
          allBuffers.push_back(std::move(valueBuffer));
          if (!reuseBuffers) {
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
      auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
      if (flushBuffer == nullptr) {
        GLUTEN_ASSIGN_OR_THROW(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, payloadPool_.get()));
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
      arenas_[partitionId] = nullptr;
    }

    if (!reuseBuffers) {
      partition2BufferSize_[partitionId] = 0;
    }
    partitionBufferIdxBase_[partitionId] = 0;

    return makeRecordBatch(numRows, allBuffers);
  }

  std::shared_ptr<arrow::RecordBatch> VeloxShuffleWriter::makeRecordBatch(
      uint32_t numRows, const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingMakeRB]);
    if (codec_ == nullptr) {
      return makeUncompressedRecordBatch(numRows, buffers, writeSchema(), payloadPool_.get());
    } else {
      TIME_NANO_START(totalCompressTime_);
      auto rb = makeCompressedRecordBatch(
          numRows,
          buffers,
          compressWriteSchema(),
          payloadPool_.get(),
          codec_.get(),
          options_.buffer_compress_threshold,
          options_.compression_mode);
      TIME_NANO_END(totalCompressTime_);
      return rb;
    }
  }

  arrow::Status VeloxShuffleWriter::evictFixedSize(int64_t size, int64_t * actual) {
    int64_t currentEvicted = 0;

    // If OOM happens during stop(), the reclaim order is shrink->spill,
    // because the partition buffers will be freed soon.
    if (currentEvicted < size && shrinkBeforeSpill()) {
      ARROW_ASSIGN_OR_RAISE(auto shrunken, shrinkPartitionBuffers());
      currentEvicted += shrunken;
    }

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

    // If OOM happens during binary buffers resize, the reclaim order is spill->shrink,
    // because the partition buffers can be reused.
    if (currentEvicted < size && shrinkAfterSpill()) {
      ARROW_ASSIGN_OR_RAISE(auto shrunken, shrinkPartitionBuffers());
      currentEvicted += shrunken;
    }

    *actual = currentEvicted;
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::evictPartitionsOnDemand(int64_t * size) {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingEvictPartition]);
    // Evict all cached partitions
    auto beforeEvict = cachedPayloadSize();
    if (beforeEvict == 0) {
      *size = 0;
    } else {
      RETURN_NOT_OK(partitionWriter_->spill());
      if (auto afterEvict = cachedPayloadSize()) {
        if (splitState_ != SplitState::kPreAlloc && splitState_ != SplitState::kStop) {
          // Apart from kPreAlloc and kStop states, spill should not be triggered by allocating payload buffers. All
          // cached data should be evicted.
          return arrow::Status::Invalid(
              "Not all cached payload evicted." + std::to_string(afterEvict) + " bytes remains.");
        }
        *size = beforeEvict - afterEvict;
      } else {
        DLOG(INFO) << "Evicted all partitions. " << std::to_string(beforeEvict) << " bytes released" << std::endl;
        *size = beforeEvict;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::resizePartitionBuffer(uint32_t partitionId, int64_t newSize) {
    for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
      auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
      auto& buffers = partitionBuffers_[i][partitionId];

      // resize validity
      if (buffers[kValidityBufferIndex]) {
        auto& validityBuffer = buffers[kValidityBufferIndex];
        auto filled = validityBuffer->capacity();
        RETURN_NOT_OK(validityBuffer->Resize(arrow::bit_util::BytesForBits(newSize)));
        partitionValidityAddrs_[i][partitionId] = validityBuffer->mutable_data();

        // If newSize is larger, fill 1 to the newly allocated bytes.
        if (validityBuffer->capacity() > filled) {
          memset(validityBuffer->mutable_data() + filled, 0xff, validityBuffer->capacity() - filled);
        }
      }

      // shrink value buffer if fixed-width, offset & value buffers if binary
      switch (columnType) {
        // binary types
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id: {
          auto& offsetBuffer = buffers[kOffsetBufferIndex];
          ARROW_RETURN_IF(!offsetBuffer, arrow::Status::Invalid("Offset buffer of binary array is null."));
          RETURN_NOT_OK(offsetBuffer->Resize((newSize + 1) * sizeof(BinaryArrayOffsetType)));

          auto binaryIdx = i - fixedWidthColumnCount_;
          auto& binaryBuf = partitionBinaryAddrs_[binaryIdx][partitionId];
          auto& valueBuffer = buffers[kValueBufferIndex];
          ARROW_RETURN_IF(!valueBuffer, arrow::Status::Invalid("Value buffer of binary array is null."));
          auto binaryNewSize = calculateValueBufferSizeForBinaryArray(binaryIdx, newSize);
          auto valueBufferSize = std::max(binaryBuf.valueOffset, binaryNewSize);
          RETURN_NOT_OK(valueBuffer->Resize(valueBufferSize));

          binaryBuf = BinaryBuf(
              valueBuffer->mutable_data(), offsetBuffer->mutable_data(), valueBufferSize, binaryBuf.valueOffset);
          break;
        }
        default: { // fixed-width types
          uint64_t valueBufferSize = 0;
          auto columnIndex = simpleColumnIndices_[i];
          if (arrowColumnTypes_[columnIndex]->id() == arrow::BooleanType::type_id) {
            valueBufferSize = arrow::bit_util::BytesForBits(newSize);
          } else if (veloxColumnTypes_[columnIndex]->isShortDecimal()) {
            valueBufferSize = newSize * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
          } else if (veloxColumnTypes_[columnIndex]->kind() == TypeKind::TIMESTAMP) {
            valueBufferSize = BaseVector::byteSize<Timestamp>(newSize);
          } else {
            valueBufferSize = newSize * (arrow::bit_width(arrowColumnTypes_[columnIndex]->id()) >> 3);
          }
          auto& valueBuffer = buffers[1];
          ARROW_RETURN_IF(!valueBuffer, arrow::Status::Invalid("Value buffer of fixed-width array is null."));
          RETURN_NOT_OK(valueBuffer->Resize(valueBufferSize));
          partitionFixedWidthValueAddrs_[i][partitionId] = valueBuffer->mutable_data();
          break;
        }
      }
    }
    partition2BufferSize_[partitionId] = newSize;
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::shrinkPartitionBuffer(uint32_t partitionId) {
    auto bufferSize = partition2BufferSize_[partitionId];
    if (bufferSize == 0) {
      return arrow::Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(auto newSize, sizeAfterShrink(partitionId));
    if (newSize > bufferSize) {
      std::stringstream invalid;
      invalid << "Cannot shrink to larger size. Partition: " << partitionId << ", before shrink: " << bufferSize
              << ", after shrink" << newSize;
      return arrow::Status::Invalid(invalid.str());
    }
    if (newSize == bufferSize) {
      return arrow::Status::OK();
    }
    if (newSize == 0) {
      return resetPartitionBuffer(partitionId);
    }
    return resizePartitionBuffer(partitionId, newSize);
  }

  arrow::Result<int64_t> VeloxShuffleWriter::shrinkPartitionBuffers() {
    auto beforeShrink = partitionBufferPool_->bytes_allocated();
    if (beforeShrink == 0) {
      return 0;
    }
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      RETURN_NOT_OK(shrinkPartitionBuffer(pid));
    }
    auto shrunken = beforeShrink - partitionBufferPool_->bytes_allocated();
    DLOG(INFO) << shrunken << " bytes released from shrinking.";
    return shrunken;
  }

  uint64_t VeloxShuffleWriter::calculateValueBufferSizeForBinaryArray(uint32_t binaryIdx, int64_t newSize) {
    return (binaryArrayTotalSizeBytes_[binaryIdx] + totalInputNumRows_ - 1) / totalInputNumRows_ * newSize + 1024;
  }

  void VeloxShuffleWriter::stat() const {
#if VELOX_SHUFFLE_WRITER_LOG_FLAG
    for (int i = CpuWallTimingBegin; i != CpuWallTimingEnd; ++i) {
      std::ostringstream oss;
      auto& timing = cpuWallTimingList_[i];
      oss << "Velox shuffle writer stat:" << CpuWallTimingName((CpuWallTimingType)i);
      oss << " " << timing.toString();
      if (timing.count > 0) {
        oss << " wallNanos-avg:" << timing.wallNanos / timing.count;
        oss << " cpuNanos-avg:" << timing.cpuNanos / timing.count;
      }
      LOG(INFO) << oss.str();
    }
#endif
  }

  arrow::Status VeloxShuffleWriter::resetPartitionBuffer(uint32_t partitionId) {
    // Reset fixed-width partition buffers
    for (auto i = 0; i < fixedWidthColumnCount_; ++i) {
      partitionValidityAddrs_[i][partitionId] = nullptr;
      partitionFixedWidthValueAddrs_[i][partitionId] = nullptr;
      partitionBuffers_[i][partitionId].clear();
    }

    // Reset binary partition buffers
    for (auto i = 0; i < binaryColumnIndices_.size(); ++i) {
      auto binaryIdx = i + fixedWidthColumnCount_;
      partitionValidityAddrs_[binaryIdx][partitionId] = nullptr;
      partitionBinaryAddrs_[i][partitionId] = BinaryBuf();
      partitionBuffers_[binaryIdx][partitionId].clear();
    }

    partition2BufferSize_[partitionId] = 0;
    return arrow::Status::OK();
  }

  const uint64_t VeloxShuffleWriter::cachedPayloadSize() const {
    return payloadPool_->bytes_allocated();
  }

  arrow::Status VeloxShuffleWriter::evictPartitionBuffer(uint32_t partitionId, uint32_t newSize, bool reuseBuffers) {
    ARROW_ASSIGN_OR_RAISE(auto payload, createPayloadFromBuffer(partitionId, reuseBuffers));
    if (payload) {
      RETURN_NOT_OK(evictPayload(partitionId, std::move(payload)));
    }
    return arrow::Status::OK();
  }

  arrow::Status VeloxShuffleWriter::evictPayload(
      uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) {
    return partitionWriter_->processPayload(partitionId, std::move(payload));
  }

  bool VeloxShuffleWriter::shrinkBeforeSpill() const {
    return options_.partitioning_name != "single" && splitState_ == SplitState::kStop;
  }

  bool VeloxShuffleWriter::shrinkAfterSpill() const {
    return options_.partitioning_name != "single" &&
        (splitState_ == SplitState::kSplit || splitState_ == SplitState::kInit);
  }

  arrow::Result<uint32_t> VeloxShuffleWriter::sizeAfterShrink(uint32_t partitionId) const {
    if (splitState_ == SplitState::kSplit) {
      return partitionBufferIdxBase_[partitionId] + partition2RowCount_[partitionId];
    }
    if (splitState_ == kInit || splitState_ == SplitState::kStop) {
      return partitionBufferIdxBase_[partitionId];
    }
    return arrow::Status::Invalid("Cannot shrink partition buffers in SplitState: " + std::to_string(splitState_));
  }

  arrow::Status VeloxShuffleWriter::preAllocPartitionBuffers(uint32_t preAllocBufferSize) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (partition2RowCount_[pid] > 0) {
        auto newSize = std::max(preAllocBufferSize, partition2RowCount_[pid]);
        // Make sure the size to be allocated is larger than the size to be filled.
        if (partition2BufferSize_[pid] == 0) {
          // Allocate buffer if it's not yet allocated.
          RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize, false));
        } else if (beyondThreshold(pid, newSize)) {
          if (newSize <= partitionBufferIdxBase_[pid]) {
            // If the newSize is smaller, cache the buffered data and reuse and shrink the buffer.
            RETURN_NOT_OK(evictPartitionBuffer(pid, newSize, true));
            RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize, true));
          } else {
            // If the newSize is larger, check if alreadyFilled + toBeFilled <= newSize
            if (partitionBufferIdxBase_[pid] + partition2RowCount_[pid] <= newSize) {
              // If so, keep the data in buffers and resize buffers.
              RETURN_NOT_OK(resizePartitionBuffer(pid, newSize)); // resize
              // Because inputHasNull_ is updated every time split is called, and resizePartitionBuffer won't allocate
              // validity buffer.
              RETURN_NOT_OK(updateValidityBuffers(pid, newSize, false));
            } else {
              // Otherwise cache the buffered data.
              // If newSize <= allocated buffer size, reuse and shrink the buffer.
              // Else free and allocate new buffers.
              bool reuseBuffers = newSize <= partition2BufferSize_[pid];
              RETURN_NOT_OK(evictPartitionBuffer(pid, newSize, reuseBuffers));
              RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize, reuseBuffers));
            }
          }
        } else if (partitionBufferIdxBase_[pid] + partition2RowCount_[pid] > partition2BufferSize_[pid]) {
          // If the size to be filled + already filled > the buffer size, need to free current buffers and allocate new
          // buffer.
          if (newSize > partition2BufferSize_[pid]) {
            // If the partition size after split is already larger than allocated buffer size, need reallocate.
            RETURN_NOT_OK(evictPartitionBuffer(pid, newSize, false));
            RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize, false));
          } else {
            // Partition size after split is smaller than buffer size. Reuse the buffers.
            RETURN_NOT_OK(evictPartitionBuffer(pid, newSize, true));
            // Reset validity buffer for reuse. Allocate based on inputHasNull_ if it's null.
            RETURN_NOT_OK(updateValidityBuffers(pid, newSize, true));
          }
        } else {
          // Otherwise keep on filling, but allocate partition buffers based on inputHasNull_ if it's null.
          RETURN_NOT_OK(updateValidityBuffers(pid, newSize, false));
        }
      }
    }
    return arrow::Status::OK();
  }
} // namespace gluten
