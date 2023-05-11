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

#include "shuffle/ArrowShuffleWriter.h"

#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "ArrowShuffleWriter.h"
#include "compute/ProtobufUtils.h"
#include "utils/compression.h"
#include "utils/macros.h"

namespace gluten {

using arrow::internal::checked_cast;

#ifndef SPLIT_BUFFER_SIZE
// by default, allocate 8M block, 2M page size
#define SPLIT_BUFFER_SIZE 16 * 1024 * 1024
#endif

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

ShuffleWriterOptions ShuffleWriterOptions::defaults() {
  return ShuffleWriterOptions();
}

// ----------------------------------------------------------------------
// ArrowShuffleWriter

arrow::Result<std::shared_ptr<ArrowShuffleWriter>> ArrowShuffleWriter::create(
    uint32_t numPartitions,
    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
    ShuffleWriterOptions options) {
  std::shared_ptr<ArrowShuffleWriter> res(
      new ArrowShuffleWriter(numPartitions, std::move(partitionWriterCreator), std::move(options)));
  RETURN_NOT_OK(res->init());
  return res;
}
arrow::Status ArrowShuffleWriter::initColumnType() {
  ARROW_ASSIGN_OR_RAISE(columnTypeId_, toShuffleWriterTypeId(schema_->fields()));
  std::vector<int32_t> binaryArrayIdx;

  for (size_t i = 0; i < columnTypeId_.size(); ++i) {
    switch (columnTypeId_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        binaryArrayIdx.push_back(i);
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        listArrayIdx_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
      default:
        arrayIdx_.push_back(i);
        break;
    }
  }
  fixedWidthColCnt_ = arrayIdx_.size();

  arrayIdx_.insert(arrayIdx_.end(), binaryArrayIdx.begin(), binaryArrayIdx.end());

  uint32_t arrayCnt = arrayIdx_.size();

  partitionValidityAddrs_.resize(arrayCnt);
  partitionFixedWidthValueAddrs_.resize(fixedWidthColCnt_);
  partitionBuffers_.resize(arrayCnt);
  binaryArrayEmpiricalSize_.resize(arrayCnt - fixedWidthColCnt_, 0);
  inputHasNull_.resize(arrayCnt, false);
  partitionBinaryAddrs_.resize(binaryArrayIdx.size());

  std::for_each(partitionValidityAddrs_.begin(), partitionValidityAddrs_.end(), [this](std::vector<uint8_t*>& v) {
    v.resize(numPartitions_, nullptr);
  });

  std::for_each(
      partitionFixedWidthValueAddrs_.begin(), partitionFixedWidthValueAddrs_.end(), [this](std::vector<uint8_t*>& v) {
        v.resize(numPartitions_, nullptr);
      });

  std::for_each(partitionBuffers_.begin(), partitionBuffers_.end(), [this](std::vector<arrow::BufferVector>& v) {
    v.resize(numPartitions_);
  });

  std::for_each(partitionBinaryAddrs_.begin(), partitionBinaryAddrs_.end(), [this](std::vector<BinaryBuff>& v) {
    v.resize(numPartitions_);
  });

  partitionListBuilders_.resize(listArrayIdx_.size());
  for (size_t i = 0; i < listArrayIdx_.size(); ++i) {
    partitionListBuilders_[i].resize(numPartitions_);
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::init() {
  supportAvx512_ = false;
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#endif
  // partition number should be less than 64k
  ARROW_CHECK_LE(numPartitions_, 64 * 1024);
  // split record batch size should be less than 32k
  ARROW_CHECK_LE(options_.buffer_size, 32 * 1024);

  ARROW_ASSIGN_OR_RAISE(partitionWriter_, partitionWriterCreator_->make(this));

  ARROW_ASSIGN_OR_RAISE(partitioner_, Partitioner::make(options_.partitioning_name, numPartitions_));

  // when partitioner is SinglePart, partial variables don`t need init
  if (options_.partitioning_name != "single") {
    // pre-computed row count for each partition after the record batch split
    partitionIdCnt_.resize(numPartitions_);
    // pre-allocated buffer size for each partition, unit is row count
    partitionBufferSize_.resize(numPartitions_);
    // the offset of each partition during record batch split
    partitionBufferIdxOffset_.resize(numPartitions_);
    reducerOffsetOffset_.resize(numPartitions_ + 1);
  }

  // start index for each partition when new record batch starts to split
  partitionBufferIdxBase_.resize(numPartitions_);

  partitionCachedRecordbatch_.resize(numPartitions_);
  partitionCachedRecordbatchSize_.resize(numPartitions_);
  partitionLengths_.resize(numPartitions_);
  rawPartitionLengths_.resize(numPartitions_);

  RETURN_NOT_OK(setCompressType(options_.compression_type));

  auto& ipcWriteOptions = options_.ipc_write_options;
  ipcWriteOptions.memory_pool = options_.memory_pool.get();
  ipcWriteOptions.use_threads = false;

  // initialize tiny batch write options
  tinyBachWriteOptions_ = ipcWriteOptions;
  tinyBachWriteOptions_.codec = nullptr;

  // Allocate first buffer for split reducer
  // when partitioner is SinglePart, don`t need init combine_buffer_
  if (options_.partitioning_name != "single") {
    ARROW_ASSIGN_OR_RAISE(combineBuffer_, arrow::AllocateResizableBuffer(0, options_.memory_pool.get()));
    RETURN_NOT_OK(combineBuffer_->Resize(0, /*shrink_to_fit =*/false));
  }

  return arrow::Status::OK();
}
arrow::Status ArrowShuffleWriter::allocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size) {
  // if size is already larger than buffer pool size, allocate it directly
  // make size 64byte aligned
  auto reminder = size & 0x3f;
  size += (64 - reminder) & ((reminder == 0) - 1);
  if (size > SPLIT_BUFFER_SIZE) {
    ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(size, options_.memory_pool.get()));
    return arrow::Status::OK();
  } else if (combineBuffer_->capacity() - combineBuffer_->size() < size) {
    // memory pool is not enough
    ARROW_ASSIGN_OR_RAISE(
        combineBuffer_, arrow::AllocateResizableBuffer(SPLIT_BUFFER_SIZE, options_.memory_pool.get()));
    RETURN_NOT_OK(combineBuffer_->Resize(0, /*shrink_to_fit = */ false));
  }
  buffer = arrow::SliceMutableBuffer(combineBuffer_, combineBuffer_->size(), size);

  RETURN_NOT_OK(combineBuffer_->Resize(combineBuffer_->size() + size, /*shrink_to_fit = */ false));
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::setCompressType(arrow::Compression::type compressedType) {
  ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec, createArrowIpcCodec(compressedType));
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::split(ColumnarBatch* batch) {
  EVAL_START("split", options_.thread_id)
  ARROW_ASSIGN_OR_RAISE(
      auto rb, arrow::ImportRecordBatch(batch->exportArrowArray().get(), batch->exportArrowSchema().get()));
  if (!partitioner_->hasPid()) {
    if (schema_ == nullptr) {
      schema_ = rb->schema();
      RETURN_NOT_OK(initColumnType());
    }
  }

  if (options_.partitioning_name == "single") {
    RETURN_NOT_OK(cacheRecordBatch(0, *rb));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto pid_arr, getFirstColumn(*rb));
    RETURN_NOT_OK(partitioner_->compute(pid_arr, rb->num_rows(), partitionId_, partitionIdCnt_));
    if (partitioner_->hasPid()) {
      ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb->RemoveColumn(0));
      if (schema_ == nullptr) {
        schema_ = remove_pid->schema();
        RETURN_NOT_OK(initColumnType());
      }
      RETURN_NOT_OK(doSplit(*remove_pid));
    } else {
      RETURN_NOT_OK(doSplit(*rb));
    }
  }
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::stop() {
  EVAL_START("write", options_.thread_id)
  RETURN_NOT_OK(partitionWriter_->stop());
  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

namespace {
int64_t batchNbytes(const arrow::RecordBatch& batch) {
  int64_t accumulated = 0L;

  for (const auto& array : batch.columns()) {
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
} // anonymous namespace

arrow::Status ArrowShuffleWriter::cacheRecordBatch(int32_t partitionId, const arrow::RecordBatch& batch) {
  int64_t rawSize = batchNbytes(batch);
  rawPartitionLengths_[partitionId] += rawSize;
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
  int64_t batchCompressTime = 0;
#ifndef SKIPCOMPRESS
  if (batch.num_rows() <= (uint32_t)options_.batch_compress_threshold) {
    TIME_NANO_OR_RAISE(
        batchCompressTime, arrow::ipc::GetRecordBatchPayload(batch, tinyBachWriteOptions_, payload.get()));
  } else {
    TIME_NANO_OR_RAISE(
        batchCompressTime, arrow::ipc::GetRecordBatchPayload(batch, options_.ipc_write_options, payload.get()));
  }
#else
  // for test reason
  TIME_NANO_OR_RAISE(
      batch_compress_time, arrow::ipc::GetRecordBatchPayload(*batch, tiny_bach_write_options_, payload.get()));
#endif

  totalCompressTime_ += batchCompressTime;
  partitionCachedRecordbatchSize_[partitionId] += payload->body_length;
  partitionCachedRecordbatch_[partitionId].push_back(std::move(payload));
  partitionBufferIdxBase_[partitionId] = 0;
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) {
  if (partitionBufferIdxBase_[partitionId] <= 0) {
    return arrow::Status::OK();
  }
  // already filled
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto listIdx = 0;
  auto numFields = schema_->num_fields();
  auto numRows = partitionBufferIdxBase_[partitionId];

  std::vector<std::shared_ptr<arrow::Array>> arrays(numFields);
  for (int i = 0; i < numFields; ++i) {
    size_t sizeofBinaryOffset = -1;
    switch (columnTypeId_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeofBinaryOffset = sizeof(arrow::BinaryType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeofBinaryOffset == -1)
          sizeofBinaryOffset = sizeof(arrow::LargeBinaryType::offset_type);

        auto buffers = partitionBuffers_[fixedWidthColCnt_ + binaryIdx][partitionId];
        // validity buffer
        if (buffers[0] != nullptr) {
          buffers[0] = arrow::SliceBuffer(buffers[0], 0, arrow::bit_util::BytesForBits(numRows));
        }
        // offset buffer
        if (buffers[1] != nullptr) {
          buffers[1] = arrow::SliceBuffer(buffers[1], 0, (numRows + 1) * sizeofBinaryOffset);
        }
        // value buffer
        if (buffers[2] != nullptr) {
          ARROW_CHECK_NE(buffers[1], nullptr);
          buffers[2] = arrow::SliceBuffer(
              buffers[2],
              0,
              sizeofBinaryOffset == 4
                  ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[1]->data())[numRows]
                  : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[1]->data())[numRows]);
        }

        arrays[i] = arrow::MakeArray(
            arrow::ArrayData::Make(schema_->field(i)->type(), numRows, {buffers[0], buffers[1], buffers[2]}));

        uint64_t dstOffset0 = sizeofBinaryOffset == 4
            ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[1]->data())[0]
            : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[1]->data())[0];
        ARROW_CHECK_EQ(dstOffset0, 0);

        if (resetBuffers) {
          partitionValidityAddrs_[fixedWidthColCnt_ + binaryIdx][partitionId] = nullptr;
          partitionBinaryAddrs_[binaryIdx][partitionId] = BinaryBuff();
          partitionBuffers_[fixedWidthColCnt_ + binaryIdx][partitionId].clear();
        } else {
          // reset the offset
          partitionBinaryAddrs_[binaryIdx][partitionId].value_offset = 0;
        }
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        auto& builder = partitionListBuilders_[listIdx][partitionId];
        if (resetBuffers) {
          RETURN_NOT_OK(builder->Finish(&arrays[i]));
          builder->Reset();
        } else {
          RETURN_NOT_OK(builder->Finish(&arrays[i]));
          builder->Reset();
          RETURN_NOT_OK(builder->Reserve(numRows));
        }
        listIdx++;
        break;
      }
      case arrow::NullType::type_id: {
        arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(arrow::null(), numRows, {nullptr, nullptr}, numRows));
        break;
      }
      default: {
        auto buffers = partitionBuffers_[fixedWidthIdx][partitionId];
        if (buffers[0] != nullptr) {
          buffers[0] = arrow::SliceBuffer(buffers[0], 0, arrow::bit_util::BytesForBits(numRows));
        }
        if (buffers[1] != nullptr) {
          if (columnTypeId_[i]->id() == arrow::BooleanType::type_id)
            buffers[1] = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(numRows));
          else
            buffers[1] = arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(columnTypeId_[i]->id()) >> 3));
        }

        arrays[i] =
            arrow::MakeArray(arrow::ArrayData::Make(schema_->field(i)->type(), numRows, {buffers[0], buffers[1]}));
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
  auto batch = arrow::RecordBatch::Make(schema_, numRows, std::move(arrays));
  return cacheRecordBatch(partitionId, *batch);
}

arrow::Status ArrowShuffleWriter::allocatePartitionBuffers(int32_t partitionId, int32_t newSize) {
  // try to allocate new
  auto numFields = schema_->num_fields();
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto listIdx = 0;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> newListBuilders;

  for (auto i = 0; i < numFields; ++i) {
    size_t sizeofBinaryOffset = -1;

    switch (columnTypeId_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeofBinaryOffset = sizeof(arrow::StringType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeofBinaryOffset == -1) {
          sizeofBinaryOffset = sizeof(arrow::LargeStringType::offset_type);
        }

        std::shared_ptr<arrow::Buffer> offsetBuffer;
        std::shared_ptr<arrow::Buffer> validityBuffer = nullptr;
        auto valueBufSize = binaryArrayEmpiricalSize_[binaryIdx] * newSize + 1024;
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Buffer> value_buffer,
            arrow::AllocateResizableBuffer(valueBufSize, options_.memory_pool.get()));
        ARROW_RETURN_NOT_OK(allocateBufferFromPool(offsetBuffer, newSize * sizeofBinaryOffset + 1));
        // set the first offset to 0
        uint8_t* offsetaddr = offsetBuffer->mutable_data();
        memset(offsetaddr, 0, 8);

        partitionBinaryAddrs_[binaryIdx][partitionId] =
            BinaryBuff(value_buffer->mutable_data(), offsetBuffer->mutable_data(), valueBufSize);

        if (inputHasNull_[fixedWidthColCnt_ + binaryIdx]) {
          ARROW_RETURN_NOT_OK(allocateBufferFromPool(validityBuffer, arrow::bit_util::BytesForBits(newSize)));
          // initialize all true once allocated
          memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
          partitionValidityAddrs_[fixedWidthColCnt_ + binaryIdx][partitionId] = validityBuffer->mutable_data();
        } else {
          partitionValidityAddrs_[fixedWidthColCnt_ + binaryIdx][partitionId] = nullptr;
        }
        partitionBuffers_[fixedWidthColCnt_ + binaryIdx][partitionId] = {
            std::move(validityBuffer), std::move(offsetBuffer), std::move(value_buffer)};
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
        RETURN_NOT_OK(MakeBuilder(options_.memory_pool.get(), columnTypeId_[i], &arrayBuilder));
        assert(arrayBuilder != nullptr);
        RETURN_NOT_OK(arrayBuilder->Reserve(newSize));
        partitionListBuilders_[listIdx][partitionId] = std::move(arrayBuilder);
        listIdx++;
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: {
        std::shared_ptr<arrow::Buffer> valueBuffer;
        std::shared_ptr<arrow::Buffer> validityBuffer = nullptr;
        if (columnTypeId_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_RETURN_NOT_OK(allocateBufferFromPool(valueBuffer, arrow::bit_util::BytesForBits(newSize)));
        } else {
          ARROW_RETURN_NOT_OK(
              allocateBufferFromPool(valueBuffer, newSize * (arrow::bit_width(columnTypeId_[i]->id()) >> 3)));
        }
        partitionFixedWidthValueAddrs_[fixedWidthIdx][partitionId] = valueBuffer->mutable_data();

        if (inputHasNull_[fixedWidthIdx]) {
          ARROW_RETURN_NOT_OK(allocateBufferFromPool(validityBuffer, arrow::bit_util::BytesForBits(newSize)));
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

  partitionBufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::allocateNew(int32_t partitionId, int32_t newSize) {
  auto status = allocatePartitionBuffers(partitionId, newSize);
  int32_t retry = 0;
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    std::cerr << status.ToString() << std::endl
              << std::to_string(++retry) << " retry to allocate new buffer for partition "
              << std::to_string(partitionId) << std::endl;
    int64_t evictedSize;
    ARROW_ASSIGN_OR_RAISE(auto partition_to_evict, evictLargestPartition(&evictedSize));
    if (partition_to_evict == -1) {
      std::cerr << "Failed to allocate new buffer for partition " << std::to_string(partitionId)
                << ". No partition buffer to evict." << std::endl;
      return status;
    }
    status = allocatePartitionBuffers(partitionId, newSize);
  }
  if (status.IsOutOfMemory()) {
    std::cerr << "Failed to allocate new buffer for partition " << std::to_string(partitionId) << ". Out of memory."
              << std::endl;
  }
  return status;
}

// call from memory management
arrow::Status ArrowShuffleWriter::evictFixedSize(int64_t size, int64_t* actual) {
  int64_t currentEvicted = 0L;
  int32_t tryCount = 0;
  while (currentEvicted < size && tryCount < 5) {
    tryCount++;
    int64_t singleCallEvicted;
    ARROW_ASSIGN_OR_RAISE(int32_t evicted_partition_id, evictLargestPartition(&singleCallEvicted))
    if (evicted_partition_id == -1) {
      break;
    }
    currentEvicted += singleCallEvicted;
  }
  *actual = currentEvicted;
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::evictPartition(int32_t partitionId) {
  RETURN_NOT_OK(partitionWriter_->evictPartition(partitionId));

  // reset validity buffer after evict
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

arrow::Result<int32_t> ArrowShuffleWriter::evictLargestPartition(int64_t* size) {
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
  return partitionToEvict;
}

ArrowShuffleWriter::row_offset_type ArrowShuffleWriter::calculateSplitBatchSize(const arrow::RecordBatch& rb) {
  uint32_t sizePerRow = 0;
  auto numRows = rb.num_rows();
  for (size_t i = fixedWidthColCnt_; i < arrayIdx_.size(); ++i) {
    if (ARROW_PREDICT_FALSE(binaryArrayEmpiricalSize_[i - fixedWidthColCnt_] == 0)) {
      auto arr = rb.column_data(arrayIdx_[i]);
      // ARROW_CHECK_EQ(arr->buffers.size(), 3);
      // offset array_data
      if (ARROW_PREDICT_TRUE(arr->buffers[1] != nullptr)) {
        auto offsetbuf = arr->buffers[1]->data();
        uint64_t length = 0;
        switch (columnTypeId_[arrayIdx_[i]]->id()) {
          case arrow::BinaryType::type_id:
          case arrow::StringType::type_id:
            length = reinterpret_cast<const arrow::StringType::offset_type*>(offsetbuf)[numRows] -
                reinterpret_cast<const arrow::StringType::offset_type*>(offsetbuf)[0];
            break;
          case arrow::LargeBinaryType::type_id:
          case arrow::LargeStringType::type_id:
            length = reinterpret_cast<const arrow::LargeStringType::offset_type*>(offsetbuf)[numRows] -
                reinterpret_cast<const arrow::LargeStringType::offset_type*>(offsetbuf)[0];
            break;
          default:
            break;
        }
        binaryArrayEmpiricalSize_[i - fixedWidthColCnt_] =
            length % numRows == 0 ? length / numRows : length / numRows + 1;
        // std::cout << "avg str length col = " << i - fixed_width_col_cnt_ << "
        // len = "
        // << binary_array_empirical_size_[i - fixed_width_col_cnt_] <<
        // std::endl;
      }
    }
  }

  sizePerRow = std::accumulate(binaryArrayEmpiricalSize_.begin(), binaryArrayEmpiricalSize_.end(), 0);

  for (size_t col = 0; col < arrayIdx_.size(); ++col) {
    auto colIdx = arrayIdx_[col];
    auto typeId = columnTypeId_[colIdx]->id();
    // why +7? to fit column bool
    sizePerRow += ((arrow::bit_width(typeId) + 7) >> 3);
  }

  int64_t preallocRowCnt = options_.offheap_per_task > 0 && sizePerRow > 0
      ? options_.offheap_per_task / sizePerRow / numPartitions_ >> 2
      : options_.buffer_size;
  preallocRowCnt = std::min(preallocRowCnt, (int64_t)options_.buffer_size);

  return (row_offset_type)preallocRowCnt;
}

arrow::Status ArrowShuffleWriter::doSplit(const arrow::RecordBatch& rb) {
  // buffer is allocated less than 64K
  // ARROW_CHECK_LE(rb.num_rows(),64*1024);

  reducerOffsets_.resize(rb.num_rows());

  reducerOffsetOffset_[0] = 0;
  for (auto pid = 1; pid <= numPartitions_; pid++) {
    reducerOffsetOffset_[pid] = reducerOffsetOffset_[pid - 1] + partitionIdCnt_[pid - 1];
  }
  for (auto row = 0; row < rb.num_rows(); row++) {
    auto pid = partitionId_[row];
    reducerOffsets_[reducerOffsetOffset_[pid]] = row;
    PREFETCHT0((reducerOffsets_.data() + reducerOffsetOffset_[pid] + 32));
    reducerOffsetOffset_[pid]++;
  }
  std::transform(
      reducerOffsetOffset_.begin(),
      std::prev(reducerOffsetOffset_.end()),
      partitionIdCnt_.begin(),
      reducerOffsetOffset_.begin(),
      [](row_offset_type x, row_offset_type y) { return x - y; });
  // for the first input record batch, scan binary arrays and large binary
  // arrays to get their empirical sizes

  for (size_t col = 0; col < arrayIdx_.size(); ++col) {
    auto colIdx = arrayIdx_[col];
    // check input_has_null_[col] is cheaper than GetNullCount()
    // once input_has_null_ is set to true, we didn't reset it after evict
    if (!inputHasNull_[col] && rb.column_data(colIdx)->GetNullCount() != 0) {
      inputHasNull_[col] = true;
    }
  }

  // prepare partition buffers and evict if necessary
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionIdCnt_[pid] > 0) {
      // make sure the size to be allocated is larger than the size to be filled
      if (partitionBufferSize_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto newSize = std::max(calculateSplitBatchSize(rb), partitionIdCnt_[pid]);
        RETURN_NOT_OK(allocatePartitionBuffers(pid, newSize));
      } else if (partitionBufferIdxBase_[pid] + partitionIdCnt_[pid] > (unsigned)partitionBufferSize_[pid]) {
        auto newSize = std::max(calculateSplitBatchSize(rb), partitionIdCnt_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to
        // allocate new buffer
        if (options_.prefer_evict) {
          // if prefer_evict is set, evict current record batch, we may reuse
          // the buffers

          if (newSize > (unsigned)partitionBufferSize_[pid]) {
            // if the partition size after split is already larger than
            // allocated buffer size, need reallocate
            RETURN_NOT_OK(createRecordBatchFromBuffer(pid, /*reset_buffers = */ true));

            // splill immediately
            RETURN_NOT_OK(evictPartition(pid));
            RETURN_NOT_OK(allocatePartitionBuffers(pid, newSize));
          } else {
            // partition size after split is smaller than buffer size, no need
            // to reset buffer, reuse it.
            RETURN_NOT_OK(createRecordBatchFromBuffer(pid, /*reset_buffers = */ false));
            RETURN_NOT_OK(evictPartition(pid));
          }
        } else {
          // if prefer_evict is disabled, cache the record batch
          RETURN_NOT_OK(createRecordBatchFromBuffer(pid, /*reset_buffers = */ true));
          // allocate partition buffer with retries
          RETURN_NOT_OK(allocateNew(pid, newSize));
        }
      }
    }
  }
  // now start to split the record batch
  RETURN_NOT_OK(splitFixedWidthValueBuffer(rb));
  RETURN_NOT_OK(splitValidityBuffer(rb));
  RETURN_NOT_OK(splitBinaryArray(rb));
  RETURN_NOT_OK(splitListArray(rb));

  // update partition buffer base after split
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partitionBufferIdxBase_[pid] += partitionIdCnt_[pid];
  }

  return arrow::Status::OK();
}

template <typename T>
arrow::Status ArrowShuffleWriter::splitFixedType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
  std::transform(
      dstAddrs.begin(),
      dstAddrs.end(),
      partitionBufferIdxBase_.begin(),
      partitionBufferIdxOffset_.begin(),
      [](uint8_t* x, row_offset_type y) { return x + y * sizeof(T); });
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < numPartitions_; pid++) {
    auto dstPidBase = reinterpret_cast<T*>(partitionBufferIdxOffset_[pid]); /*32k*/
    auto r = reducerOffsetOffset_[pid]; /*8k*/
    auto size = reducerOffsetOffset_[pid + 1];
    for (; r < size; r++) {
      auto srcOffset = reducerOffsets_[r]; /*16k*/
      *dstPidBase = reinterpret_cast<const T*>(srcAddr)[srcOffset]; /*64k*/
      PREFETCHT2((srcAddr + srcOffset * sizeof(T) + 64));
      dstPidBase += 1;
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::splitFixedWidthValueBuffer(const arrow::RecordBatch& rb) {
  for (auto col = 0; col < fixedWidthColCnt_; ++col) {
    const auto& dstAddrs = partitionFixedWidthValueAddrs_[col];
    auto colIdx = arrayIdx_[col];
    auto srcAddr = const_cast<uint8_t*>(rb.column_data(colIdx)->buffers[1]->data());

    switch (arrow::bit_width(columnTypeId_[colIdx]->id())) {
      case 8:
        RETURN_NOT_OK(splitFixedType<uint8_t>(srcAddr, dstAddrs));
        break;
      case 16:
        RETURN_NOT_OK(splitFixedType<uint16_t>(srcAddr, dstAddrs));
        break;
      case 32:
        RETURN_NOT_OK(splitFixedType<uint32_t>(srcAddr, dstAddrs));
        break;
      case 64:
#ifdef PROCESSAVX
        std::transform(
            dstAddrs.begin(),
            dstAddrs.end(),
            partitionBufferIdxBase_.begin(),
            partitionBufferIdxOffset_.begin(),
            [](uint8_t* x, row_offset_type y) { return x + y * sizeof(uint64_t); });
        for (auto pid = 0; pid < numPartitions_; pid++) {
          auto dstPidBase = reinterpret_cast<uint64_t*>(partitionBufferIdxOffset_[pid]); /*32k*/
          auto r = reducerOffsetOffset_[pid]; /*8k*/
          auto size = reducerOffsetOffset_[pid + 1];
#if 1
          for (r; r < size && (((uint64_t)dstPidBase & 0x1f) > 0); r++) {
            auto srcOffset = reducerOffsets_[r]; /*16k*/
            *dstPidBase = reinterpret_cast<uint64_t*>(srcAddr)[srcOffset]; /*64k*/
            _mm_prefetch(&(srcAddr)[srcOffset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dstPidBase += 1;
          }
#if 0
          for (r; r+4<size; r+=4)                              
          {                                                                                    
            auto srcOffset = reducerOffsets_[r];                                 /*16k*/
            __m128i srcLd = _mm_loadl_epi64((__m128i*)(&reducerOffsets_[r]));
            __m128i srcOffset4x = _mm_cvtepu16_epi32(srcLd);
            
            __m256i src4x = _mm256_i32gather_epi64((const long long int*)srcAddr,srcOffset4x,8);
            //_mm256_store_si256((__m256i*)dstPidBase,src4x);
            _mm_stream_si128((__m128i*)dstPidBase,src2x);
                                                         
            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r+1]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r+2]*sizeof(uint64_t)+64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r+3]*sizeof(uint64_t)+64], _MM_HINT_T2);
            dstPidBase+=4;
          }
#endif
          for (r; r + 2 < size; r += 2) {
            __m128i srcOffset2x = _mm_cvtsi32_si128(*((int32_t*)(reducerOffsets_.data() + r)));
            srcOffset2x = _mm_shufflelo_epi16(srcOffset2x, 0x98);

            __m128i src2x = _mm_i32gather_epi64((const long long int*)srcAddr, srcOffset2x, 8);
            _mm_store_si128((__m128i*)dstPidBase, src2x);
            //_mm_stream_si128((__m128i*)dstPidBase,src2x);

            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            _mm_prefetch(&(srcAddr)[(uint32_t)reducerOffsets_[r + 1] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dstPidBase += 2;
          }
#endif
          for (r; r < size; r++) {
            auto srcOffset = reducerOffsets_[r]; /*16k*/
            *dstPidBase = reinterpret_cast<const uint64_t*>(srcAddr)[srcOffset]; /*64k*/
            _mm_prefetch(&(srcAddr)[srcOffset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dstPidBase += 1;
          }
        }
        break;
#else
        RETURN_NOT_OK(splitFixedType<uint64_t>(srcAddr, dstAddrs));
#endif
        break;
#if defined(__x86_64__)
      case 128: // arrow::Decimal128Type::type_id
        // too bad gcc generates movdqa even we use __m128i_u data type.
        // splitFixedType<__m128i_u>(srcAddr, dstAddrs);
        {
          std::transform(
              dstAddrs.begin(),
              dstAddrs.end(),
              partitionBufferIdxBase_.begin(),
              partitionBufferIdxOffset_.begin(),
              [](uint8_t* x, row_offset_type y) { return x + y * sizeof(__m128i_u); });
          // assume batch size = 32k; reducer# = 4K; row/reducer = 8
          for (auto pid = 0; pid < numPartitions_; pid++) {
            auto dstPidBase = reinterpret_cast<__m128i_u*>(partitionBufferIdxOffset_[pid]); /*32k*/
            auto r = reducerOffsetOffset_[pid]; /*8k*/
            auto size = reducerOffsetOffset_[pid + 1];
            for (; r < size; r++) {
              auto srcOffset = reducerOffsets_[r]; /*16k*/
              __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i_u*>(srcAddr) + srcOffset);
              _mm_storeu_si128(dstPidBase, value);
              _mm_prefetch(srcAddr + srcOffset * sizeof(__m128i_u) + 64, _MM_HINT_T2);
              dstPidBase += 1;
            }
          }
        }
#elif defined(__aarch64__)
      case 128:
        RETURN_NOT_OK(splitFixedType<uint32x4_t>(srcAddr, dstAddrs));
#endif
        break;
      case 1: // arrow::BooleanType::type_id:
        RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
        break;
      default:
        return arrow::Status::Invalid(
            "Column type " + schema_->field(colIdx)->type()->ToString() + " is not fixed width");
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < numPartitions_; pid++) {
    // set the last byte
    auto dstaddr = dstAddrs[pid];
    if (partitionIdCnt_[pid] > 0 && dstaddr != nullptr) {
      auto r = reducerOffsetOffset_[pid]; /*8k*/
      auto size = reducerOffsetOffset_[pid + 1];
      row_offset_type dstOffset = partitionBufferIdxBase_[pid];
      row_offset_type dstOffsetInByte = (8 - (dstOffset & 0x7)) & 0x7;
      row_offset_type dstIdxByte = dstOffsetInByte;
      uint8_t dst = dstaddr[dstOffset >> 3];
      if (pid + 1 < numPartitions_) {
        PREFETCHT1((&dstaddr[partitionBufferIdxBase_[pid + 1] >> 3]));
      }
      for (; r < size && dstIdxByte > 0; r++, dstIdxByte--) {
        auto srcOffset = reducerOffsets_[r]; /*16k*/
        uint8_t src = srcAddr[srcOffset >> 3];
        src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, 8 - dstIdxByte);
#else
        src = rotateLeft(src, (8 - dst_idx_byte));
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
        auto srcOffset = reducerOffsets_[r]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        PREFETCHT0((&(srcAddr)[(srcOffset >> 3) + 64]));
        dst = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 1]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 1 | 0xfd; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 2]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 2 | 0xfb; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 3]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 3 | 0xf7; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 4]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 4 | 0xef; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 5]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 5 | 0xdf; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 6]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 6 | 0xbf; // get the bit in bit 0, other bits set to 1

        srcOffset = reducerOffsets_[r + 7]; /*16k*/
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
        auto srcOffset = reducerOffsets_[r]; /*16k*/
        uint8_t src = srcAddr[srcOffset >> 3];
        src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, dstIdxByte);
#else
        src = rotateLeft(src, dst_idx_byte);
#endif
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dstOffset >> 3] = dst;
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::splitValidityBuffer(const arrow::RecordBatch& rb) {
  for (size_t col = 0; col < arrayIdx_.size(); ++col) {
    auto colIdx = arrayIdx_[col];
    auto& dstAddrs = partitionValidityAddrs_[col];
    if (rb.column_data(colIdx)->GetNullCount() > 0) {
      // there is Null count
      for (auto pid = 0; pid < numPartitions_; ++pid) {
        if (partitionIdCnt_[pid] > 0 && dstAddrs[pid] == nullptr) {
          // init bitmap if it's null, initialize the buffer as true
          auto newSize = std::max(partitionIdCnt_[pid], (row_offset_type)options_.buffer_size);
          std::shared_ptr<arrow::Buffer> validityBuffer;
          auto status = allocateBufferFromPool(validityBuffer, arrow::bit_util::BytesForBits(newSize));
          ARROW_RETURN_NOT_OK(status);
          dstAddrs[pid] = const_cast<uint8_t*>(validityBuffer->data());
          memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
          partitionBuffers_[col][pid][0] = std::move(validityBuffer);
        }
      }
      auto srcAddr = const_cast<uint8_t*>(rb.column_data(colIdx)->buffers[0]->data());

      RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
    }
  }
  return arrow::Status::OK();
}

template <typename T>
arrow::Status ArrowShuffleWriter::splitBinaryType(
    const uint8_t* srcAddr,
    const T* srcOffsetAddr,
    std::vector<BinaryBuff>& dstAddrs,
    const int binaryIdx) {
  for (auto pid = 0; pid < numPartitions_; pid++) {
    auto dstOffsetBase = reinterpret_cast<T*>(dstAddrs[pid].offsetptr) + partitionBufferIdxBase_[pid];
    if (pid + 1 < numPartitions_) {
      PREFETCHT1((reinterpret_cast<T*>(dstAddrs[pid + 1].offsetptr) + partitionBufferIdxBase_[pid + 1]));
      PREFETCHT1((dstAddrs[pid + 1].valueptr + dstAddrs[pid + 1].value_offset));
    }
    auto valueOffset = dstAddrs[pid].value_offset;
    auto dstValueBase = dstAddrs[pid].valueptr + valueOffset;
    auto capacity = dstAddrs[pid].value_capacity;

    auto r = reducerOffsetOffset_[pid]; /*128k*/
    auto size = reducerOffsetOffset_[pid + 1] - r;

    auto multiply = 1;
    for (uint32_t x = 0; x < size; x++) {
      auto srcOffset = reducerOffsets_[x + r]; /*128k*/
      auto strlength = srcOffsetAddr[srcOffset + 1] - srcOffsetAddr[srcOffset];
      valueOffset = dstOffsetBase[x + 1] = valueOffset + strlength;
      if (ARROW_PREDICT_FALSE(valueOffset >= capacity)) {
        // allocate value buffer again, enlarge the buffer
        auto oldCapacity = capacity;
        capacity = capacity + std::max((capacity >> multiply), (uint64_t)strlength);
        multiply = std::min(3, multiply + 1);
        auto valueBuffer =
            std::static_pointer_cast<arrow::ResizableBuffer>(partitionBuffers_[fixedWidthColCnt_ + binaryIdx][pid][2]);
        RETURN_NOT_OK(valueBuffer->Reserve(capacity));

        dstAddrs[pid].valueptr = valueBuffer->mutable_data();
        dstAddrs[pid].value_capacity = capacity;
        dstValueBase = dstAddrs[pid].valueptr + valueOffset - strlength;
        std::cerr << "Split value buffer resized colid = " << binaryIdx << " dst_start " << dstOffsetBase[x]
                  << " dst_end " << dstOffsetBase[x + 1] << " old size = " << oldCapacity << " new size = " << capacity
                  << " row = " << partitionBufferIdxBase_[pid] << " strlen = " << strlength << std::endl;
      }
      auto valueSrcPtr = srcAddr + srcOffsetAddr[srcOffset];
#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(supportAvx512_)) {
        // write the variable value
        T k;
        for (k = 0; k + 32 < strlength; k += 32) {
          __m256i v = _mm256_loadu_si256((const __m256i*)(valueSrcPtr + k));
          _mm256_storeu_si256((__m256i*)(dstValueBase + k), v);
        }
        auto mask = (1L << (strlength - k)) - 1;
        __m256i v = _mm256_maskz_loadu_epi8(mask, valueSrcPtr + k);
        _mm256_mask_storeu_epi8(dstValueBase + k, mask, v);
      } else
#endif
      {
        memcpy(dstValueBase, valueSrcPtr, strlength);
      }
      dstValueBase += strlength;
      PREFETCHT1((valueSrcPtr + 64));
      PREFETCHT1((srcOffsetAddr + srcOffset + 64 / sizeof(T)));
    }
    dstAddrs[pid].value_offset = valueOffset;
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::splitBinaryArray(const arrow::RecordBatch& rb) {
  for (auto col = fixedWidthColCnt_; col < arrayIdx_.size(); ++col) {
    auto& dstAddrs = partitionBinaryAddrs_[col - fixedWidthColCnt_];
    auto colIdx = arrayIdx_[col];
    auto arrData = rb.column_data(colIdx);
    auto srcValueAddr = arrData->GetValuesSafe<uint8_t>(2);

    auto typeids = columnTypeId_[colIdx]->id();
    if (typeids == arrow::BinaryType::type_id || typeids == arrow::StringType::type_id) {
      auto srcOffsetAddr = arrData->GetValuesSafe<arrow::BinaryType::offset_type>(1);
      RETURN_NOT_OK(splitBinaryType<arrow::BinaryType::offset_type>(
          srcValueAddr, srcOffsetAddr, dstAddrs, col - fixedWidthColCnt_));
    } else {
      auto srcOffsetAddr = arrData->GetValuesSafe<arrow::LargeBinaryType::offset_type>(1);
      RETURN_NOT_OK(splitBinaryType<arrow::LargeBinaryType::offset_type>(
          srcValueAddr, srcOffsetAddr, dstAddrs, col - fixedWidthColCnt_));
    }
  }

  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Decimal128Type)         \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::BinaryType)
arrow::Status ArrowShuffleWriter::splitListArray(const arrow::RecordBatch& rb) {
  for (size_t i = 0; i < listArrayIdx_.size(); ++i) {
    auto srcArr = std::static_pointer_cast<arrow::ListArray>(rb.column(listArrayIdx_[i]));
    auto status = appendList(rb.column(listArrayIdx_[i]), partitionListBuilders_[i], rb.num_rows());
    if (!status.ok())
      return status;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

arrow::Status ArrowShuffleWriter::appendList(
    const std::shared_ptr<arrow::Array>& srcArr,
    const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dstBuilders,
    int64_t numRows) {
  for (auto row = 0; row < numRows; ++row) {
    RETURN_NOT_OK(dstBuilders[partitionId_[row]]->AppendArraySlice(*(srcArr->data().get()), row, 1));
  }
  return arrow::Status::OK();
}

arrow::Result<const int32_t*> ArrowShuffleWriter::getFirstColumn(const arrow::RecordBatch& rb) {
  if (partitioner_->hasPid()) {
    if (rb.num_columns() == 0) {
      return arrow::Status::Invalid("Recordbatch missing partition id column.");
    }

    if (rb.column(0)->type_id() != arrow::Type::INT32) {
      return arrow::Status::Invalid(
          "RecordBatch field 0 should be ",
          arrow::int32()->ToString(),
          ", actual is ",
          rb.column(0)->type()->ToString());
    }

    auto pidArr = reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
    if (pidArr == nullptr) {
      return arrow::Status::Invalid("failed to cast rb.column(0), this column should be pid");
    }

    return pidArr;
  } else {
    return nullptr;
  }
}

} // namespace gluten
