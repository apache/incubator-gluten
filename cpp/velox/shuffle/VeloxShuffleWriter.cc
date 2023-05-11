#include "VeloxShuffleWriter.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/vector/arrow/Bridge.h"

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

namespace gluten {

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

namespace {

bool vectorHasNull(const velox::VectorPtr& vp) {
  const auto& nulls = vp->nulls();
  if (!nulls) {
    return false;
  }
  return vp->countNulls(nulls, vp->size()) != 0;
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
  support_avx512_ = false;
#endif

  // partition number should be less than 64k
  ARROW_CHECK_LE(numPartitions_, 64 * 1024);

  // split record batch size should be less than 32k
  ARROW_CHECK_LE(options_.buffer_size, 32 * 1024);

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

  RETURN_NOT_OK(initIpcWriteOptions());

  // Allocate first buffer for split reducer
  // when partitioner is SinglePart, don`t need init combine_buffer_
  if (options_.partitioning_name != "single") {
    ARROW_ASSIGN_OR_RAISE(combineBuffer_, arrow::AllocateResizableBuffer(0, options_.memory_pool.get()));
    RETURN_NOT_OK(combineBuffer_->Resize(0, /*shrink_to_fit =*/false));
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initIpcWriteOptions() {
  auto& ipcWriteOptions = options_.ipc_write_options;
  ipcWriteOptions.memory_pool = options_.memory_pool.get();
  ipcWriteOptions.use_threads = false;

  tinyBatchWriteOptions_ = ipcWriteOptions;
  tinyBatchWriteOptions_.codec = nullptr;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initPartitions(const velox::RowVector& rv) {
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
  std::for_each(partitionBinaryAddrs_.begin(), partitionBinaryAddrs_.end(), [this](std::vector<BinaryBuff>& v) {
    v.resize(numPartitions_);
  });

  partitionListBuilders_.resize(complexColumnIndices_.size());
  for (size_t i = 0; i < complexColumnIndices_.size(); ++i) {
    partitionListBuilders_[i].resize(numPartitions_);
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::setCompressType(arrow::Compression::type compressedType) {
  ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec, createArrowIpcCodec(compressedType));
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::split(ColumnarBatch* cb) {
  auto veloxColumnBatch = dynamic_cast<VeloxColumnarBatch*>(cb);
  if (options_.partitioning_name == "single") {
    auto vp = veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(*vp));
    // 1. convert RowVector to RecordBatch
    ArrowArray arrowArray;
    velox::exportToArrow(vp, arrowArray, getDefaultVeloxLeafMemoryPool().get());

    auto result = arrow::ImportRecordBatch(&arrowArray, schema_);
    RETURN_NOT_OK(result);

    // 2. call CacheRecordBatch with RecordBatch
    RETURN_NOT_OK(cacheRecordBatch(0, *(*result)));
  } else {
    auto& rv = *veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    ARROW_ASSIGN_OR_RAISE(auto pid_arr, getFirstColumn(rv));
    RETURN_NOT_OK(partitioner_->compute(pid_arr, rv.size(), row2Partition_, partition2RowCount_));
    if (partitioner_->hasPid()) {
      auto strippedRv = getStrippedRowVector(rv);
      RETURN_NOT_OK(doSplit(strippedRv));
    } else {
      RETURN_NOT_OK(doSplit(rv));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::stop() {
  EVAL_START("write", options_.thread_id)
  RETURN_NOT_OK(partitionWriter_->stop());
  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::createPartition2Row(uint32_t rowNum) {
  // calc partition2RowOffset_
  partition2RowOffset_[0] = 0;
  for (auto pid = 1; pid <= numPartitions_; ++pid) {
    partition2RowOffset_[pid] = partition2RowOffset_[pid - 1] + partition2RowCount_[pid - 1];
  }

  // get a copy of partition2RowOffset_
  auto partition2RowOffsetCopy = partition2RowOffset_;

  // calc rowOffset2RowId_
  rowOffset2RowId_.resize(rowNum);
  for (auto row = 0; row < rowNum; ++row) {
    auto pid = row2Partition_[row];
    rowOffset2RowId_[partition2RowOffsetCopy[pid]++] = row;
  }

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
      if (partition2BufferSize_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto newSize = std::max(calculatePartitionBufferSize(rv), partition2RowCount_[pid]);
        RETURN_NOT_OK(allocatePartitionBuffers(pid, newSize));
      } else if (partitionBufferIdxBase_[pid] + partition2RowCount_[pid] > partition2BufferSize_[pid]) {
        auto newSize = std::max(calculatePartitionBufferSize(rv), partition2RowCount_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to allocate new buffer
        if (options_.prefer_evict) {
          // if prefer_evict is set, evict current RowVector, we may reuse the buffers
          if (newSize > partition2BufferSize_[pid]) {
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
  RETURN_NOT_OK(splitListArray(rv));
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
          if (column->type()->isShortDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            for (auto pid = 0; pid < numPartitions_; pid++) {
              auto dstPidBase = reinterpret_cast<__m128i_u*>(partitionBufferIdxOffset_[pid]); /*32k*/
              auto r = partition2RowOffset_[pid]; /*8k*/
              auto size = partition2RowOffset_[pid + 1];
              for (; r < size; r++) {
                auto srcOffset = rowOffset2RowId_[r]; /*16k*/
                const int64_t value = *(reinterpret_cast<const int64_t*>(srcAddr) + srcOffset);
                memcpy(dstPidBase, &value, sizeof(int64_t));
                dstPidBase += 1;
              }
            }
          } else if (column->type()->isLongDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            for (auto pid = 0; pid < numPartitions_; pid++) {
              auto dstPidBase = reinterpret_cast<__m128i_u*>(partitionBufferIdxOffset_[pid]); /*32k*/
              auto r = partition2RowOffset_[pid]; /*8k*/
              auto size = partition2RowOffset_[pid + 1];
              for (; r < size; r++) {
                auto srcOffset = rowOffset2RowId_[r]; /*16k*/
                __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i_u*>(srcAddr) + srcOffset);
                _mm_storeu_si128(dstPidBase, value);
                _mm_prefetch(srcAddr + srcOffset * sizeof(__m128i_u) + 64, _MM_HINT_T2);
                dstPidBase += 1;
              }
            }
          } else {
            return arrow::Status::Invalid(
                "Column type " + schema_->field(colIdx)->type()->ToString() + " is not supported.");
          }
        }
#elif defined(__aarch64__)
      case 128:
        if (column->type()->isShortDecimal()) {
          std::transform(
              dstAddrs.begin(),
              dstAddrs.end(),
              partitionBufferIdxBase_.begin(),
              partitionBufferIdxOffset_.begin(),
              [](uint8_t* x, uint32_t y) { return x + y * sizeof(uint32x4_t); });
          for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
            auto dstPidBase = reinterpret_cast<uint32x4_t*>(partitionBufferIdxOffset_[pid]);
            auto pos = partition2RowOffset_[pid];
            auto end = partition2RowOffset_[pid + 1];
            for (; pos < end; ++pos) {
              auto row_id = rowOffset2RowId_[pos];
              const uint64_t value = reinterpret_cast<const uint64_t*>(srcAddr)[row_id]; // copy
              memcpy(dstPidBase, &value, sizeof(uint64_t));
              dstPidBase += 1;
            }
          }
        } else if (column->type()->isLongDecimal()) {
          RETURN_NOT_OK(splitFixedType<uint32x4_t>(srcAddr, dstAddrs));
        } else {
          return arrow::Status::Invalid(
              "Column type " + schema_->field(colIdx)->type()->ToString() + " is not supported.");
        }
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
          auto status = allocateBufferFromPool(validityBuffer, arrow::bit_util::BytesForBits(newSize));
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
    uint32_t binaryIdx,
    const velox::FlatVector<velox::StringView>& src,
    std::vector<BinaryBuff>& dst) {
  auto rawValues = src.rawValues();

  for (auto pid = 0; pid < numPartitions_; ++pid) {
    auto& binaryBuf = dst[pid];

    // use 32bit offset
    using offset_type = arrow::BinaryType::offset_type;
    auto dstOffsetBase = (offset_type*)(binaryBuf.offset_ptr) + partitionBufferIdxBase_[pid];

    auto valueOffset = binaryBuf.value_offset;
    auto dstValuePtr = binaryBuf.value_ptr + valueOffset;
    auto capacity = binaryBuf.value_capacity;

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
            partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][pid][kValueBufferInedx]);

        RETURN_NOT_OK(valueBuffer->Reserve(capacity));

        binaryBuf.value_ptr = valueBuffer->mutable_data();
        binaryBuf.value_capacity = capacity;
        dstValuePtr = binaryBuf.value_ptr + valueOffset - stringLen;

        VsPrintSplit("Split value buffer resized colIdx", binaryIdx);
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

    binaryBuf.value_offset = valueOffset;
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::splitListArray(const velox::RowVector& rv) {
  for (size_t i = 0; i < complexColumnIndices_.size(); ++i) {
    auto colIdx = complexColumnIndices_[i];
    auto column = rv.childAt(colIdx);

    // TODO: rethink the cost of `exportToArrow+ImportArray`
    ArrowArray arrowArray;
    velox::exportToArrow(column, arrowArray, getDefaultVeloxLeafMemoryPool().get());

    auto result = arrow::ImportArray(&arrowArray, arrowColumnTypes_[colIdx]);
    RETURN_NOT_OK(result);

    auto numRows = rv.size();
    for (auto row = 0; row < numRows; ++row) {
      auto partition = row2Partition_[row];
      RETURN_NOT_OK(partitionListBuilders_[i][partition]->AppendArraySlice(*((*result)->data().get()), row, 1));
    }
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::splitBinaryArray(const velox::RowVector& rv) {
  for (auto col = fixedWidthColumnCount_; col < simpleColumnIndices_.size(); ++col) {
    auto binaryIdx = col - fixedWidthColumnCount_;
    auto& dstAddrs = partitionBinaryAddrs_[binaryIdx];
    auto colIdx = simpleColumnIndices_[col];
    auto column = rv.childAt(colIdx);
    auto typeKind = column->typeKind();
    if (typeKind == velox::TypeKind::VARCHAR || typeKind == velox::TypeKind::VARBINARY) {
      auto stringColumn = column->asFlatVector<velox::StringView>();
      assert(stringColumn);
      RETURN_NOT_OK(splitBinaryType(binaryIdx, *stringColumn, dstAddrs));
    } else {
      VsPrintLF("INVALID TYPE: neither VARCHAR nor VARBINARY!");
      assert(false);
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::veloxType2ArrowSchema(const velox::TypePtr& type) {
  auto out = std::make_shared<ArrowSchema>();
  auto rvp = velox::RowVector::createEmpty(type, getDefaultVeloxLeafMemoryPool().get());

  // get ArrowSchema from velox::RowVector
  velox::exportToArrow(rvp, *out);

  // convert ArrowSchema to arrow::Schema
  ARROW_ASSIGN_OR_RAISE(schema_, arrow::ImportSchema(out.get()));

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initColumnTypes(const velox::RowVector& rv) {
  RETURN_NOT_OK(veloxType2ArrowSchema(rv.type()));

  // remove the first column
  if (partitioner_->hasPid()) {
    ARROW_ASSIGN_OR_RAISE(schema_, schema_->RemoveField(0));
    // skip the first column
    for (size_t i = 1; i < rv.childrenSize(); ++i) {
      veloxColumnTypes_.push_back(rv.childAt(i)->type());
    }
  } else {
    if (veloxColumnTypes_.empty()) {
      for (size_t i = 0; i < rv.childrenSize(); ++i) {
        veloxColumnTypes_.push_back(rv.childAt(i)->type());
      }
    }
  }

  VsPrintSplitLF("schema_", schema_->ToString());

  // get arrow_column_types_ from schema
  ARROW_ASSIGN_OR_RAISE(arrowColumnTypes_, toShuffleWriterTypeId(schema_->fields()));

  for (size_t i = 0; i < arrowColumnTypes_.size(); ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        binaryColumnIndices_.push_back(i);
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        complexColumnIndices_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
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

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::initFromRowVector(const velox::RowVector& rv) {
  if (veloxColumnTypes_.empty()) {
    RETURN_NOT_OK(initColumnTypes(rv));
    RETURN_NOT_OK(initPartitions(rv));
  }
  return arrow::Status::OK();
}

velox::RowVector VeloxShuffleWriter::getStrippedRowVector(const velox::RowVector& rv) const {
  // get new row type
  auto rowType = rv.type()->asRow();
  auto typeChildren = rowType.children();
  typeChildren.erase(typeChildren.begin());
  auto newRowType = velox::ROW(std::move(typeChildren));

  // get null buffers
  const auto& nullbuffers = rv.nulls();

  // get length
  auto length = rv.size();

  // get children
  auto children = rv.children();
  children.erase(children.begin());

  // get nullcount
  auto nullcount = rv.getNullCount();

  return velox::RowVector(rv.pool(), newRowType, nullbuffers, length, children, nullcount);
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
      uint64_t length = 0;
      auto stringViews = stringViewColumn->rawValues<velox::StringView>();
      for (size_t row = 0; row != numRows; ++row) {
        length += stringViews[row].size();
      }

      binaryArrayEmpiricalSize_[index] = length % numRows == 0 ? length / numRows : length / numRows + 1;
    }
  }

  VS_PRINT_VECTOR_MAPPING(binary_array_empirical_size_);

  sizePerRow = std::accumulate(binaryArrayEmpiricalSize_.begin(), binaryArrayEmpiricalSize_.end(), 0);

  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    auto colIdx = simpleColumnIndices_[col];
    // `bool(1) >> 3` gets 0, so +7
    sizePerRow += ((arrow::bit_width(arrowColumnTypes_[colIdx]->id()) + 7) >> 3);
  }

  VS_PRINTLF(size_per_row);

  uint64_t preallocRowCnt = options_.offheap_per_task > 0 && sizePerRow > 0
      ? options_.offheap_per_task / sizePerRow / numPartitions_ >> 2
      : options_.buffer_size;
  preallocRowCnt = std::min(preallocRowCnt, (uint64_t)options_.buffer_size);

  VS_PRINTLF(prealloc_row_cnt);

  return preallocRowCnt;
}

arrow::Status VeloxShuffleWriter::allocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size) {
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

arrow::Status VeloxShuffleWriter::allocatePartitionBuffers(uint32_t partitionId, uint32_t newSize) {
  // try to allocate new
  auto numFields = schema_->num_fields();
  assert(numFields == arrowColumnTypes_.size());

  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto listIdx = 0;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> newListBuilders;

  for (auto i = 0; i < numFields; ++i) {
    size_t sizeofBinaryOffset = -1;
    switch (arrowColumnTypes_[i]->id()) {
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

        auto index = fixedWidthColumnCount_ + binaryIdx;
        if (inputHasNull_[index]) {
          ARROW_RETURN_NOT_OK(allocateBufferFromPool(validityBuffer, arrow::bit_util::BytesForBits(newSize)));
          // initialize all true once allocated
          memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
          partitionValidityAddrs_[index][partitionId] = validityBuffer->mutable_data();
        } else {
          partitionValidityAddrs_[index][partitionId] = nullptr;
        }
        partitionBuffers_[index][partitionId] = {
            std::move(validityBuffer), std::move(offsetBuffer), std::move(value_buffer)};
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
        RETURN_NOT_OK(MakeBuilder(options_.memory_pool.get(), arrowColumnTypes_[i], &arrayBuilder));
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
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_RETURN_NOT_OK(allocateBufferFromPool(valueBuffer, arrow::bit_util::BytesForBits(newSize)));
        } else {
          ARROW_RETURN_NOT_OK(
              allocateBufferFromPool(valueBuffer, newSize * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3)));
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

  partition2BufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::allocateNew(uint32_t partitionId, uint32_t newSize) {
  auto retry = 0;
  auto status = allocatePartitionBuffers(partitionId, newSize);
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    ++retry;
    std::cout << status.ToString() << std::endl
              << std::to_string(retry) << " retry to allocate new buffer for partition " << std::to_string(partitionId)
              << std::endl;

    int64_t evictedSize;
    ARROW_ASSIGN_OR_RAISE(auto partition_to_evict, evictLargestPartition(&evictedSize));
    if (partition_to_evict == -1) {
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

arrow::Status VeloxShuffleWriter::createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) {
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
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeofBinaryOffset = sizeof(arrow::BinaryType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeofBinaryOffset == -1)
          sizeofBinaryOffset = sizeof(arrow::LargeBinaryType::offset_type);

        auto buffers = partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId];
        // validity buffer
        if (buffers[kValidityBufferIndex] != nullptr) {
          buffers[kValidityBufferIndex] =
              arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows));
        }
        // offset buffer
        if (buffers[kOffsetBufferIndex] != nullptr) {
          buffers[kOffsetBufferIndex] =
              arrow::SliceBuffer(buffers[kOffsetBufferIndex], 0, (numRows + 1) * sizeofBinaryOffset);
        }
        // value buffer
        if (buffers[kValueBufferInedx] != nullptr) {
          ARROW_CHECK_NE(buffers[kOffsetBufferIndex], nullptr);
          buffers[kValueBufferInedx] = arrow::SliceBuffer(
              buffers[kValueBufferInedx],
              0,
              sizeofBinaryOffset == 4 ? reinterpret_cast<const arrow::BinaryType::offset_type*>(
                                            buffers[kOffsetBufferIndex]->data())[numRows]
                                      : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(
                                            buffers[kOffsetBufferIndex]->data())[numRows]);
        }

        arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
            schema_->field(i)->type(),
            numRows,
            {buffers[kValidityBufferIndex], buffers[kOffsetBufferIndex], buffers[kValueBufferInedx]}));

        uint64_t dstOffset0 = sizeofBinaryOffset == 4
            ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[kOffsetBufferIndex]->data())[0]
            : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[kOffsetBufferIndex]->data())[0];
        ARROW_CHECK_EQ(dstOffset0, 0);

        if (resetBuffers) {
          partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx][partitionId] = nullptr;
          partitionBinaryAddrs_[binaryIdx][partitionId] = BinaryBuff();
          partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId].clear();
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
          if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id)
            buffers[1] = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(numRows));
          else
            buffers[1] =
                arrow::SliceBuffer(buffers[1], 0, numRows * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3));
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
  auto rb = arrow::RecordBatch::Make(schema_, numRows, std::move(arrays));
  return cacheRecordBatch(partitionId, *rb);
}

namespace {

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

} // namespace

arrow::Status VeloxShuffleWriter::cacheRecordBatch(uint32_t partitionId, const arrow::RecordBatch& rb) {
  int64_t rawSize = getBatchNbytes(rb);
  rawPartitionLengths_[partitionId] += rawSize;
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
#ifndef SKIPCOMPRESS
  if (rb.num_rows() <= (uint32_t)options_.batch_compress_threshold) {
    TIME_NANO_OR_RAISE(
        totalCompressTime_, arrow::ipc::GetRecordBatchPayload(rb, tinyBatchWriteOptions_, payload.get()));
  } else {
    TIME_NANO_OR_RAISE(
        totalCompressTime_, arrow::ipc::GetRecordBatchPayload(rb, options_.ipc_write_options, payload.get()));
  }
#else
  // for test reason
  TIME_NANO_OR_RAISE(
      total_compress_time_, arrow::ipc::GetRecordBatchPayload(*rb, tiny_bach_write_options_, payload.get()));
#endif

  partitionCachedRecordbatchSize_[partitionId] += payload->body_length;
  partitionCachedRecordbatch_[partitionId].push_back(std::move(payload));
  partitionBufferIdxBase_[partitionId] = 0;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::evictFixedSize(int64_t size, int64_t* actual) {
  int64_t currentEvicted = 0L;
  auto tryCount = 0;
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

arrow::Result<int32_t> VeloxShuffleWriter::evictLargestPartition(int64_t* size) {
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

arrow::Status VeloxShuffleWriter::evictPartition(uint32_t partitionId) {
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

arrow::Result<const int32_t*> VeloxShuffleWriter::getFirstColumn(const velox::RowVector& rv) {
  if (partitioner_->hasPid()) {
    if (rv.childrenSize() == 0) {
      return arrow::Status::Invalid("RowVector missing partition id column.");
    }

    auto& firstChild = rv.childAt(0);
    if (!firstChild->type()->isInteger()) {
      return arrow::Status::Invalid("RecordBatch field 0 should be integer");
    }

    // first column is partition key hash value
    using NativeType = velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType;
    auto pidFlatVector = firstChild->asFlatVector<NativeType>();
    if (pidFlatVector == nullptr) {
      return arrow::Status::Invalid("failed to cast rv.column(0), this column should be pid");
    }
    return pidFlatVector->rawValues();
  } else {
    return nullptr;
  }
}

} // namespace gluten
