#include "VeloxShuffleWriter.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/vector/arrow/Bridge.h"

#include "utils/compression.h"
#include "utils/macros.h"

#include "shuffle/PartitionWriter.h"

#include "include/arrow/c/bridge.h"

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
std::string __m128i_toString(const __m128i var) {
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

bool VectorHasNull(const velox::VectorPtr& vp) {
  const auto& nulls = vp->nulls();
  if (!nulls) {
    return false;
  }
  return vp->countNulls(nulls, vp->size()) != 0;
}

} // namespace

// VeloxShuffleWriter
arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxShuffleWriter::Create(
    uint32_t num_partitions,
    SplitOptions options) {
  std::shared_ptr<VeloxShuffleWriter> res(new VeloxShuffleWriter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status VeloxShuffleWriter::Init() {
#if defined(__x86_64__)
  support_avx512_ = __builtin_cpu_supports("avx512bw");
#else
  support_avx512_ = false;
#endif

  // partition number should be less than 64k
  ARROW_CHECK_LE(num_partitions_, 64 * 1024);

  // split record batch size should be less than 32k
  ARROW_CHECK_LE(options_.buffer_size, 32 * 1024);

  ARROW_ASSIGN_OR_RAISE(partition_writer_, PartitionWriter::Make(this, num_partitions_));

  ARROW_ASSIGN_OR_RAISE(partitioner_, Partitioner::Make(options_.partitioning_name, num_partitions_));

  // pre-allocated buffer size for each partition, unit is row count
  // when partitioner is SinglePart, partial variables don`t need init
  if (options_.partitioning_name != "single") {
    partition_2_row_count_.resize(num_partitions_);
    partition_2_buffer_size_.resize(num_partitions_);
    partition_buffer_idx_offset_.resize(num_partitions_);
    partition_2_row_offset_.resize(num_partitions_ + 1);
  }

  partition_buffer_idx_base_.resize(num_partitions_);

  partition_cached_recordbatch_.resize(num_partitions_);
  partition_cached_recordbatch_size_.resize(num_partitions_);

  partition_lengths_.resize(num_partitions_);
  raw_partition_lengths_.resize(num_partitions_);

  RETURN_NOT_OK(SetCompressType(options_.compression_type));

  RETURN_NOT_OK(InitIpcWriteOptions());

  // Allocate first buffer for split reducer
  // when partitioner is SinglePart, don`t need init combine_buffer_
  if (options_.partitioning_name != "single") {
    ARROW_ASSIGN_OR_RAISE(combine_buffer_, arrow::AllocateResizableBuffer(0, options_.memory_pool.get()));
    RETURN_NOT_OK(combine_buffer_->Resize(0, /*shrink_to_fit =*/false));
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::InitIpcWriteOptions() {
  auto& ipc_write_options = options_.ipc_write_options;
  ipc_write_options.memory_pool = options_.memory_pool.get();
  ipc_write_options.use_threads = false;

  tiny_batch_write_options_ = ipc_write_options;
  tiny_batch_write_options_.codec = nullptr;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::InitPartitions(const velox::RowVector& rv) {
  auto simple_column_count = simple_column_indices_.size();

  partition_validity_addrs_.resize(simple_column_count);
  std::for_each(partition_validity_addrs_.begin(), partition_validity_addrs_.end(), [this](std::vector<uint8_t*>& v) {
    v.resize(num_partitions_, nullptr);
  });

  partition_fixed_width_value_addrs_.resize(fixed_width_column_count_);
  std::for_each(
      partition_fixed_width_value_addrs_.begin(),
      partition_fixed_width_value_addrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(num_partitions_, nullptr); });

  partition_buffers_.resize(simple_column_count);
  std::for_each(partition_buffers_.begin(), partition_buffers_.end(), [this](std::vector<arrow::BufferVector>& v) {
    v.resize(num_partitions_);
  });

  partition_binary_addrs_.resize(binary_column_indices_.size());
  std::for_each(partition_binary_addrs_.begin(), partition_binary_addrs_.end(), [this](std::vector<BinaryBuff>& v) {
    v.resize(num_partitions_);
  });

  partition_list_builders_.resize(complex_column_indices_.size());
  for (size_t i = 0; i < complex_column_indices_.size(); ++i) {
    partition_list_builders_[i].resize(num_partitions_);
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SetCompressType(arrow::Compression::type compressed_type) {
  ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec, CreateArrowIpcCodec(compressed_type));
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::Split(ColumnarBatch* cb) {
  auto veloxColumnBatch = dynamic_cast<VeloxColumnarBatch*>(cb);
  if (options_.partitioning_name == "single") {
    auto vp = veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(InitFromRowVector(*vp));
    // 1. convert RowVector to RecordBatch
    ArrowArray arrowArray;
    velox::exportToArrow(vp, arrowArray, GetDefaultLeafWrappedVeloxMemoryPool().get());

    auto result = arrow::ImportRecordBatch(&arrowArray, schema_);
    RETURN_NOT_OK(result);

    // 2. call CacheRecordBatch with RecordBatch
    RETURN_NOT_OK(CacheRecordBatch(0, *(*result)));
  } else {
    auto& rv = *veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(InitFromRowVector(rv));
    ARROW_ASSIGN_OR_RAISE(auto pid_arr, GetFirstColumn(rv));
    RETURN_NOT_OK(partitioner_->Compute(pid_arr, rv.size(), row_2_partition_, partition_2_row_count_));
    if (partitioner_->HasPid()) {
      auto stripped_rv = GetStrippedRowVector(rv);
      RETURN_NOT_OK(DoSplit(stripped_rv));
    } else {
      RETURN_NOT_OK(DoSplit(rv));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::Stop() {
  EVAL_START("write", options_.thread_id)
  RETURN_NOT_OK(partition_writer_->Stop());
  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::CreatePartition2Row(uint32_t row_num) {
  // calc partition_2_row_offset_
  partition_2_row_offset_[0] = 0;
  for (auto pid = 1; pid <= num_partitions_; ++pid) {
    partition_2_row_offset_[pid] = partition_2_row_offset_[pid - 1] + partition_2_row_count_[pid - 1];
  }

  // get a copy of partition_2_row_offset_
  auto partition_2_row_offset_copy = partition_2_row_offset_;

  // calc row_offset_2_row_id_
  row_offset_2_row_id_.resize(row_num);
  for (auto row = 0; row < row_num; ++row) {
    auto pid = row_2_partition_[row];
    row_offset_2_row_id_[partition_2_row_offset_copy[pid]++] = row;
  }

  PrintPartition2Row();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::UpdateInputHasNull(const velox::RowVector& rv) {
  for (size_t col = 0; col < simple_column_indices_.size(); ++col) {
    // check input_has_null_[col] is cheaper than GetNullCount()
    // once input_has_null_ is set to true, we didn't reset it after evict
    if (!input_has_null_[col]) {
      auto col_idx = simple_column_indices_[col];
      if (VectorHasNull(rv.childAt(col_idx))) {
        input_has_null_[col] = true;
      }
    }
  }

  PrintInputHasNull();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::DoSplit(const velox::RowVector& rv) {
  auto row_num = rv.size();

  RETURN_NOT_OK(CreatePartition2Row(row_num));

  RETURN_NOT_OK(UpdateInputHasNull(rv));

  for (auto pid = 0; pid < num_partitions_; ++pid) {
    if (partition_2_row_count_[pid] > 0) {
      // make sure the size to be allocated is larger than the size to be filled
      if (partition_2_buffer_size_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto new_size = std::max(CalculatePartitionBufferSize(rv), partition_2_row_count_[pid]);
        RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
      } else if (partition_buffer_idx_base_[pid] + partition_2_row_count_[pid] > partition_2_buffer_size_[pid]) {
        auto new_size = std::max(CalculatePartitionBufferSize(rv), partition_2_row_count_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to allocate new buffer
        if (options_.prefer_evict) {
          // if prefer_evict is set, evict current RowVector, we may reuse the buffers
          if (new_size > partition_2_buffer_size_[pid]) {
            // if the partition size after split is already larger than
            // allocated buffer size, need reallocate
            RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, /*reset_buffers = */ true));

            // splill immediately
            RETURN_NOT_OK(EvictPartition(pid));
            RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
          } else {
            // partition size after split is smaller than buffer size, no need
            // to reset buffer, reuse it.
            RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, /*reset_buffers = */ false));
            RETURN_NOT_OK(EvictPartition(pid));
          }
        } else {
          // if prefer_evict is disabled, cache the record batch
          RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, /*reset_buffers = */ true));
          // allocate partition buffer with retries
          RETURN_NOT_OK(AllocateNew(pid, new_size));
        }
      }
    }
  }

  PrintPartitionBuffer();

  RETURN_NOT_OK(SplitRowVector(rv));

  // update partition buffer base after split
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    partition_buffer_idx_base_[pid] += partition_2_row_count_[pid];
  }

  PrintPartitionBuffer();

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitRowVector(const velox::RowVector& rv) {
  // now start to split the RowVector
  RETURN_NOT_OK(SplitFixedWidthValueBuffer(rv));
  RETURN_NOT_OK(SplitValidityBuffer(rv));
  RETURN_NOT_OK(SplitBinaryArray(rv));
  RETURN_NOT_OK(SplitListArray(rv));
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitFixedWidthValueBuffer(const velox::RowVector& rv) {
  for (auto col = 0; col < fixed_width_column_count_; ++col) {
    auto col_idx = simple_column_indices_[col];
    auto column = rv.childAt(col_idx);
    assert(column->isFlatEncoding());

    const uint8_t* src_addr = (const uint8_t*)column->valuesAsVoid();
    const auto& dst_addrs = partition_fixed_width_value_addrs_[col];

    switch (arrow::bit_width(arrow_column_types_[col_idx]->id())) {
      case 8:
        RETURN_NOT_OK(SplitFixedType<uint8_t>(src_addr, dst_addrs));
        break;
      case 16:
        RETURN_NOT_OK(SplitFixedType<uint16_t>(src_addr, dst_addrs));
        break;
      case 32:
        RETURN_NOT_OK(SplitFixedType<uint32_t>(src_addr, dst_addrs));
        break;
      case 64:
#ifdef PROCESSAVX
        std::transform(
            dst_addrs.begin(),
            dst_addrs.end(),
            partition_buffer_idx_base_.begin(),
            partition_buffer_idx_offset_.begin(),
            [](uint8_t* x, row_offset_type y) { return x + y * sizeof(uint64_t); });
        for (auto pid = 0; pid < num_partitions_; pid++) {
          auto dst_pid_base = reinterpret_cast<uint64_t*>(partition_buffer_idx_offset_[pid]); /*32k*/
          auto r = partition_2_row_offset_[pid]; /*8k*/
          auto size = partition_2_row_offset_[pid + 1];
#if 1
          for (r; r < size && (((uint64_t)dst_pid_base & 0x1f) > 0); r++) {
            auto src_offset = row_offset_2_row_id_[r]; /*16k*/
            *dst_pid_base = reinterpret_cast<uint64_t*>(src_addr)[src_offset]; /*64k*/
            _mm_prefetch(&(src_addr)[src_offset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 1;
          }
#if 0
          for (r; r+4<size; r+=4)                              
          {                                                                                    
            auto src_offset = row_offset_2_row_id_[r];                                 /*16k*/ 
            __m128i src_ld = _mm_loadl_epi64((__m128i*)(&row_offset_2_row_id_[r]));    
            __m128i src_offset_4x = _mm_cvtepu16_epi32(src_ld);
            
            __m256i src_4x = _mm256_i32gather_epi64((const long long int*)src_addr,src_offset_4x,8);
            //_mm256_store_si256((__m256i*)dst_pid_base,src_4x); 
            _mm_stream_si128((__m128i*)dst_pid_base,src_2x);
                                                         
            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r+1]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r+2]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r+3]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            dst_pid_base+=4;                                                                   
          }
#endif
          for (r; r + 2 < size; r += 2) {
            __m128i src_offset_2x = _mm_cvtsi32_si128(*((int32_t*)(row_offset_2_row_id_.data() + r)));
            src_offset_2x = _mm_shufflelo_epi16(src_offset_2x, 0x98);

            __m128i src_2x = _mm_i32gather_epi64((const long long int*)src_addr, src_offset_2x, 8);
            _mm_store_si128((__m128i*)dst_pid_base, src_2x);
            //_mm_stream_si128((__m128i*)dst_pid_base,src_2x);

            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            _mm_prefetch(&(src_addr)[(uint32_t)row_offset_2_row_id_[r + 1] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 2;
          }
#endif
          for (r; r < size; r++) {
            auto src_offset = row_offset_2_row_id_[r]; /*16k*/
            *dst_pid_base = reinterpret_cast<const uint64_t*>(src_addr)[src_offset]; /*64k*/
            _mm_prefetch(&(src_addr)[src_offset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 1;
          }
        }
        break;
#else
        RETURN_NOT_OK(SplitFixedType<uint64_t>(src_addr, dst_addrs));
#endif
        break;
#if defined(__x86_64__)
      case 128: // arrow::Decimal128Type::type_id
        // too bad gcc generates movdqa even we use __m128i_u data type.
        // SplitFixedType<__m128i_u>(src_addr, dst_addrs);
        {
          std::transform(
              dst_addrs.begin(),
              dst_addrs.end(),
              partition_buffer_idx_base_.begin(),
              partition_buffer_idx_offset_.begin(),
              [](uint8_t* x, row_offset_type y) { return x + y * sizeof(__m128i_u); });
          if (column->type()->isShortDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            for (auto pid = 0; pid < num_partitions_; pid++) {
              auto dst_pid_base = reinterpret_cast<__m128i_u*>(partition_buffer_idx_offset_[pid]); /*32k*/
              auto r = partition_2_row_offset_[pid]; /*8k*/
              auto size = partition_2_row_offset_[pid + 1];
              for (; r < size; r++) {
                auto src_offset = row_offset_2_row_id_[r]; /*16k*/
                const int64_t value = *(reinterpret_cast<const int64_t*>(src_addr) + src_offset);
                memcpy(dst_pid_base, &value, sizeof(int64_t));
                dst_pid_base += 1;
              }
            }
          } else if (column->type()->isLongDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            for (auto pid = 0; pid < num_partitions_; pid++) {
              auto dst_pid_base = reinterpret_cast<__m128i_u*>(partition_buffer_idx_offset_[pid]); /*32k*/
              auto r = partition_2_row_offset_[pid]; /*8k*/
              auto size = partition_2_row_offset_[pid + 1];
              for (; r < size; r++) {
                auto src_offset = row_offset_2_row_id_[r]; /*16k*/
                __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i_u*>(src_addr) + src_offset);
                _mm_storeu_si128(dst_pid_base, value);
                _mm_prefetch(src_addr + src_offset * sizeof(__m128i_u) + 64, _MM_HINT_T2);
                dst_pid_base += 1;
              }
            }
          } else {
            return arrow::Status::Invalid(
                "Column type " + schema_->field(col_idx)->type()->ToString() + " is not supported.");
          }
        }
#elif defined(__aarch64__)
      case 128:
        if (column->type()->isShortDecimal()) {
          std::transform(
              dst_addrs.begin(),
              dst_addrs.end(),
              partition_buffer_idx_base_.begin(),
              partition_buffer_idx_offset_.begin(),
              [](uint8_t* x, uint32_t y) { return x + y * sizeof(uint32x4_t); });
          for (uint32_t pid = 0; pid < num_partitions_; ++pid) {
            auto dst_pid_base = reinterpret_cast<uint32x4_t*>(partition_buffer_idx_offset_[pid]);
            auto pos = partition_2_row_offset_[pid];
            auto end = partition_2_row_offset_[pid + 1];
            for (; pos < end; ++pos) {
              auto row_id = row_offset_2_row_id_[pos];
              const uint64_t value = reinterpret_cast<const uint64_t*>(src_addr)[row_id]; // copy
              memcpy(dst_pid_base, &value, sizeof(uint64_t));
              dst_pid_base += 1;
            }
          }
        } else if (column->type()->isLongDecimal()) {
          RETURN_NOT_OK(SplitFixedType<uint32x4_t>(src_addr, dst_addrs));
        } else {
          return arrow::Status::Invalid(
              "Column type " + schema_->field(col_idx)->type()->ToString() + " is not supported.");
        }
#endif
        break;
      case 1: // arrow::BooleanType::type_id:
        RETURN_NOT_OK(SplitBoolType(src_addr, dst_addrs));
        break;
      default:
        return arrow::Status::Invalid(
            "Column type " + schema_->field(col_idx)->type()->ToString() + " is not fixed width");
    }
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitBoolType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs) {
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    // set the last byte
    auto dstaddr = dst_addrs[pid];
    if (partition_2_row_count_[pid] > 0 && dstaddr != nullptr) {
      auto r = partition_2_row_offset_[pid]; /*8k*/
      auto size = partition_2_row_offset_[pid + 1];
      uint32_t dst_offset = partition_buffer_idx_base_[pid];
      uint32_t dst_offset_in_byte = (8 - (dst_offset & 0x7)) & 0x7;
      uint32_t dst_idx_byte = dst_offset_in_byte;
      uint8_t dst = dstaddr[dst_offset >> 3];
      if (pid + 1 < num_partitions_) {
        PREFETCHT1((&dstaddr[partition_buffer_idx_base_[pid + 1] >> 3]));
      }
      for (; r < size && dst_idx_byte > 0; r++, dst_idx_byte--) {
        auto src_offset = row_offset_2_row_id_[r]; /*16k*/
        uint8_t src = src_addr[src_offset >> 3];
        src = src >> (src_offset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, 8 - dst_idx_byte);
#else
        src = rotateLeft(src, (8 - dst_idx_byte));
#endif
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dst_offset >> 3] = dst;
      if (r == size) {
        continue;
      }
      dst_offset += dst_offset_in_byte;
      // now dst_offset is 8 aligned
      for (; r + 8 < size; r += 8) {
        uint8_t src = 0;
        auto src_offset = row_offset_2_row_id_[r]; /*16k*/
        src = src_addr[src_offset >> 3];
        // PREFETCHT0((&(src_addr)[(src_offset >> 3) + 64]));
        dst = src >> (src_offset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 1]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 1 | 0xfd; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 2]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 2 | 0xfb; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 3]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 3 | 0xf7; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 4]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 4 | 0xef; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 5]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 5 | 0xdf; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 6]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 6 | 0xbf; // get the bit in bit 0, other bits set to 1

        src_offset = row_offset_2_row_id_[r + 7]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 7 | 0x7f; // get the bit in bit 0, other bits set to 1

        dstaddr[dst_offset >> 3] = dst;
        dst_offset += 8;
        //_mm_prefetch(dstaddr + (dst_offset >> 3) + 64, _MM_HINT_T0);
      }
      // last byte, set it to 0xff is ok
      dst = 0xff;
      dst_idx_byte = 0;
      for (; r < size; r++, dst_idx_byte++) {
        auto src_offset = row_offset_2_row_id_[r]; /*16k*/
        uint8_t src = src_addr[src_offset >> 3];
        src = src >> (src_offset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, dst_idx_byte);
#else
        src = rotateLeft(src, dst_idx_byte);
#endif
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dst_offset >> 3] = dst;
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitValidityBuffer(const velox::RowVector& rv) {
  for (size_t col = 0; col < simple_column_indices_.size(); ++col) {
    auto col_idx = simple_column_indices_[col];
    auto column = rv.childAt(col_idx);
    if (VectorHasNull(column)) {
      auto& dst_addrs = partition_validity_addrs_[col];
      for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (partition_2_row_count_[pid] > 0 && dst_addrs[pid] == nullptr) {
          // init bitmap if it's null, initialize the buffer as true
          auto new_size = std::max(partition_2_row_count_[pid], (uint32_t)options_.buffer_size);
          std::shared_ptr<arrow::Buffer> validity_buffer;
          auto status = AllocateBufferFromPool(validity_buffer, arrow::bit_util::BytesForBits(new_size));
          ARROW_RETURN_NOT_OK(status);
          dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data());
          memset(validity_buffer->mutable_data(), 0xff, validity_buffer->capacity());
          partition_buffers_[col][pid][VALIDITY_BUFFER_INDEX] = std::move(validity_buffer);
        }
      }

      auto src_addr = (const uint8_t*)(column->mutableRawNulls());
      RETURN_NOT_OK(SplitBoolType(src_addr, dst_addrs));
    } else {
      VsPrintLF(col_idx, " column hasn't null");
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitBinaryType(
    uint32_t binary_idx,
    const velox::FlatVector<velox::StringView>& src,
    std::vector<BinaryBuff>& dst) {
  auto raw_values = src.rawValues();

  for (auto pid = 0; pid < num_partitions_; ++pid) {
    auto& binary_buf = dst[pid];

    // use 32bit offset
    using offset_type = arrow::BinaryType::offset_type;
    auto dst_offset_base = (offset_type*)(binary_buf.offset_ptr) + partition_buffer_idx_base_[pid];

    auto value_offset = binary_buf.value_offset;
    auto dst_value_ptr = binary_buf.value_ptr + value_offset;
    auto capacity = binary_buf.value_capacity;

    auto r = partition_2_row_offset_[pid];
    auto size = partition_2_row_offset_[pid + 1] - r;
    auto multiply = 1;

    for (uint32_t x = 0; x < size; x++) {
      auto row_id = row_offset_2_row_id_[x + r];
      auto& string_view = raw_values[row_id];
      auto string_len = string_view.size();

      // 1. copy offset
      value_offset = dst_offset_base[x + 1] = value_offset + string_len;

      if (value_offset >= capacity) {
        auto old_capacity = capacity;
        (void)old_capacity; // suppress warning
        capacity = capacity + std::max((capacity >> multiply), (uint64_t)string_len);
        multiply = std::min(3, multiply + 1);

        auto value_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            partition_buffers_[fixed_width_column_count_ + binary_idx][pid][VALUE_BUFFER_INEDX]);

        RETURN_NOT_OK(value_buffer->Reserve(capacity));

        binary_buf.value_ptr = value_buffer->mutable_data();
        binary_buf.value_capacity = capacity;
        dst_value_ptr = binary_buf.value_ptr + value_offset - string_len;

        VsPrintSplit("Split value buffer resized col_idx", binary_idx);
        VsPrintSplit(" dst_start", dst_offset_base[x]);
        VsPrintSplit(" dst_end", dst_offset_base[x + 1]);
        VsPrintSplit(" old size", old_capacity);
        VsPrintSplit(" new size", capacity);
        VsPrintSplit(" row", partition_buffer_idx_base_[pid]);
        VsPrintSplitLF(" string len", string_len);
      }

      // 2. copy value
      memcpy(dst_value_ptr, string_view.data(), string_len);

      dst_value_ptr += string_len;
    }

    binary_buf.value_offset = value_offset;
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitListArray(const velox::RowVector& rv) {
  for (size_t i = 0; i < complex_column_indices_.size(); ++i) {
    auto col_idx = complex_column_indices_[i];
    auto column = rv.childAt(col_idx);

    // TODO: rethink the cost of `exportToArrow+ImportArray`
    ArrowArray arrowArray;
    velox::exportToArrow(column, arrowArray, GetDefaultLeafWrappedVeloxMemoryPool().get());

    auto result = arrow::ImportArray(&arrowArray, arrow_column_types_[col_idx]);
    RETURN_NOT_OK(result);

    auto num_rows = rv.size();
    for (auto row = 0; row < num_rows; ++row) {
      auto partition = row_2_partition_[row];
      RETURN_NOT_OK(partition_list_builders_[i][partition]->AppendArraySlice(*((*result)->data().get()), row, 1));
    }
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::SplitBinaryArray(const velox::RowVector& rv) {
  for (auto col = fixed_width_column_count_; col < simple_column_indices_.size(); ++col) {
    auto binary_idx = col - fixed_width_column_count_;
    auto& dst_addrs = partition_binary_addrs_[binary_idx];
    auto col_idx = simple_column_indices_[col];
    auto column = rv.childAt(col_idx);
    auto type_kind = column->typeKind();
    if (type_kind == velox::TypeKind::VARCHAR || type_kind == velox::TypeKind::VARBINARY) {
      auto string_column = column->asFlatVector<velox::StringView>();
      assert(string_column);
      RETURN_NOT_OK(SplitBinaryType(binary_idx, *string_column, dst_addrs));
    } else {
      VsPrintLF("INVALID TYPE: neither VARCHAR nor VARBINARY!");
      assert(false);
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::VeloxType2ArrowSchema(const velox::TypePtr& type) {
  auto out = std::make_shared<ArrowSchema>();
  auto rvp = velox::RowVector::createEmpty(type, GetDefaultLeafWrappedVeloxMemoryPool().get());

  // get ArrowSchema from velox::RowVector
  velox::exportToArrow(rvp, *out);

  // convert ArrowSchema to arrow::Schema
  ARROW_ASSIGN_OR_RAISE(schema_, arrow::ImportSchema(out.get()));

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::InitColumnTypes(const velox::RowVector& rv) {
  RETURN_NOT_OK(VeloxType2ArrowSchema(rv.type()));

  // remove the first column
  if (partitioner_->HasPid()) {
    ARROW_ASSIGN_OR_RAISE(schema_, schema_->RemoveField(0));
    // skip the first column
    for (size_t i = 1; i < rv.childrenSize(); ++i) {
      velox_column_types_.push_back(rv.childAt(i)->type());
    }
  } else {
    if (velox_column_types_.empty()) {
      for (size_t i = 0; i < rv.childrenSize(); ++i) {
        velox_column_types_.push_back(rv.childAt(i)->type());
      }
    }
  }

  VsPrintSplitLF("schema_", schema_->ToString());

  // get arrow_column_types_ from schema
  ARROW_ASSIGN_OR_RAISE(arrow_column_types_, ToShuffleWriterTypeId(schema_->fields()));

  for (size_t i = 0; i < arrow_column_types_.size(); ++i) {
    switch (arrow_column_types_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        binary_column_indices_.push_back(i);
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        complex_column_indices_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
      default:
        simple_column_indices_.push_back(i);
        break;
    }
  }

  fixed_width_column_count_ = simple_column_indices_.size();

  simple_column_indices_.insert(
      simple_column_indices_.end(), binary_column_indices_.begin(), binary_column_indices_.end());

  PrintColumnsInfo();

  binary_array_empirical_size_.resize(binary_column_indices_.size(), 0);

  input_has_null_.resize(simple_column_indices_.size(), false);

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::InitFromRowVector(const velox::RowVector& rv) {
  if (velox_column_types_.empty()) {
    RETURN_NOT_OK(InitColumnTypes(rv));
    RETURN_NOT_OK(InitPartitions(rv));
  }
  return arrow::Status::OK();
}

velox::RowVector VeloxShuffleWriter::GetStrippedRowVector(const velox::RowVector& rv) const {
  // get new row type
  auto row_type = rv.type()->asRow();
  auto type_children = row_type.children();
  type_children.erase(type_children.begin());
  auto new_row_type = velox::ROW(std::move(type_children));

  // get null buffers
  const auto& nullbuffers = rv.nulls();

  // get length
  auto length = rv.size();

  // get children
  auto children = rv.children();
  children.erase(children.begin());

  // get nullcount
  auto nullcount = rv.getNullCount();

  return velox::RowVector(rv.pool(), new_row_type, nullbuffers, length, children, nullcount);
}

uint32_t VeloxShuffleWriter::CalculatePartitionBufferSize(const velox::RowVector& rv) {
  uint32_t size_per_row = 0;
  auto num_rows = rv.size();
  for (size_t i = fixed_width_column_count_; i < simple_column_indices_.size(); ++i) {
    auto index = i - fixed_width_column_count_;
    if (binary_array_empirical_size_[index] == 0) {
      auto column = rv.childAt(simple_column_indices_[i]);
      auto string_view_column = column->asFlatVector<velox::StringView>();
      assert(string_view_column);

      // accumulate length
      uint64_t length = 0;
      auto string_views = string_view_column->rawValues<velox::StringView>();
      for (size_t row = 0; row != num_rows; ++row) {
        length += string_views[row].size();
      }

      binary_array_empirical_size_[index] = length % num_rows == 0 ? length / num_rows : length / num_rows + 1;
    }
  }

  VS_PRINT_VECTOR_MAPPING(binary_array_empirical_size_);

  size_per_row = std::accumulate(binary_array_empirical_size_.begin(), binary_array_empirical_size_.end(), 0);

  for (size_t col = 0; col < simple_column_indices_.size(); ++col) {
    auto col_idx = simple_column_indices_[col];
    // `bool(1) >> 3` gets 0, so +7
    size_per_row += ((arrow::bit_width(arrow_column_types_[col_idx]->id()) + 7) >> 3);
  }

  VS_PRINTLF(size_per_row);

  uint64_t prealloc_row_cnt = options_.offheap_per_task > 0 && size_per_row > 0
      ? options_.offheap_per_task / size_per_row / num_partitions_ >> 2
      : options_.buffer_size;
  prealloc_row_cnt = std::min(prealloc_row_cnt, (uint64_t)options_.buffer_size);

  VS_PRINTLF(prealloc_row_cnt);

  return prealloc_row_cnt;
}

arrow::Status VeloxShuffleWriter::AllocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size) {
  // if size is already larger than buffer pool size, allocate it directly
  // make size 64byte aligned
  auto reminder = size & 0x3f;
  size += (64 - reminder) & ((reminder == 0) - 1);
  if (size > SPLIT_BUFFER_SIZE) {
    ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(size, options_.memory_pool.get()));
    return arrow::Status::OK();
  } else if (combine_buffer_->capacity() - combine_buffer_->size() < size) {
    // memory pool is not enough
    ARROW_ASSIGN_OR_RAISE(
        combine_buffer_, arrow::AllocateResizableBuffer(SPLIT_BUFFER_SIZE, options_.memory_pool.get()));
    RETURN_NOT_OK(combine_buffer_->Resize(0, /*shrink_to_fit = */ false));
  }
  buffer = arrow::SliceMutableBuffer(combine_buffer_, combine_buffer_->size(), size);
  RETURN_NOT_OK(combine_buffer_->Resize(combine_buffer_->size() + size, /*shrink_to_fit = */ false));
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::AllocatePartitionBuffers(uint32_t partition_id, uint32_t new_size) {
  // try to allocate new
  auto num_fields = schema_->num_fields();
  assert(num_fields == arrow_column_types_.size());

  auto fixed_width_idx = 0;
  auto binary_idx = 0;
  auto list_idx = 0;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> new_list_builders;

  for (auto i = 0; i < num_fields; ++i) {
    size_t sizeof_binary_offset = -1;
    switch (arrow_column_types_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeof_binary_offset = sizeof(arrow::StringType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeof_binary_offset == -1) {
          sizeof_binary_offset = sizeof(arrow::LargeStringType::offset_type);
        }

        std::shared_ptr<arrow::Buffer> offset_buffer;
        std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
        auto value_buf_size = binary_array_empirical_size_[binary_idx] * new_size + 1024;
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Buffer> value_buffer,
            arrow::AllocateResizableBuffer(value_buf_size, options_.memory_pool.get()));
        ARROW_RETURN_NOT_OK(AllocateBufferFromPool(offset_buffer, new_size * sizeof_binary_offset + 1));

        // set the first offset to 0
        uint8_t* offsetaddr = offset_buffer->mutable_data();
        memset(offsetaddr, 0, 8);

        partition_binary_addrs_[binary_idx][partition_id] =
            BinaryBuff(value_buffer->mutable_data(), offset_buffer->mutable_data(), value_buf_size);

        auto index = fixed_width_column_count_ + binary_idx;
        if (input_has_null_[index]) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(validity_buffer, arrow::bit_util::BytesForBits(new_size)));
          // initialize all true once allocated
          memset(validity_buffer->mutable_data(), 0xff, validity_buffer->capacity());
          partition_validity_addrs_[index][partition_id] = validity_buffer->mutable_data();
        } else {
          partition_validity_addrs_[index][partition_id] = nullptr;
        }
        partition_buffers_[index][partition_id] = {
            std::move(validity_buffer), std::move(offset_buffer), std::move(value_buffer)};
        binary_idx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        RETURN_NOT_OK(MakeBuilder(options_.memory_pool.get(), arrow_column_types_[i], &array_builder));
        assert(array_builder != nullptr);
        RETURN_NOT_OK(array_builder->Reserve(new_size));
        partition_list_builders_[list_idx][partition_id] = std::move(array_builder);
        list_idx++;
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: {
        std::shared_ptr<arrow::Buffer> value_buffer;
        std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
        if (arrow_column_types_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(value_buffer, arrow::bit_util::BytesForBits(new_size)));
        } else {
          ARROW_RETURN_NOT_OK(
              AllocateBufferFromPool(value_buffer, new_size * (arrow::bit_width(arrow_column_types_[i]->id()) >> 3)));
        }
        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = value_buffer->mutable_data();

        if (input_has_null_[fixed_width_idx]) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(validity_buffer, arrow::bit_util::BytesForBits(new_size)));
          // initialize all true once allocated
          memset(validity_buffer->mutable_data(), 0xff, validity_buffer->capacity());
          partition_validity_addrs_[fixed_width_idx][partition_id] = validity_buffer->mutable_data();
        } else {
          partition_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
        }
        partition_buffers_[fixed_width_idx][partition_id] = {std::move(validity_buffer), std::move(value_buffer)};
        fixed_width_idx++;
        break;
      }
    }
  }

  partition_2_buffer_size_[partition_id] = new_size;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::AllocateNew(uint32_t partition_id, uint32_t new_size) {
  auto retry = 0;
  auto status = AllocatePartitionBuffers(partition_id, new_size);
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    ++retry;
    std::cout << status.ToString() << std::endl
              << std::to_string(retry) << " retry to allocate new buffer for partition " << std::to_string(partition_id)
              << std::endl;

    int64_t evicted_size;
    ARROW_ASSIGN_OR_RAISE(auto partition_to_evict, EvictLargestPartition(&evicted_size));
    if (partition_to_evict == -1) {
      std::cout << "Failed to allocate new buffer for partition " << std::to_string(partition_id)
                << ". No partition buffer to evict." << std::endl;
      return status;
    }

    status = AllocatePartitionBuffers(partition_id, new_size);
  }

  if (status.IsOutOfMemory()) {
    std::cout << "Failed to allocate new buffer for partition " << std::to_string(partition_id) << ". Out of memory."
              << std::endl;
  }

  return status;
}

arrow::Status VeloxShuffleWriter::CreateRecordBatchFromBuffer(uint32_t partition_id, bool reset_buffers) {
  if (partition_buffer_idx_base_[partition_id] <= 0) {
    return arrow::Status::OK();
  }

  // already filled
  auto fixed_width_idx = 0;
  auto binary_idx = 0;
  auto list_idx = 0;
  auto num_fields = schema_->num_fields();
  auto num_rows = partition_buffer_idx_base_[partition_id];

  std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
  for (int i = 0; i < num_fields; ++i) {
    size_t sizeof_binary_offset = -1;
    switch (arrow_column_types_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeof_binary_offset = sizeof(arrow::BinaryType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeof_binary_offset == -1)
          sizeof_binary_offset = sizeof(arrow::LargeBinaryType::offset_type);

        auto buffers = partition_buffers_[fixed_width_column_count_ + binary_idx][partition_id];
        // validity buffer
        if (buffers[VALIDITY_BUFFER_INDEX] != nullptr) {
          buffers[VALIDITY_BUFFER_INDEX] =
              arrow::SliceBuffer(buffers[VALIDITY_BUFFER_INDEX], 0, arrow::bit_util::BytesForBits(num_rows));
        }
        // offset buffer
        if (buffers[OFFSET_BUFFER_INDEX] != nullptr) {
          buffers[OFFSET_BUFFER_INDEX] =
              arrow::SliceBuffer(buffers[OFFSET_BUFFER_INDEX], 0, (num_rows + 1) * sizeof_binary_offset);
        }
        // value buffer
        if (buffers[VALUE_BUFFER_INEDX] != nullptr) {
          ARROW_CHECK_NE(buffers[OFFSET_BUFFER_INDEX], nullptr);
          buffers[VALUE_BUFFER_INEDX] = arrow::SliceBuffer(
              buffers[VALUE_BUFFER_INEDX],
              0,
              sizeof_binary_offset == 4 ? reinterpret_cast<const arrow::BinaryType::offset_type*>(
                                              buffers[OFFSET_BUFFER_INDEX]->data())[num_rows]
                                        : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(
                                              buffers[OFFSET_BUFFER_INDEX]->data())[num_rows]);
        }

        arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
            schema_->field(i)->type(),
            num_rows,
            {buffers[VALIDITY_BUFFER_INDEX], buffers[OFFSET_BUFFER_INDEX], buffers[VALUE_BUFFER_INEDX]}));

        uint64_t dst_offset0 = sizeof_binary_offset == 4
            ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[OFFSET_BUFFER_INDEX]->data())[0]
            : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[OFFSET_BUFFER_INDEX]->data())[0];
        ARROW_CHECK_EQ(dst_offset0, 0);

        if (reset_buffers) {
          partition_validity_addrs_[fixed_width_column_count_ + binary_idx][partition_id] = nullptr;
          partition_binary_addrs_[binary_idx][partition_id] = BinaryBuff();
          partition_buffers_[fixed_width_column_count_ + binary_idx][partition_id].clear();
        } else {
          // reset the offset
          partition_binary_addrs_[binary_idx][partition_id].value_offset = 0;
        }
        binary_idx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        auto& builder = partition_list_builders_[list_idx][partition_id];
        if (reset_buffers) {
          RETURN_NOT_OK(builder->Finish(&arrays[i]));
          builder->Reset();
        } else {
          RETURN_NOT_OK(builder->Finish(&arrays[i]));
          builder->Reset();
          RETURN_NOT_OK(builder->Reserve(num_rows));
        }
        list_idx++;
        break;
      }
      case arrow::NullType::type_id: {
        arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(arrow::null(), num_rows, {nullptr, nullptr}, num_rows));
        break;
      }
      default: {
        auto buffers = partition_buffers_[fixed_width_idx][partition_id];
        if (buffers[0] != nullptr) {
          buffers[0] = arrow::SliceBuffer(buffers[0], 0, arrow::bit_util::BytesForBits(num_rows));
        }
        if (buffers[1] != nullptr) {
          if (arrow_column_types_[i]->id() == arrow::BooleanType::type_id)
            buffers[1] = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(num_rows));
          else
            buffers[1] =
                arrow::SliceBuffer(buffers[1], 0, num_rows * (arrow::bit_width(arrow_column_types_[i]->id()) >> 3));
        }

        arrays[i] =
            arrow::MakeArray(arrow::ArrayData::Make(schema_->field(i)->type(), num_rows, {buffers[0], buffers[1]}));
        if (reset_buffers) {
          partition_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
          partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = nullptr;
          partition_buffers_[fixed_width_idx][partition_id].clear();
        }
        fixed_width_idx++;
        break;
      }
    }
  }
  auto rb = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
  return CacheRecordBatch(partition_id, *rb);
}

namespace {

int64_t get_batch_nbytes(const arrow::RecordBatch& rb) {
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

arrow::Status VeloxShuffleWriter::CacheRecordBatch(uint32_t partition_id, const arrow::RecordBatch& rb) {
  int64_t raw_size = get_batch_nbytes(rb);
  raw_partition_lengths_[partition_id] += raw_size;
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
#ifndef SKIPCOMPRESS
  if (rb.num_rows() <= (uint32_t)options_.batch_compress_threshold) {
    TIME_NANO_OR_RAISE(
        total_compress_time_, arrow::ipc::GetRecordBatchPayload(rb, tiny_batch_write_options_, payload.get()));
  } else {
    TIME_NANO_OR_RAISE(
        total_compress_time_, arrow::ipc::GetRecordBatchPayload(rb, options_.ipc_write_options, payload.get()));
  }
#else
  // for test reason
  TIME_NANO_OR_RAISE(
      total_compress_time_, arrow::ipc::GetRecordBatchPayload(*rb, tiny_bach_write_options_, payload.get()));
#endif

  partition_cached_recordbatch_size_[partition_id] += payload->body_length;
  partition_cached_recordbatch_[partition_id].push_back(std::move(payload));
  partition_buffer_idx_base_[partition_id] = 0;
  return arrow::Status::OK();
}

arrow::Status VeloxShuffleWriter::EvictFixedSize(int64_t size, int64_t* actual) {
  int64_t current_evicted = 0L;
  auto try_count = 0;
  while (current_evicted < size && try_count < 5) {
    try_count++;
    int64_t single_call_evicted;
    ARROW_ASSIGN_OR_RAISE(int32_t evicted_partition_id, EvictLargestPartition(&single_call_evicted))
    if (evicted_partition_id == -1) {
      break;
    }
    current_evicted += single_call_evicted;
  }
  *actual = current_evicted;
  return arrow::Status::OK();
}

arrow::Result<int32_t> VeloxShuffleWriter::EvictLargestPartition(int64_t* size) {
  // evict the largest partition
  auto max_size = 0;
  int32_t partition_to_evict = -1;
  for (auto i = 0; i < num_partitions_; ++i) {
    if (partition_cached_recordbatch_size_[i] > max_size) {
      max_size = partition_cached_recordbatch_size_[i];
      partition_to_evict = i;
    }
  }
  if (partition_to_evict != -1) {
    RETURN_NOT_OK(EvictPartition(partition_to_evict));
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "Evicted partition " << std::to_string(partition_to_evict) << ", " << std::to_string(max_size)
              << " bytes released" << std::endl;
#endif
    *size = max_size;
  } else {
    *size = 0;
  }
  return partition_to_evict;
}

arrow::Status VeloxShuffleWriter::EvictPartition(uint32_t partition_id) {
  RETURN_NOT_OK(partition_writer_->EvictPartition(partition_id));

  // reset validity buffer after evict
  std::for_each(
      partition_buffers_.begin(), partition_buffers_.end(), [partition_id](std::vector<arrow::BufferVector>& bufs) {
        if (bufs[partition_id].size() != 0 && bufs[partition_id][0] != nullptr) {
          // initialize all true once allocated
          auto addr = bufs[partition_id][0]->mutable_data();
          memset(addr, 0xff, bufs[partition_id][0]->capacity());
        }
      });

  return arrow::Status::OK();
}

arrow::Result<const int32_t*> VeloxShuffleWriter::GetFirstColumn(const velox::RowVector& rv) {
  if (partitioner_->HasPid()) {
    if (rv.childrenSize() == 0) {
      return arrow::Status::Invalid("RowVector missing partition id column.");
    }

    auto& firstChild = rv.childAt(0);
    if (!firstChild->type()->isInteger()) {
      return arrow::Status::Invalid("RecordBatch field 0 should be integer");
    }

    // first column is partition key hash value
    using NativeType = velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType;
    auto pid_flat_vector = firstChild->asFlatVector<NativeType>();
    if (pid_flat_vector == nullptr) {
      return arrow::Status::Invalid("failed to cast rv.column(0), this column should be pid");
    }
    return pid_flat_vector->rawValues();
  } else {
    return nullptr;
  }
}

} // namespace gluten
