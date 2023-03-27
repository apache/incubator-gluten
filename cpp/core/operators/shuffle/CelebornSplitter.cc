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

#include "operators/shuffle/CelebornSplitter.h"

#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "utils/macros.h"

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

// ----------------------------------------------------------------------
// CelebornSplitter
arrow::Result<std::shared_ptr<CelebornSplitter>>
CelebornSplitter::Make(const std::string& short_name, int num_partitions, SplitOptions options) {
  if (short_name == "hash") {
    return CelebornHashSplitter::Create(num_partitions, std::move(options));
  } else if (short_name == "rr") {
    return CelebornRoundRobinSplitter::Create(num_partitions, std::move(options));
  } else if (short_name == "range") {
    return CelebornFallbackRangeSplitter::Create(num_partitions, std::move(options));
  } else if (short_name == "single") {
    return CelebornSinglePartSplitter::Create(1, std::move(options));
  }
  return arrow::Status::NotImplemented("Partitioning " + short_name + " not supported yet.");
}

arrow::Status CelebornSplitter::Stop() {
  EVAL_START("push", options_.thread_id)

  // push data and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, true));
    if (partition_cached_recordbatch_size_[pid] > 0) {
      RETURN_NOT_OK(PushPartition(pid));
    }
    total_bytes_written_ += partition_lengths_[pid];
  }
  this->combine_buffer_.reset();
  partition_buffers_.clear();

  EVAL_END("push", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

// call from memory management
arrow::Status CelebornSplitter::EvictFixedSize(int64_t size, int64_t* actual) {
  int64_t current_pushed = 0L;
  int32_t try_count = 0;
  while (current_pushed < size && try_count < 5) {
    try_count++;
    int64_t single_call_spilled;
    ARROW_ASSIGN_OR_RAISE(int32_t pushed_partition_id, PushLargestPartition(&single_call_spilled))
    if (pushed_partition_id == -1) {
      break;
    }
    current_pushed += single_call_spilled;
  }
  *actual = current_pushed;
  return arrow::Status::OK();
}

arrow::Status CelebornSplitter::PushPartition(int32_t partition_id) {
  int64_t temp_total_write_time = 0;
  int64_t temp_total_push_time = 0;
  TIME_NANO_OR_RAISE(temp_total_push_time, WriteArrowToOutputStream(partition_id));
  total_write_time_ += temp_total_write_time;
  TIME_NANO_OR_RAISE(temp_total_push_time, Push(partition_id));
  total_evict_time_ += temp_total_push_time;
  // reset validity buffer after push
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

arrow::Status CelebornSplitter::WriteArrowToOutputStream(int32_t partition_id) {
  ARROW_ASSIGN_OR_RAISE(
      celeborn_buffer_os_, arrow::io::BufferOutputStream::Create(options_.buffer_size, options_.memory_pool.get()));
  int32_t metadata_length = 0; // unused
#ifndef SKIPWRITE
  for (auto& payload : partition_cached_recordbatch_[partition_id]) {
    RETURN_NOT_OK(
        arrow::ipc::WriteIpcPayload(*payload, options_.ipc_write_options, celeborn_buffer_os_.get(), &metadata_length));
    payload = nullptr;
  }
#endif
  return arrow::Status::OK();
}

arrow::Status CelebornSplitter::Push(int32_t partition_id) {
  auto buffer = celeborn_buffer_os_->Finish();
  int32_t size = buffer->get()->size();
  char* dst = reinterpret_cast<char*>(buffer->get()->mutable_data());
  celeborn_client_->PushPartitonData(partition_id, dst, size);
  partition_cached_recordbatch_[partition_id].clear();
  partition_cached_recordbatch_size_[partition_id] = 0;
  partition_lengths_[partition_id] += size;
  return arrow::Status::OK();
}

arrow::Result<int32_t> CelebornSplitter::PushLargestPartition(int64_t* size) {
  // push the largest partition
  auto max_size = 0;
  int32_t partition_to_push = -1;
  for (auto i = 0; i < num_partitions_; ++i) {
    if (partition_cached_recordbatch_size_[i] > max_size) {
      max_size = partition_cached_recordbatch_size_[i];
      partition_to_push = i;
    }
  }
  if (partition_to_push != -1) {
    RETURN_NOT_OK(PushPartition(partition_to_push));
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "Pushed partition " << std::to_string(partition_to_push) << ", " << std::to_string(max_size)
              << " bytes released" << std::endl;
#endif
    *size = max_size;
  } else {
    *size = 0;
  }
  return partition_to_push;
}

arrow::Status CelebornSplitter::DoSplit(const arrow::RecordBatch& rb) {
  // buffer is allocated less than 64K
  // ARROW_CHECK_LE(rb.num_rows(),64*1024);

  reducer_offsets_.resize(rb.num_rows());

  reducer_offset_offset_[0] = 0;
  for (auto pid = 1; pid <= num_partitions_; pid++) {
    reducer_offset_offset_[pid] = reducer_offset_offset_[pid - 1] + partition_id_cnt_[pid - 1];
  }
  for (auto row = 0; row < rb.num_rows(); row++) {
    auto pid = partition_id_[row];
    reducer_offsets_[reducer_offset_offset_[pid]] = row;
    PREFETCHT0((reducer_offsets_.data() + reducer_offset_offset_[pid] + 32));
    reducer_offset_offset_[pid]++;
  }
  std::transform(
      reducer_offset_offset_.begin(),
      std::prev(reducer_offset_offset_.end()),
      partition_id_cnt_.begin(),
      reducer_offset_offset_.begin(),
      [](row_offset_type x, row_offset_type y) { return x - y; });
  // for the first input record batch, scan binary arrays and large binary
  // arrays to get their empirical sizes

  for (size_t col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    // check input_has_null_[col] is cheaper than GetNullCount()
    // once input_has_null_ is set to true, we didn't reset it after spill
    if (!input_has_null_[col] && rb.column_data(col_idx)->GetNullCount() != 0) {
      input_has_null_[col] = true;
    }
  }

  // prepare partition buffers and spill if necessary
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    if (partition_id_cnt_[pid] > 0) {
      // make sure the size to be allocated is larger than the size to be filled
      if (partition_buffer_size_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto new_size = std::max(CalculateSplitBatchSize(rb), partition_id_cnt_[pid]);
        RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
      } else if (partition_buffer_idx_base_[pid] + partition_id_cnt_[pid] > (unsigned)partition_buffer_size_[pid]) {
        auto new_size = std::max(CalculateSplitBatchSize(rb), partition_id_cnt_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to
        // allocate new buffer
        if (new_size > (unsigned)partition_buffer_size_[pid]) {
          RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, /*reset_buffers = */ true));
          RETURN_NOT_OK(PushPartition(pid));
          RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
        } else {
          RETURN_NOT_OK(CreateRecordBatchFromBuffer(pid, /*reset_buffers = */ false));
          RETURN_NOT_OK(PushPartition(pid));
        }
      }
    }
  }
  // now start to split the record batch
  RETURN_NOT_OK(SplitFixedWidthValueBuffer(rb));
  RETURN_NOT_OK(SplitValidityBuffer(rb));
  RETURN_NOT_OK(SplitBinaryArray(rb));
  RETURN_NOT_OK(SplitListArray(rb));

  // update partition buffer base after split
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    partition_buffer_idx_base_[pid] += partition_id_cnt_[pid];
  }

  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// CelebornRoundRobinSplitter

arrow::Result<std::shared_ptr<CelebornRoundRobinSplitter>> CelebornRoundRobinSplitter::Create(
    int32_t num_partitions,
    SplitOptions options) {
  std::shared_ptr<CelebornRoundRobinSplitter> res(new CelebornRoundRobinSplitter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status CelebornRoundRobinSplitter::ComputeAndCountPartitionId(const arrow::RecordBatch& rb) {
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  partition_id_.resize(rb.num_rows());
  for (auto& pid : partition_id_) {
    pid = pid_selection_;
    partition_id_cnt_[pid_selection_]++;
    pid_selection_ = (pid_selection_ + 1) == num_partitions_ ? 0 : (pid_selection_ + 1);
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// CelebornSinglePartSplitter

arrow::Result<std::shared_ptr<CelebornSinglePartSplitter>> CelebornSinglePartSplitter::Create(
    int32_t num_partitions,
    SplitOptions options) {
  std::shared_ptr<CelebornSinglePartSplitter> res(new CelebornSinglePartSplitter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status CelebornSinglePartSplitter::ComputeAndCountPartitionId(const arrow::RecordBatch& rb) {
  return arrow::Status::OK();
}

arrow::Status CelebornSinglePartSplitter::Init() {
  partition_writer_.resize(num_partitions_);
  partition_buffer_idx_base_.resize(num_partitions_);
  partition_cached_recordbatch_.resize(num_partitions_);
  partition_cached_recordbatch_size_.resize(num_partitions_);
  partition_lengths_.resize(num_partitions_);
  raw_partition_lengths_.resize(num_partitions_);

  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (options_.data_file.length() == 0) {
    ARROW_ASSIGN_OR_RAISE(options_.data_file, CreateTempShuffleFile(configured_dirs_[0]));
  }

  RETURN_NOT_OK(SetCompressType(options_.compression_type));

  auto& ipc_write_options = options_.ipc_write_options;
  ipc_write_options.memory_pool = options_.memory_pool.get();
  ipc_write_options.use_threads = false;

  // initialize tiny batch write options
  tiny_bach_write_options_ = ipc_write_options;
  tiny_bach_write_options_.codec = nullptr;

  return arrow::Status::OK();
}

arrow::Status CelebornSinglePartSplitter::Split(ColumnarBatch* batch) {
  ARROW_ASSIGN_OR_RAISE(
      auto rb, arrow::ImportRecordBatch(batch->exportArrowArray().get(), batch->exportArrowSchema().get()));
  EVAL_START("split", options_.thread_id)
  if (schema_ == nullptr) {
    schema_ = rb->schema();
    RETURN_NOT_OK(InitColumnType());
  }
  RETURN_NOT_OK(CacheRecordBatch(0, *rb));
  if (partition_cached_recordbatch_size_[0] > options_.push_buffer_max_size) {
    RETURN_NOT_OK(PushPartition(0));
  }

  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status CelebornSinglePartSplitter::Stop() {
  EVAL_START("push", options_.thread_id)

  // push data and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    if (partition_cached_recordbatch_size_[pid] > 0) {
      RETURN_NOT_OK(PushPartition(pid));
    }
    total_bytes_written_ += partition_lengths_[pid];
  }

  EVAL_END("push", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// CelebornHashSplitter

arrow::Result<std::shared_ptr<CelebornHashSplitter>> CelebornHashSplitter::Create(
    int32_t num_partitions,
    SplitOptions options) {
  std::shared_ptr<CelebornHashSplitter> res(new CelebornHashSplitter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status CelebornHashSplitter::ComputeAndCountPartitionId(const arrow::RecordBatch& rb) {
  if (rb.num_columns() == 0) {
    return arrow::Status::Invalid("Recordbatch missing partition id column.");
  }
  if (rb.column(0)->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid(
        "RecordBatch field 0 should be ", arrow::int32()->ToString(), ", actual is ", rb.column(0)->type()->ToString());
  }
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  // first column is partition key hash value
  auto pid_arr = std::dynamic_pointer_cast<arrow::Int32Array>(rb.column(0));
  if (pid_arr == nullptr) {
    return arrow::Status::Invalid("failed to cast rb.column(0), this column should be hash_partition_key");
  }
  for (auto i = 0; i < num_rows; ++i) {
    // positive mod
    auto pid = pid_arr->Value(i) % num_partitions_;
#if defined(__x86_64__)
    // force to generate ASM
    __asm__(
        "lea (%[num_partitions],%[pid],1),%[tmp]\n"
        "test %[pid],%[pid]\n"
        "cmovs %[tmp],%[pid]\n"
        : [pid] "+r"(pid)
        : [num_partitions] "r"(num_partitions_), [tmp] "r"(0));
#else
    if (pid < 0)
      pid += num_partitions_;
#endif
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}

arrow::Status CelebornHashSplitter::Split(ColumnarBatch* batch) {
  EVAL_START("split", options_.thread_id)
  ARROW_ASSIGN_OR_RAISE(
      auto rb, arrow::ImportRecordBatch(batch->exportArrowArray().get(), batch->exportArrowSchema().get()));
  RETURN_NOT_OK(ComputeAndCountPartitionId(*rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb->RemoveColumn(0));
  if (schema_ == nullptr) {
    schema_ = remove_pid->schema();
    RETURN_NOT_OK(InitColumnType());
  }
  RETURN_NOT_OK(DoSplit(*remove_pid));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// CelebornFallbackRangeSplitter

arrow::Result<std::shared_ptr<CelebornFallbackRangeSplitter>> CelebornFallbackRangeSplitter::Create(
    int32_t num_partitions,
    SplitOptions options) {
  auto res = std::shared_ptr<CelebornFallbackRangeSplitter>(
      new CelebornFallbackRangeSplitter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status CelebornFallbackRangeSplitter::Split(ColumnarBatch* batch) {
  EVAL_START("split", options_.thread_id)
  ARROW_ASSIGN_OR_RAISE(
      auto rb, arrow::ImportRecordBatch(batch->exportArrowArray().get(), batch->exportArrowSchema().get()));
  RETURN_NOT_OK(ComputeAndCountPartitionId(*rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb->RemoveColumn(0));
  if (schema_ == nullptr) {
    schema_ = remove_pid->schema();
    RETURN_NOT_OK(InitColumnType());
  }
  RETURN_NOT_OK(DoSplit(*remove_pid));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status CelebornFallbackRangeSplitter::ComputeAndCountPartitionId(const arrow::RecordBatch& rb) {
  if (rb.column(0)->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid(
        "RecordBatch field 0 should be ", arrow::int32()->ToString(), ", actual is ", rb.column(0)->type()->ToString());
  }

  auto pid_arr = reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i];
    if (pid >= num_partitions_) {
      return arrow::Status::Invalid(
          "Partition id ", std::to_string(pid), " is equal or greater than ", std::to_string(num_partitions_));
    }
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}
} // namespace gluten
