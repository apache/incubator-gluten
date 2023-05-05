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

SplitOptions SplitOptions::Defaults() {
  return SplitOptions();
}

// ----------------------------------------------------------------------
// ArrowShuffleWriter

arrow::Result<std::shared_ptr<ArrowShuffleWriter>> ArrowShuffleWriter::Create(
    uint32_t num_partitions,
    SplitOptions options) {
  std::shared_ptr<ArrowShuffleWriter> res(new ArrowShuffleWriter(num_partitions, std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}
arrow::Status ArrowShuffleWriter::InitColumnType() {
  ARROW_ASSIGN_OR_RAISE(column_type_id_, ToShuffleWriterTypeId(schema_->fields()));
  std::vector<int32_t> binary_array_idx;

  for (size_t i = 0; i < column_type_id_.size(); ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        binary_array_idx.push_back(i);
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        list_array_idx_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
      default:
        array_idx_.push_back(i);
        break;
    }
  }
  fixed_width_col_cnt_ = array_idx_.size();

  array_idx_.insert(array_idx_.end(), binary_array_idx.begin(), binary_array_idx.end());

  uint32_t array_cnt = array_idx_.size();

  partition_validity_addrs_.resize(array_cnt);
  partition_fixed_width_value_addrs_.resize(fixed_width_col_cnt_);
  partition_buffers_.resize(array_cnt);
  binary_array_empirical_size_.resize(array_cnt - fixed_width_col_cnt_, 0);
  input_has_null_.resize(array_cnt, false);
  partition_binary_addrs_.resize(binary_array_idx.size());

  std::for_each(partition_validity_addrs_.begin(), partition_validity_addrs_.end(), [this](std::vector<uint8_t*>& v) {
    v.resize(num_partitions_, nullptr);
  });

  std::for_each(
      partition_fixed_width_value_addrs_.begin(),
      partition_fixed_width_value_addrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(num_partitions_, nullptr); });

  std::for_each(partition_buffers_.begin(), partition_buffers_.end(), [this](std::vector<arrow::BufferVector>& v) {
    v.resize(num_partitions_);
  });

  std::for_each(partition_binary_addrs_.begin(), partition_binary_addrs_.end(), [this](std::vector<BinaryBuff>& v) {
    v.resize(num_partitions_);
  });

  partition_list_builders_.resize(list_array_idx_.size());
  for (size_t i = 0; i < list_array_idx_.size(); ++i) {
    partition_list_builders_[i].resize(num_partitions_);
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::Init() {
  support_avx512_ = false;
#if defined(__x86_64__)
  support_avx512_ = __builtin_cpu_supports("avx512bw");
#endif
  // partition number should be less than 64k
  ARROW_CHECK_LE(num_partitions_, 64 * 1024);
  // split record batch size should be less than 32k
  ARROW_CHECK_LE(options_.buffer_size, 32 * 1024);

  ARROW_ASSIGN_OR_RAISE(partition_writer_, PartitionWriter::Make(this, num_partitions_));

  ARROW_ASSIGN_OR_RAISE(partitioner_, Partitioner::Make(options_.partitioning_name, num_partitions_));

  // when partitioner is SinglePart, partial variables don`t need init
  if (options_.partitioning_name != "single") {
    // pre-computed row count for each partition after the record batch split
    partition_id_cnt_.resize(num_partitions_);
    // pre-allocated buffer size for each partition, unit is row count
    partition_buffer_size_.resize(num_partitions_);
    // the offset of each partition during record batch split
    partition_buffer_idx_offset_.resize(num_partitions_);
    reducer_offset_offset_.resize(num_partitions_ + 1);
  }

  // start index for each partition when new record batch starts to split
  partition_buffer_idx_base_.resize(num_partitions_);

  partition_cached_recordbatch_.resize(num_partitions_);
  partition_cached_recordbatch_size_.resize(num_partitions_);
  partition_lengths_.resize(num_partitions_);
  raw_partition_lengths_.resize(num_partitions_);

  RETURN_NOT_OK(SetCompressType(options_.compression_type));

  auto& ipc_write_options = options_.ipc_write_options;
  ipc_write_options.memory_pool = options_.memory_pool.get();
  ipc_write_options.use_threads = false;

  // initialize tiny batch write options
  tiny_bach_write_options_ = ipc_write_options;
  tiny_bach_write_options_.codec = nullptr;

  // Allocate first buffer for split reducer
  // when partitioner is SinglePart, don`t need init combine_buffer_
  if (options_.partitioning_name != "single") {
    ARROW_ASSIGN_OR_RAISE(combine_buffer_, arrow::AllocateResizableBuffer(0, options_.memory_pool.get()));
    RETURN_NOT_OK(combine_buffer_->Resize(0, /*shrink_to_fit =*/false));
  }

  return arrow::Status::OK();
}
arrow::Status ArrowShuffleWriter::AllocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size) {
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

arrow::Status ArrowShuffleWriter::SetCompressType(arrow::Compression::type compressed_type) {
  ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec, CreateArrowIpcCodec(compressed_type));
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::Split(ColumnarBatch* batch) {
  EVAL_START("split", options_.thread_id)
  ARROW_ASSIGN_OR_RAISE(
      auto rb, arrow::ImportRecordBatch(batch->exportArrowArray().get(), batch->exportArrowSchema().get()));
  if (!partitioner_->HasPid()) {
    if (schema_ == nullptr) {
      schema_ = rb->schema();
      RETURN_NOT_OK(InitColumnType());
    }
  }

  if (options_.partitioning_name == "single") {
    RETURN_NOT_OK(CacheRecordBatch(0, *rb));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto pid_arr, GetFirstColumn(*rb));
    RETURN_NOT_OK(partitioner_->Compute(pid_arr, rb->num_rows(), partition_id_, partition_id_cnt_));
    if (partitioner_->HasPid()) {
      ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb->RemoveColumn(0));
      if (schema_ == nullptr) {
        schema_ = remove_pid->schema();
        RETURN_NOT_OK(InitColumnType());
      }
      RETURN_NOT_OK(DoSplit(*remove_pid));
    } else {
      RETURN_NOT_OK(DoSplit(*rb));
    }
  }
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::Stop() {
  EVAL_START("write", options_.thread_id)
  RETURN_NOT_OK(partition_writer_->Stop());
  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}

namespace {
int64_t batch_nbytes(const arrow::RecordBatch& batch) {
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

arrow::Status ArrowShuffleWriter::CacheRecordBatch(int32_t partition_id, const arrow::RecordBatch& batch) {
  int64_t raw_size = batch_nbytes(batch);
  raw_partition_lengths_[partition_id] += raw_size;
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
  int64_t batch_compress_time = 0;
#ifndef SKIPCOMPRESS
  if (batch.num_rows() <= (uint32_t)options_.batch_compress_threshold) {
    TIME_NANO_OR_RAISE(
        batch_compress_time, arrow::ipc::GetRecordBatchPayload(batch, tiny_bach_write_options_, payload.get()));
  } else {
    TIME_NANO_OR_RAISE(
        batch_compress_time, arrow::ipc::GetRecordBatchPayload(batch, options_.ipc_write_options, payload.get()));
  }
#else
  // for test reason
  TIME_NANO_OR_RAISE(
      batch_compress_time, arrow::ipc::GetRecordBatchPayload(*batch, tiny_bach_write_options_, payload.get()));
#endif

  total_compress_time_ += batch_compress_time;
  partition_cached_recordbatch_size_[partition_id] += payload->body_length;
  partition_cached_recordbatch_[partition_id].push_back(std::move(payload));
  partition_buffer_idx_base_[partition_id] = 0;
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::CreateRecordBatchFromBuffer(uint32_t partition_id, bool reset_buffers) {
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
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeof_binary_offset = sizeof(arrow::BinaryType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeof_binary_offset == -1)
          sizeof_binary_offset = sizeof(arrow::LargeBinaryType::offset_type);

        auto buffers = partition_buffers_[fixed_width_col_cnt_ + binary_idx][partition_id];
        // validity buffer
        if (buffers[0] != nullptr) {
          buffers[0] = arrow::SliceBuffer(buffers[0], 0, arrow::bit_util::BytesForBits(num_rows));
        }
        // offset buffer
        if (buffers[1] != nullptr) {
          buffers[1] = arrow::SliceBuffer(buffers[1], 0, (num_rows + 1) * sizeof_binary_offset);
        }
        // value buffer
        if (buffers[2] != nullptr) {
          ARROW_CHECK_NE(buffers[1], nullptr);
          buffers[2] = arrow::SliceBuffer(
              buffers[2],
              0,
              sizeof_binary_offset == 4
                  ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[1]->data())[num_rows]
                  : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[1]->data())[num_rows]);
        }

        arrays[i] = arrow::MakeArray(
            arrow::ArrayData::Make(schema_->field(i)->type(), num_rows, {buffers[0], buffers[1], buffers[2]}));

        uint64_t dst_offset0 = sizeof_binary_offset == 4
            ? reinterpret_cast<const arrow::BinaryType::offset_type*>(buffers[1]->data())[0]
            : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(buffers[1]->data())[0];
        ARROW_CHECK_EQ(dst_offset0, 0);

        if (reset_buffers) {
          partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx][partition_id] = nullptr;
          partition_binary_addrs_[binary_idx][partition_id] = BinaryBuff();
          partition_buffers_[fixed_width_col_cnt_ + binary_idx][partition_id].clear();
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
          if (column_type_id_[i]->id() == arrow::BooleanType::type_id)
            buffers[1] = arrow::SliceBuffer(buffers[1], 0, arrow::bit_util::BytesForBits(num_rows));
          else
            buffers[1] =
                arrow::SliceBuffer(buffers[1], 0, num_rows * (arrow::bit_width(column_type_id_[i]->id()) >> 3));
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
  auto batch = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
  return CacheRecordBatch(partition_id, *batch);
}

arrow::Status ArrowShuffleWriter::AllocatePartitionBuffers(int32_t partition_id, int32_t new_size) {
  // try to allocate new
  auto num_fields = schema_->num_fields();
  auto fixed_width_idx = 0;
  auto binary_idx = 0;
  auto list_idx = 0;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> new_list_builders;

  for (auto i = 0; i < num_fields; ++i) {
    size_t sizeof_binary_offset = -1;

    switch (column_type_id_[i]->id()) {
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

        if (input_has_null_[fixed_width_col_cnt_ + binary_idx]) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(validity_buffer, arrow::bit_util::BytesForBits(new_size)));
          // initialize all true once allocated
          memset(validity_buffer->mutable_data(), 0xff, validity_buffer->capacity());
          partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx][partition_id] = validity_buffer->mutable_data();
        } else {
          partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx][partition_id] = nullptr;
        }
        partition_buffers_[fixed_width_col_cnt_ + binary_idx][partition_id] = {
            std::move(validity_buffer), std::move(offset_buffer), std::move(value_buffer)};
        binary_idx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        RETURN_NOT_OK(MakeBuilder(options_.memory_pool.get(), column_type_id_[i], &array_builder));
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
        if (column_type_id_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(value_buffer, arrow::bit_util::BytesForBits(new_size)));
        } else {
          ARROW_RETURN_NOT_OK(
              AllocateBufferFromPool(value_buffer, new_size * (arrow::bit_width(column_type_id_[i]->id()) >> 3)));
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

  partition_buffer_size_[partition_id] = new_size;
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::AllocateNew(int32_t partition_id, int32_t new_size) {
  auto status = AllocatePartitionBuffers(partition_id, new_size);
  int32_t retry = 0;
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    std::cerr << status.ToString() << std::endl
              << std::to_string(++retry) << " retry to allocate new buffer for partition "
              << std::to_string(partition_id) << std::endl;
    int64_t evicted_size;
    ARROW_ASSIGN_OR_RAISE(auto partition_to_evict, EvictLargestPartition(&evicted_size));
    if (partition_to_evict == -1) {
      std::cerr << "Failed to allocate new buffer for partition " << std::to_string(partition_id)
                << ". No partition buffer to evict." << std::endl;
      return status;
    }
    status = AllocatePartitionBuffers(partition_id, new_size);
  }
  if (status.IsOutOfMemory()) {
    std::cerr << "Failed to allocate new buffer for partition " << std::to_string(partition_id) << ". Out of memory."
              << std::endl;
  }
  return status;
}

// call from memory management
arrow::Status ArrowShuffleWriter::EvictFixedSize(int64_t size, int64_t* actual) {
  int64_t current_evicted = 0L;
  int32_t try_count = 0;
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

arrow::Status ArrowShuffleWriter::EvictPartition(int32_t partition_id) {
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

arrow::Result<int32_t> ArrowShuffleWriter::EvictLargestPartition(int64_t* size) {
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

ArrowShuffleWriter::row_offset_type ArrowShuffleWriter::CalculateSplitBatchSize(const arrow::RecordBatch& rb) {
  uint32_t size_per_row = 0;
  auto num_rows = rb.num_rows();
  for (size_t i = fixed_width_col_cnt_; i < array_idx_.size(); ++i) {
    if (ARROW_PREDICT_FALSE(binary_array_empirical_size_[i - fixed_width_col_cnt_] == 0)) {
      auto arr = rb.column_data(array_idx_[i]);
      // ARROW_CHECK_EQ(arr->buffers.size(), 3);
      // offset array_data
      if (ARROW_PREDICT_TRUE(arr->buffers[1] != nullptr)) {
        auto offsetbuf = arr->buffers[1]->data();
        uint64_t length = 0;
        switch (column_type_id_[array_idx_[i]]->id()) {
          case arrow::BinaryType::type_id:
          case arrow::StringType::type_id:
            length = reinterpret_cast<const arrow::StringType::offset_type*>(offsetbuf)[num_rows] -
                reinterpret_cast<const arrow::StringType::offset_type*>(offsetbuf)[0];
            break;
          case arrow::LargeBinaryType::type_id:
          case arrow::LargeStringType::type_id:
            length = reinterpret_cast<const arrow::LargeStringType::offset_type*>(offsetbuf)[num_rows] -
                reinterpret_cast<const arrow::LargeStringType::offset_type*>(offsetbuf)[0];
            break;
          default:
            break;
        }
        binary_array_empirical_size_[i - fixed_width_col_cnt_] =
            length % num_rows == 0 ? length / num_rows : length / num_rows + 1;
        // std::cout << "avg str length col = " << i - fixed_width_col_cnt_ << "
        // len = "
        // << binary_array_empirical_size_[i - fixed_width_col_cnt_] <<
        // std::endl;
      }
    }
  }

  size_per_row = std::accumulate(binary_array_empirical_size_.begin(), binary_array_empirical_size_.end(), 0);

  for (size_t col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    auto type_id = column_type_id_[col_idx]->id();
    // why +7? to fit column bool
    size_per_row += ((arrow::bit_width(type_id) + 7) >> 3);
  }

  int64_t prealloc_row_cnt = options_.offheap_per_task > 0 && size_per_row > 0
      ? options_.offheap_per_task / size_per_row / num_partitions_ >> 2
      : options_.buffer_size;
  prealloc_row_cnt = std::min(prealloc_row_cnt, (int64_t)options_.buffer_size);

  return (row_offset_type)prealloc_row_cnt;
}

arrow::Status ArrowShuffleWriter::DoSplit(const arrow::RecordBatch& rb) {
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
    // once input_has_null_ is set to true, we didn't reset it after evict
    if (!input_has_null_[col] && rb.column_data(col_idx)->GetNullCount() != 0) {
      input_has_null_[col] = true;
    }
  }

  // prepare partition buffers and evict if necessary
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
        if (options_.prefer_evict) {
          // if prefer_evict is set, evict current record batch, we may reuse
          // the buffers

          if (new_size > (unsigned)partition_buffer_size_[pid]) {
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

template <typename T>
arrow::Status ArrowShuffleWriter::SplitFixedType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs) {
  std::transform(
      dst_addrs.begin(),
      dst_addrs.end(),
      partition_buffer_idx_base_.begin(),
      partition_buffer_idx_offset_.begin(),
      [](uint8_t* x, row_offset_type y) { return x + y * sizeof(T); });
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < num_partitions_; pid++) {
    auto dst_pid_base = reinterpret_cast<T*>(partition_buffer_idx_offset_[pid]); /*32k*/
    auto r = reducer_offset_offset_[pid]; /*8k*/
    auto size = reducer_offset_offset_[pid + 1];
    for (; r < size; r++) {
      auto src_offset = reducer_offsets_[r]; /*16k*/
      *dst_pid_base = reinterpret_cast<const T*>(src_addr)[src_offset]; /*64k*/
      PREFETCHT2((src_addr + src_offset * sizeof(T) + 64));
      dst_pid_base += 1;
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::SplitFixedWidthValueBuffer(const arrow::RecordBatch& rb) {
  for (auto col = 0; col < fixed_width_col_cnt_; ++col) {
    const auto& dst_addrs = partition_fixed_width_value_addrs_[col];
    auto col_idx = array_idx_[col];
    auto src_addr = const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[1]->data());

    switch (arrow::bit_width(column_type_id_[col_idx]->id())) {
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
          auto r = reducer_offset_offset_[pid]; /*8k*/
          auto size = reducer_offset_offset_[pid + 1];
#if 1
          for (r; r < size && (((uint64_t)dst_pid_base & 0x1f) > 0); r++) {
            auto src_offset = reducer_offsets_[r]; /*16k*/
            *dst_pid_base = reinterpret_cast<uint64_t*>(src_addr)[src_offset]; /*64k*/
            _mm_prefetch(&(src_addr)[src_offset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 1;
          }
#if 0
          for (r; r+4<size; r+=4)                              
          {                                                                                    
            auto src_offset = reducer_offsets_[r];                                 /*16k*/ 
            __m128i src_ld = _mm_loadl_epi64((__m128i*)(&reducer_offsets_[r]));    
            __m128i src_offset_4x = _mm_cvtepu16_epi32(src_ld);
            
            __m256i src_4x = _mm256_i32gather_epi64((const long long int*)src_addr,src_offset_4x,8);
            //_mm256_store_si256((__m256i*)dst_pid_base,src_4x); 
            _mm_stream_si128((__m128i*)dst_pid_base,src_2x);
                                                         
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+1]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+2]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+3]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            dst_pid_base+=4;                                                                   
          }
#endif
          for (r; r + 2 < size; r += 2) {
            __m128i src_offset_2x = _mm_cvtsi32_si128(*((int32_t*)(reducer_offsets_.data() + r)));
            src_offset_2x = _mm_shufflelo_epi16(src_offset_2x, 0x98);

            __m128i src_2x = _mm_i32gather_epi64((const long long int*)src_addr, src_offset_2x, 8);
            _mm_store_si128((__m128i*)dst_pid_base, src_2x);
            //_mm_stream_si128((__m128i*)dst_pid_base,src_2x);

            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r + 1] * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 2;
          }
#endif
          for (r; r < size; r++) {
            auto src_offset = reducer_offsets_[r]; /*16k*/
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
          // assume batch size = 32k; reducer# = 4K; row/reducer = 8
          for (auto pid = 0; pid < num_partitions_; pid++) {
            auto dst_pid_base = reinterpret_cast<__m128i_u*>(partition_buffer_idx_offset_[pid]); /*32k*/
            auto r = reducer_offset_offset_[pid]; /*8k*/
            auto size = reducer_offset_offset_[pid + 1];
            for (; r < size; r++) {
              auto src_offset = reducer_offsets_[r]; /*16k*/
              __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i_u*>(src_addr) + src_offset);
              _mm_storeu_si128(dst_pid_base, value);
              _mm_prefetch(src_addr + src_offset * sizeof(__m128i_u) + 64, _MM_HINT_T2);
              dst_pid_base += 1;
            }
          }
        }
#elif defined(__aarch64__)
      case 128:
        RETURN_NOT_OK(SplitFixedType<uint32x4_t>(src_addr, dst_addrs));
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

arrow::Status ArrowShuffleWriter::SplitBoolType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs) {
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < num_partitions_; pid++) {
    // set the last byte
    auto dstaddr = dst_addrs[pid];
    if (partition_id_cnt_[pid] > 0 && dstaddr != nullptr) {
      auto r = reducer_offset_offset_[pid]; /*8k*/
      auto size = reducer_offset_offset_[pid + 1];
      row_offset_type dst_offset = partition_buffer_idx_base_[pid];
      row_offset_type dst_offset_in_byte = (8 - (dst_offset & 0x7)) & 0x7;
      row_offset_type dst_idx_byte = dst_offset_in_byte;
      uint8_t dst = dstaddr[dst_offset >> 3];
      if (pid + 1 < num_partitions_) {
        PREFETCHT1((&dstaddr[partition_buffer_idx_base_[pid + 1] >> 3]));
      }
      for (; r < size && dst_idx_byte > 0; r++, dst_idx_byte--) {
        auto src_offset = reducer_offsets_[r]; /*16k*/
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
        auto src_offset = reducer_offsets_[r]; /*16k*/
        src = src_addr[src_offset >> 3];
        PREFETCHT0((&(src_addr)[(src_offset >> 3) + 64]));
        dst = src >> (src_offset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 1]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 1 | 0xfd; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 2]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 2 | 0xfb; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 3]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 3 | 0xf7; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 4]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 4 | 0xef; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 5]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 5 | 0xdf; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 6]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 6 | 0xbf; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 7]; /*16k*/
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
        auto src_offset = reducer_offsets_[r]; /*16k*/
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

arrow::Status ArrowShuffleWriter::SplitValidityBuffer(const arrow::RecordBatch& rb) {
  for (size_t col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    auto& dst_addrs = partition_validity_addrs_[col];
    if (rb.column_data(col_idx)->GetNullCount() > 0) {
      // there is Null count
      for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (partition_id_cnt_[pid] > 0 && dst_addrs[pid] == nullptr) {
          // init bitmap if it's null, initialize the buffer as true
          auto new_size = std::max(partition_id_cnt_[pid], (row_offset_type)options_.buffer_size);
          std::shared_ptr<arrow::Buffer> validity_buffer;
          auto status = AllocateBufferFromPool(validity_buffer, arrow::bit_util::BytesForBits(new_size));
          ARROW_RETURN_NOT_OK(status);
          dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data());
          memset(validity_buffer->mutable_data(), 0xff, validity_buffer->capacity());
          partition_buffers_[col][pid][0] = std::move(validity_buffer);
        }
      }
      auto src_addr = const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[0]->data());

      RETURN_NOT_OK(SplitBoolType(src_addr, dst_addrs));
    }
  }
  return arrow::Status::OK();
}

template <typename T>
arrow::Status ArrowShuffleWriter::SplitBinaryType(
    const uint8_t* src_addr,
    const T* src_offset_addr,
    std::vector<BinaryBuff>& dst_addrs,
    const int binary_idx) {
  for (auto pid = 0; pid < num_partitions_; pid++) {
    auto dst_offset_base = reinterpret_cast<T*>(dst_addrs[pid].offsetptr) + partition_buffer_idx_base_[pid];
    if (pid + 1 < num_partitions_) {
      PREFETCHT1((reinterpret_cast<T*>(dst_addrs[pid + 1].offsetptr) + partition_buffer_idx_base_[pid + 1]));
      PREFETCHT1((dst_addrs[pid + 1].valueptr + dst_addrs[pid + 1].value_offset));
    }
    auto value_offset = dst_addrs[pid].value_offset;
    auto dst_value_base = dst_addrs[pid].valueptr + value_offset;
    auto capacity = dst_addrs[pid].value_capacity;

    auto r = reducer_offset_offset_[pid]; /*128k*/
    auto size = reducer_offset_offset_[pid + 1] - r;

    auto multiply = 1;
    for (uint32_t x = 0; x < size; x++) {
      auto src_offset = reducer_offsets_[x + r]; /*128k*/
      auto strlength = src_offset_addr[src_offset + 1] - src_offset_addr[src_offset];
      value_offset = dst_offset_base[x + 1] = value_offset + strlength;
      if (ARROW_PREDICT_FALSE(value_offset >= capacity)) {
        // allocate value buffer again, enlarge the buffer
        auto old_capacity = capacity;
        capacity = capacity + std::max((capacity >> multiply), (uint64_t)strlength);
        multiply = std::min(3, multiply + 1);
        auto value_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            partition_buffers_[fixed_width_col_cnt_ + binary_idx][pid][2]);
        RETURN_NOT_OK(value_buffer->Reserve(capacity));

        dst_addrs[pid].valueptr = value_buffer->mutable_data();
        dst_addrs[pid].value_capacity = capacity;
        dst_value_base = dst_addrs[pid].valueptr + value_offset - strlength;
        std::cerr << "Split value buffer resized colid = " << binary_idx << " dst_start " << dst_offset_base[x]
                  << " dst_end " << dst_offset_base[x + 1] << " old size = " << old_capacity
                  << " new size = " << capacity << " row = " << partition_buffer_idx_base_[pid]
                  << " strlen = " << strlength << std::endl;
      }
      auto value_src_ptr = src_addr + src_offset_addr[src_offset];
#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(support_avx512_)) {
        // write the variable value
        T k;
        for (k = 0; k + 32 < strlength; k += 32) {
          __m256i v = _mm256_loadu_si256((const __m256i*)(value_src_ptr + k));
          _mm256_storeu_si256((__m256i*)(dst_value_base + k), v);
        }
        auto mask = (1L << (strlength - k)) - 1;
        __m256i v = _mm256_maskz_loadu_epi8(mask, value_src_ptr + k);
        _mm256_mask_storeu_epi8(dst_value_base + k, mask, v);
      } else
#endif
      {
        memcpy(dst_value_base, value_src_ptr, strlength);
      }
      dst_value_base += strlength;
      PREFETCHT1((value_src_ptr + 64));
      PREFETCHT1((src_offset_addr + src_offset + 64 / sizeof(T)));
    }
    dst_addrs[pid].value_offset = value_offset;
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleWriter::SplitBinaryArray(const arrow::RecordBatch& rb) {
  for (auto col = fixed_width_col_cnt_; col < array_idx_.size(); ++col) {
    auto& dst_addrs = partition_binary_addrs_[col - fixed_width_col_cnt_];
    auto col_idx = array_idx_[col];
    auto arr_data = rb.column_data(col_idx);
    auto src_value_addr = arr_data->GetValuesSafe<uint8_t>(2);

    auto typeids = column_type_id_[col_idx]->id();
    if (typeids == arrow::BinaryType::type_id || typeids == arrow::StringType::type_id) {
      auto src_offset_addr = arr_data->GetValuesSafe<arrow::BinaryType::offset_type>(1);
      RETURN_NOT_OK(SplitBinaryType<arrow::BinaryType::offset_type>(
          src_value_addr, src_offset_addr, dst_addrs, col - fixed_width_col_cnt_));
    } else {
      auto src_offset_addr = arr_data->GetValuesSafe<arrow::LargeBinaryType::offset_type>(1);
      RETURN_NOT_OK(SplitBinaryType<arrow::LargeBinaryType::offset_type>(
          src_value_addr, src_offset_addr, dst_addrs, col - fixed_width_col_cnt_));
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
arrow::Status ArrowShuffleWriter::SplitListArray(const arrow::RecordBatch& rb) {
  for (size_t i = 0; i < list_array_idx_.size(); ++i) {
    auto src_arr = std::static_pointer_cast<arrow::ListArray>(rb.column(list_array_idx_[i]));
    auto status = AppendList(rb.column(list_array_idx_[i]), partition_list_builders_[i], rb.num_rows());
    if (!status.ok())
      return status;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

arrow::Status ArrowShuffleWriter::AppendList(
    const std::shared_ptr<arrow::Array>& src_arr,
    const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dst_builders,
    int64_t num_rows) {
  for (auto row = 0; row < num_rows; ++row) {
    RETURN_NOT_OK(dst_builders[partition_id_[row]]->AppendArraySlice(*(src_arr->data().get()), row, 1));
  }
  return arrow::Status::OK();
}

arrow::Result<const int32_t*> ArrowShuffleWriter::GetFirstColumn(const arrow::RecordBatch& rb) {
  if (partitioner_->HasPid()) {
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

    auto pid_arr = reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
    if (pid_arr == nullptr) {
      return arrow::Status::Invalid("failed to cast rb.column(0), this column should be pid");
    }

    return pid_arr;
  } else {
    return nullptr;
  }
}

} // namespace gluten
