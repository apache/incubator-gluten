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

#include "shuffle/splitter.h"

#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <memory>
#include <utility>

#include "shuffle/utils.h"
#include "utils/macros.h"

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
#include <immintrin.h>
#else
#include <xmmintrin.h>
#endif

namespace sparkcolumnarplugin {
namespace shuffle {
using arrow::internal::checked_cast;

SplitOptions SplitOptions::Defaults() { return SplitOptions(); }
#if defined(COLUMNAR_PLUGIN_USE_AVX512)
inline __m256i CountPartitionIdOccurrence(const std::vector<int32_t>& partition_id,
                                          int32_t row) {
  __m128i partid_cnt_low;
  __m128i partid_cnt_high;
  int32_t tmp1, tmp2, tmp3, tmp4, tmp5, tmp6, tmp7;

  partid_cnt_low = _mm_xor_si128(partid_cnt_low, partid_cnt_low);

  tmp1 = (partition_id[row + 1] ^ partition_id[row]) == 0;
  partid_cnt_low = _mm_insert_epi32(partid_cnt_low, tmp1, 1);

  tmp2 = (partition_id[row + 2] ^ partition_id[row]) == 0;
  tmp2 += (partition_id[row + 2] ^ partition_id[row + 1]) == 0;
  partid_cnt_low = _mm_insert_epi32(partid_cnt_low, tmp2, 2);

  tmp3 = (partition_id[row + 3] ^ partition_id[row]) == 0;
  tmp3 += (partition_id[row + 3] ^ partition_id[row + 1]) == 0;
  tmp3 += (partition_id[row + 3] ^ partition_id[row + 2]) == 0;
  partid_cnt_low = _mm_insert_epi32(partid_cnt_low, tmp3, 3);

  tmp4 = (partition_id[row + 4] ^ partition_id[row]) == 0;
  tmp4 += (partition_id[row + 4] ^ partition_id[row + 1]) == 0;
  tmp4 += (partition_id[row + 4] ^ partition_id[row + 2]) == 0;
  tmp4 += (partition_id[row + 4] ^ partition_id[row + 3]) == 0;
  partid_cnt_high = _mm_insert_epi32(partid_cnt_high, tmp4, 0);

  tmp5 = (partition_id[row + 5] ^ partition_id[row]) == 0;
  tmp5 += (partition_id[row + 5] ^ partition_id[row + 1]) == 0;
  tmp5 += (partition_id[row + 5] ^ partition_id[row + 2]) == 0;
  tmp5 += (partition_id[row + 5] ^ partition_id[row + 3]) == 0;
  tmp5 += (partition_id[row + 5] ^ partition_id[row + 4]) == 0;
  partid_cnt_high = _mm_insert_epi32(partid_cnt_high, tmp5, 1);

  tmp6 = (partition_id[row + 6] ^ partition_id[row]) == 0;
  tmp6 += (partition_id[row + 6] ^ partition_id[row + 1]) == 0;
  tmp6 += (partition_id[row + 6] ^ partition_id[row + 2]) == 0;
  tmp6 += (partition_id[row + 6] ^ partition_id[row + 3]) == 0;
  tmp6 += (partition_id[row + 6] ^ partition_id[row + 4]) == 0;
  tmp6 += (partition_id[row + 6] ^ partition_id[row + 5]) == 0;
  partid_cnt_high = _mm_insert_epi32(partid_cnt_high, tmp6, 2);

  tmp7 = (partition_id[row + 7] ^ partition_id[row]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 1]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 2]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 3]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 4]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 5]) == 0;
  tmp7 += (partition_id[row + 7] ^ partition_id[row + 6]) == 0;
  partid_cnt_high = _mm_insert_epi32(partid_cnt_high, tmp7, 3);

  __m256i partid_cnt_8x = _mm256_castsi128_si256(partid_cnt_low);
  partid_cnt_8x = _mm256_inserti128_si256(partid_cnt_8x, partid_cnt_high, 1);
  return partid_cnt_8x;
}

inline void PrefetchDstAddr(__m512i dst_addr_8x, int32_t scale) {
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 0), 0) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 0), 1) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 1), 0) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 1), 1) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 2), 0) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 2), 1) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 3), 0) + scale),
      _MM_HINT_T0);
  _mm_prefetch(
      (void*)(_mm_extract_epi64(_mm512_extracti64x2_epi64(dst_addr_8x, 3), 1) + scale),
      _MM_HINT_T0);
}
#endif

class Splitter::PartitionWriter {
 public:
  explicit PartitionWriter(Splitter* splitter, int32_t partition_id)
      : splitter_(splitter), partition_id_(partition_id) {}

  arrow::Status Spill() {
    RETURN_NOT_OK(EnsureOpened());
    RETURN_NOT_OK(WriteRecordBatchPayload(spilled_file_os_.get(), partition_id_));
    ClearCache();
    return arrow::Status::OK();
  }

  arrow::Status WriteCachedRecordBatchAndClose() {
    const auto& data_file_os = splitter_->data_file_os_;
    ARROW_ASSIGN_OR_RAISE(auto before_write, data_file_os->Tell());

    if (splitter_->options_.write_schema) {
      RETURN_NOT_OK(WriteSchemaPayload(data_file_os.get()));
    }

    if (spilled_file_opened_) {
      RETURN_NOT_OK(spilled_file_os_->Close());
      RETURN_NOT_OK(MergeSpilled());
    } else {
      if (splitter_->partition_cached_recordbatch_size_[partition_id_] == 0) {
        return arrow::Status::Invalid("Partition writer got empty partition");
      }
    }

    RETURN_NOT_OK(WriteRecordBatchPayload(data_file_os.get(), partition_id_));
    RETURN_NOT_OK(WriteEOS(data_file_os.get()));
    ClearCache();

    ARROW_ASSIGN_OR_RAISE(auto after_write, data_file_os->Tell());
    partition_length = after_write - before_write;

    return arrow::Status::OK();
  }

  // metrics
  int64_t bytes_spilled = 0;
  int64_t partition_length = 0;
  int64_t compress_time = 0;

 private:
  arrow::Status EnsureOpened() {
    if (!spilled_file_opened_) {
      ARROW_ASSIGN_OR_RAISE(spilled_file_,
                            CreateTempShuffleFile(splitter_->NextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(spilled_file_os_,
                            arrow::io::FileOutputStream::Open(spilled_file_, true));
      spilled_file_opened_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeSpilled() {
    ARROW_ASSIGN_OR_RAISE(
        auto spilled_file_is_,
        arrow::io::MemoryMappedFile::Open(spilled_file_, arrow::io::FileMode::READ));
    // copy spilled data blocks
    ARROW_ASSIGN_OR_RAISE(auto nbytes, spilled_file_is_->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, spilled_file_is_->Read(nbytes));
    RETURN_NOT_OK(splitter_->data_file_os_->Write(buffer));

    // close spilled file streams and delete the file
    RETURN_NOT_OK(spilled_file_is_->Close());
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    RETURN_NOT_OK(fs->DeleteFile(spilled_file_));
    bytes_spilled += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status WriteSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, splitter_->GetSchemaPayload());
    int32_t metadata_length = 0;  // unused
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *payload, splitter_->options_.ipc_write_options, os, &metadata_length));
    return arrow::Status::OK();
  }

  arrow::Status WriteRecordBatchPayload(arrow::io::OutputStream* os,
                                        int32_t partition_id) {
    int32_t metadata_length = 0;  // unused
    for (auto& payload : splitter_->partition_cached_recordbatch_[partition_id_]) {
      RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
          *payload, splitter_->options_.ipc_write_options, os, &metadata_length));
      payload = nullptr;
    }
    return arrow::Status::OK();
  }

  arrow::Status WriteEOS(arrow::io::OutputStream* os) {
    // write EOS
    constexpr int32_t kZeroLength = 0;
    RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(int32_t)));
    RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(int32_t)));
    return arrow::Status::OK();
  }

  void ClearCache() {
    splitter_->partition_cached_recordbatch_[partition_id_].clear();
    splitter_->partition_cached_recordbatch_size_[partition_id_] = 0;
  }

  Splitter* splitter_;
  int32_t partition_id_;
  std::string spilled_file_;
  std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;

  bool spilled_file_opened_ = false;
};

// ----------------------------------------------------------------------
// Splitter

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions, const gandiva::ExpressionVector& expr_vector,
    SplitOptions options) {
  if (short_name == "hash") {
    return HashSplitter::Create(num_partitions, std::move(schema), expr_vector,
                                std::move(options));
  } else if (short_name == "rr") {
    return RoundRobinSplitter::Create(num_partitions, std::move(schema),
                                      std::move(options));
  } else if (short_name == "range") {
    return FallbackRangeSplitter::Create(num_partitions, std::move(schema),
                                         std::move(options));
  } else if (short_name == "single") {
    return RoundRobinSplitter::Create(1, std::move(schema), std::move(options));
  }
  return arrow::Status::NotImplemented("Partitioning " + short_name +
                                       " not supported yet.");
}

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions, SplitOptions options) {
  return Make(short_name, std::move(schema), num_partitions, {}, std::move(options));
}

arrow::Status Splitter::Init() {
  const auto& fields = schema_->fields();
  ARROW_ASSIGN_OR_RAISE(column_type_id_, ToSplitterTypeId(schema_->fields()));

  partition_writer_.resize(num_partitions_);
  partition_id_cnt_.resize(num_partitions_);
  partition_buffer_size_.resize(num_partitions_);
  partition_buffer_idx_base_.resize(num_partitions_);
  partition_buffer_idx_offset_.resize(num_partitions_);
  partition_cached_recordbatch_.resize(num_partitions_);
  partition_cached_recordbatch_size_.resize(num_partitions_);
  partition_lengths_.resize(num_partitions_);
  raw_partition_lengths_.resize(num_partitions_);

  for (int i = 0; i < column_type_id_.size(); ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        binary_array_idx_.push_back(i);
        break;
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        large_binary_array_idx_.push_back(i);
        break;
      case arrow::ListType::type_id:
        list_array_idx_.push_back(i);
        break;
      case arrow::LargeListType::type_id:
        list_array_idx_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
      default:
        fixed_width_array_idx_.push_back(i);
        break;
    }
  }

  auto num_fixed_width = fixed_width_array_idx_.size();
  partition_fixed_width_validity_addrs_.resize(num_fixed_width);
  partition_fixed_width_value_addrs_.resize(num_fixed_width);
  partition_fixed_width_buffers_.resize(num_fixed_width);
  binary_array_empirical_size_.resize(binary_array_idx_.size());
  large_binary_array_empirical_size_.resize(large_binary_array_idx_.size());
  input_fixed_width_has_null_.resize(num_fixed_width, false);
  for (auto i = 0; i < num_fixed_width; ++i) {
    partition_fixed_width_validity_addrs_[i].resize(num_partitions_);
    partition_fixed_width_value_addrs_[i].resize(num_partitions_);
    partition_fixed_width_buffers_[i].resize(num_partitions_);
  }
  partition_binary_builders_.resize(binary_array_idx_.size());
  for (auto i = 0; i < binary_array_idx_.size(); ++i) {
    partition_binary_builders_[i].resize(num_partitions_);
  }
  partition_large_binary_builders_.resize(large_binary_array_idx_.size());
  for (auto i = 0; i < large_binary_array_idx_.size(); ++i) {
    partition_large_binary_builders_[i].resize(num_partitions_);
  }
  partition_list_builders_.resize(list_array_idx_.size());
  for (auto i = 0; i < list_array_idx_.size(); ++i) {
    partition_list_builders_[i].resize(num_partitions_);
  }

  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (options_.data_file.length() == 0) {
    ARROW_ASSIGN_OR_RAISE(options_.data_file, CreateTempShuffleFile(configured_dirs_[0]));
  }

  auto& ipc_write_options = options_.ipc_write_options;
  ipc_write_options.memory_pool = options_.memory_pool;
  ipc_write_options.use_threads = false;

  if (options_.compression_type == arrow::Compression::FASTPFOR) {
    ARROW_ASSIGN_OR_RAISE(ipc_write_options.codec,
                          arrow::util::Codec::CreateInt32(arrow::Compression::FASTPFOR));

  } else if (options_.compression_type == arrow::Compression::LZ4_FRAME) {
    ARROW_ASSIGN_OR_RAISE(ipc_write_options.codec,
                          arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  } else {
    ARROW_ASSIGN_OR_RAISE(ipc_write_options.codec, arrow::util::Codec::CreateInt32(
                                                       arrow::Compression::UNCOMPRESSED));
  }

  // initialize tiny batch write options
  tiny_bach_write_options_ = ipc_write_options;
  ARROW_ASSIGN_OR_RAISE(
      tiny_bach_write_options_.codec,
      arrow::util::Codec::CreateInt32(arrow::Compression::UNCOMPRESSED));

  return arrow::Status::OK();
}

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
      accumulated += buf->capacity();
    }
  }
  return accumulated;
}

int64_t batch_nbytes(std::shared_ptr<arrow::RecordBatch> batch) {
  if (batch == nullptr) {
    return 0;
  }
  return batch_nbytes(*batch);
}

int64_t Splitter::CompressedSize(const arrow::RecordBatch& rb) {
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::Status result;
  result =
      arrow::ipc::GetRecordBatchPayload(rb, options_.ipc_write_options, payload.get());
  if (result.ok()) {
    return payload->body_length;
  } else {
    result.UnknownError("Failed to get the compressed size.");
    return -1;
  }
}

arrow::Status Splitter::SetCompressType(arrow::Compression::type compressed_type) {
  if (compressed_type == arrow::Compression::FASTPFOR) {
    ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec,
                          arrow::util::Codec::CreateInt32(arrow::Compression::FASTPFOR));

  } else if (compressed_type == arrow::Compression::LZ4_FRAME) {
    ARROW_ASSIGN_OR_RAISE(options_.ipc_write_options.codec,
                          arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  } else {
    ARROW_ASSIGN_OR_RAISE(
        options_.ipc_write_options.codec,
        arrow::util::Codec::CreateInt32(arrow::Compression::UNCOMPRESSED));
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::Split(const arrow::RecordBatch& rb) {
  EVAL_START("split", options_.thread_id)
  RETURN_NOT_OK(ComputeAndCountPartitionId(rb));
  RETURN_NOT_OK(DoSplit(rb));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status Splitter::Stop() {
  EVAL_START("write", options_.thread_id)
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout,
                        arrow::io::FileOutputStream::Open(options_.data_file, true));
  if (options_.buffered_write) {
    ARROW_ASSIGN_OR_RAISE(data_file_os_, arrow::io::UnlockedBufferedOutputStream::Create(
                                             16384, options_.memory_pool, fout));
  } else {
    data_file_os_ = fout;
  }

  // stop PartitionWriter and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(CacheRecordBatch(pid, true));
    if (partition_cached_recordbatch_size_[pid] > 0) {
      if (partition_writer_[pid] == nullptr) {
        partition_writer_[pid] = std::make_shared<PartitionWriter>(this, pid);
      }
    }
    if (partition_writer_[pid] != nullptr) {
      const auto& writer = partition_writer_[pid];
      TIME_NANO_OR_RAISE(total_write_time_, writer->WriteCachedRecordBatchAndClose());
      partition_lengths_[pid] = writer->partition_length;
      total_bytes_written_ += writer->partition_length;
      total_bytes_spilled_ += writer->bytes_spilled;
      total_compress_time_ += writer->compress_time;
    } else {
      partition_lengths_[pid] = 0;
    }
  }

  // close data file output Stream
  RETURN_NOT_OK(data_file_os_->Close());

  EVAL_END("write", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status Splitter::CacheRecordBatch(int32_t partition_id, bool reset_buffers) {
  if (partition_buffer_idx_base_[partition_id] > 0) {
    auto fixed_width_idx = 0;
    auto binary_idx = 0;
    auto large_binary_idx = 0;
    auto list_idx = 0;
    auto num_fields = schema_->num_fields();
    auto num_rows = partition_buffer_idx_base_[partition_id];
    auto buffer_sizes = 0;
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      switch (column_type_id_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id: {
          auto& builder = partition_binary_builders_[binary_idx][partition_id];
          if (reset_buffers) {
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
          } else {
            auto data_size = builder->value_data_length();
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
            RETURN_NOT_OK(builder->Reserve(num_rows));
            RETURN_NOT_OK(builder->ReserveData(data_size));
          }
          binary_idx++;
          break;
        }
        case arrow::LargeBinaryType::type_id:
        case arrow::LargeStringType::type_id: {
          auto& builder =
              partition_large_binary_builders_[large_binary_idx][partition_id];
          if (reset_buffers) {
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
          } else {
            auto data_size = builder->value_data_length();
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
            RETURN_NOT_OK(builder->Reserve(num_rows));
            RETURN_NOT_OK(builder->ReserveData(data_size));
          }
          large_binary_idx++;
          break;
        }
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
        case arrow::LargeListType::type_id: {
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
          arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
              arrow::null(), num_rows, {nullptr, nullptr}, num_rows));
          break;
        }
        default: {
          auto& buffers = partition_fixed_width_buffers_[fixed_width_idx][partition_id];
          if (reset_buffers) {
            arrays[i] = arrow::MakeArray(
                arrow::ArrayData::Make(schema_->field(i)->type(), num_rows,
                                       {std::move(buffers[0]), std::move(buffers[1])}));
            buffers = {nullptr, nullptr};
            partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] =
                nullptr;
            partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = nullptr;
          } else {
            arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
                schema_->field(i)->type(), num_rows, {buffers[0], buffers[1]}));
          }
          fixed_width_idx++;
          break;
        }
      }
    }
    auto batch = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
    int64_t raw_size = batch_nbytes(batch);
    raw_partition_lengths_[partition_id] += raw_size;
    auto payload = std::make_shared<arrow::ipc::IpcPayload>();
    if (num_rows <= options_.batch_compress_threshold) {
      TIME_NANO_OR_RAISE(total_compress_time_,
                         arrow::ipc::GetRecordBatchPayload(
                             *batch, tiny_bach_write_options_, payload.get()));
    } else {
      TIME_NANO_OR_RAISE(total_compress_time_,
                         arrow::ipc::GetRecordBatchPayload(
                             *batch, options_.ipc_write_options, payload.get()));
    }
    partition_cached_recordbatch_size_[partition_id] += payload->body_length;
    partition_cached_recordbatch_[partition_id].push_back(std::move(payload));
    partition_buffer_idx_base_[partition_id] = 0;
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::AllocatePartitionBuffers(int32_t partition_id, int32_t new_size) {
  // try to allocate new
  auto num_fields = schema_->num_fields();
  auto fixed_width_idx = 0;
  auto binary_idx = 0;
  auto large_binary_idx = 0;
  auto list_idx = 0;

  std::vector<std::shared_ptr<arrow::BinaryBuilder>> new_binary_builders;
  std::vector<std::shared_ptr<arrow::LargeBinaryBuilder>> new_large_binary_builders;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> new_list_builders;
  std::vector<std::shared_ptr<arrow::ResizableBuffer>> new_value_buffers;
  std::vector<std::shared_ptr<arrow::ResizableBuffer>> new_validity_buffers;
  for (auto i = 0; i < num_fields; ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        auto builder = std::make_shared<arrow::BinaryBuilder>(options_.memory_pool);
        assert(builder != nullptr);
        RETURN_NOT_OK(builder->Reserve(new_size));
        RETURN_NOT_OK(
            builder->ReserveData(binary_array_empirical_size_[binary_idx] * new_size));
        new_binary_builders.push_back(std::move(builder));
        binary_idx++;
        break;
      }
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        auto builder = std::make_shared<arrow::LargeBinaryBuilder>(options_.memory_pool);
        assert(builder != nullptr);
        RETURN_NOT_OK(builder->Reserve(new_size));
        RETURN_NOT_OK(builder->ReserveData(
            large_binary_array_empirical_size_[large_binary_idx] * new_size));
        new_large_binary_builders.push_back(std::move(builder));
        large_binary_idx++;
        break;
      }
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        RETURN_NOT_OK(
            MakeBuilder(options_.memory_pool, column_type_id_[i], &array_builder));
        assert(array_builder != nullptr);
        RETURN_NOT_OK(array_builder->Reserve(new_size));
        new_list_builders.push_back(std::move(array_builder));
        list_idx++;
        break;
      }
      case arrow::LargeListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        RETURN_NOT_OK(
            MakeBuilder(options_.memory_pool, column_type_id_[i], &array_builder));
        assert(array_builder != nullptr);
        RETURN_NOT_OK(array_builder->Reserve(new_size));
        new_list_builders.push_back(std::move(array_builder));
        list_idx++;
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: {
        std::shared_ptr<arrow::ResizableBuffer> value_buffer;
        if (column_type_id_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_ASSIGN_OR_RAISE(value_buffer, arrow::AllocateResizableBuffer(
                                                  arrow::BitUtil::BytesForBits(new_size),
                                                  options_.memory_pool));
        } else {
          ARROW_ASSIGN_OR_RAISE(
              value_buffer,
              arrow::AllocateResizableBuffer(
                  new_size * (arrow::bit_width(column_type_id_[i]->id()) / 8),
                  options_.memory_pool));
        }
        new_value_buffers.push_back(std::move(value_buffer));
        if (input_fixed_width_has_null_[fixed_width_idx]) {
          std::shared_ptr<arrow::ResizableBuffer> validity_buffer;
          ARROW_ASSIGN_OR_RAISE(
              validity_buffer,
              arrow::AllocateResizableBuffer(arrow::BitUtil::BytesForBits(new_size),
                                             options_.memory_pool));
          new_validity_buffers.push_back(std::move(validity_buffer));
        } else {
          new_validity_buffers.push_back(nullptr);
        }
        fixed_width_idx++;
        break;
      }
    }
  }

  // point to newly allocated buffers
  fixed_width_idx = binary_idx = large_binary_idx = 0;
  list_idx = 0;
  for (auto i = 0; i < num_fields; ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        partition_binary_builders_[binary_idx][partition_id] =
            std::move(new_binary_builders[binary_idx]);
        binary_idx++;
        break;
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        partition_large_binary_builders_[large_binary_idx][partition_id] =
            std::move(new_large_binary_builders[large_binary_idx]);
        large_binary_idx++;
        break;
      case arrow::ListType::type_id:
        partition_list_builders_[list_idx][partition_id] =
            std::move(new_list_builders[list_idx]);
        list_idx++;
        break;
      case arrow::LargeListType::type_id:
        partition_list_builders_[list_idx][partition_id] =
            std::move(new_list_builders[list_idx]);
        list_idx++;
        break;
      case arrow::NullType::type_id:
        break;
      default:
        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] =
            const_cast<uint8_t*>(new_value_buffers[fixed_width_idx]->data());
        if (input_fixed_width_has_null_[fixed_width_idx]) {
          partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] =
              const_cast<uint8_t*>(new_validity_buffers[fixed_width_idx]->data());
        } else {
          partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
        }
        partition_fixed_width_buffers_[fixed_width_idx][partition_id] = {
            std::move(new_validity_buffers[fixed_width_idx]),
            std::move(new_value_buffers[fixed_width_idx])};
        fixed_width_idx++;
        break;
    }
  }
  partition_buffer_size_[partition_id] = new_size;
  return arrow::Status::OK();
}

arrow::Status Splitter::AllocateNew(int32_t partition_id, int32_t new_size) {
  auto status = AllocatePartitionBuffers(partition_id, new_size);
  int32_t retry = 0;
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    std::cout << status.ToString() << std::endl
              << std::to_string(++retry) << " retry to allocate new buffer for partition "
              << std::to_string(partition_id) << std::endl;
    int64_t spilled_size;
    ARROW_ASSIGN_OR_RAISE(auto partition_to_spill, SpillLargestPartition(&spilled_size));
    if (partition_to_spill == -1) {
      std::cout << "Failed to allocate new buffer for partition "
                << std::to_string(partition_id) << ". No partition buffer to spill."
                << std::endl;
      return status;
    }
    status = AllocatePartitionBuffers(partition_id, new_size);
  }
  if (status.IsOutOfMemory()) {
    std::cout << "Failed to allocate new buffer for partition "
              << std::to_string(partition_id) << ". Out of memory." << std::endl;
  }
  return status;
}

arrow::Status Splitter::SpillFixedSize(int64_t size, int64_t* actual) {
  int64_t current_spilled = 0L;
  int32_t try_count = 0;
  while (current_spilled < size && try_count < 5) {
    try_count++;
    int64_t single_call_spilled;
    ARROW_ASSIGN_OR_RAISE(int32_t spilled_partition_id,
                          SpillLargestPartition(&single_call_spilled))
    if (spilled_partition_id == -1) {
      break;
    }
    current_spilled += single_call_spilled;
  }
  *actual = current_spilled;
  return arrow::Status::OK();
}

arrow::Status Splitter::SpillPartition(int32_t partition_id) {
  if (partition_writer_[partition_id] == nullptr) {
    partition_writer_[partition_id] =
        std::make_shared<PartitionWriter>(this, partition_id);
  }
  TIME_NANO_OR_RAISE(total_spill_time_, partition_writer_[partition_id]->Spill());
  return arrow::Status::OK();
}

arrow::Result<int32_t> Splitter::SpillLargestPartition(int64_t* size) {
  // spill the largest partition
  auto max_size = 0;
  int32_t partition_to_spill = -1;
  for (auto i = 0; i < num_partitions_; ++i) {
    if (partition_cached_recordbatch_size_[i] > max_size) {
      max_size = partition_cached_recordbatch_size_[i];
      partition_to_spill = i;
    }
  }
  if (partition_to_spill != -1) {
    RETURN_NOT_OK(SpillPartition(partition_to_spill));
#ifdef DEBUG
    std::cout << "Spilled partition " << std::to_string(partition_to_spill) << ", "
              << std::to_string(max_size) << " bytes released" << std::endl;
#endif
    *size = max_size;
  } else {
    *size = 0;
  }
  return partition_to_spill;
}

arrow::Status Splitter::DoSplit(const arrow::RecordBatch& rb) {
  // for the first input record batch, scan binary arrays and large binary
  // arrays to get their empirical sizes
  uint32_t size_per_row = 0;
  if (!empirical_size_calculated_) {
    auto num_rows = rb.num_rows();
    for (int i = 0; i < binary_array_idx_.size(); ++i) {
      auto arr =
          std::static_pointer_cast<arrow::BinaryArray>(rb.column(binary_array_idx_[i]));
      auto length = arr->value_offset(num_rows) - arr->value_offset(0);
      binary_array_empirical_size_[i] = length / num_rows;
      size_per_row += binary_array_empirical_size_[i];
    }
    for (int i = 0; i < large_binary_array_idx_.size(); ++i) {
      auto arr = std::static_pointer_cast<arrow::LargeBinaryArray>(
          rb.column(large_binary_array_idx_[i]));
      auto length = arr->value_offset(num_rows) - arr->value_offset(0);
      large_binary_array_empirical_size_[i] = length / num_rows;
      size_per_row += large_binary_array_empirical_size_[i];
    }
    empirical_size_calculated_ = true;
  }

  for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
    auto col_idx = fixed_width_array_idx_[col];
    size_per_row += arrow::bit_width(column_type_id_[col]->id()) / 8;
    if (rb.column_data(col_idx)->GetNullCount() != 0) {
      input_fixed_width_has_null_[col] = true;
    }
  }

  int64_t prealloc_row_cnt =
      options_.offheap_per_task > 0 && size_per_row > 0
          ? options_.offheap_per_task / 4 / size_per_row / num_partitions_
          : options_.buffer_size;

  // prepare partition buffers and spill if necessary
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    if (partition_id_cnt_[pid] > 0 &&
        partition_buffer_idx_base_[pid] + partition_id_cnt_[pid] >
            partition_buffer_size_[pid]) {
      auto new_size = std::min((int32_t)prealloc_row_cnt, options_.buffer_size);
      // make sure the splitted record batch can be filled
      if (partition_id_cnt_[pid] > new_size) new_size = partition_id_cnt_[pid];
      if (options_.prefer_spill) {
        if (partition_buffer_size_[pid] == 0) {  // first allocate?
          RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
        } else {  // not first allocate, spill
          if (partition_id_cnt_[pid] > partition_buffer_size_[pid]) {  // need reallocate?
            // TODO(): CacheRecordBatch will try to reset builder buffer
            // AllocatePartitionBuffers will then Reserve memory for builder based on last
            // recordbatch, the logic on reservation size should be cleaned up
            RETURN_NOT_OK(CacheRecordBatch(pid, true));
            RETURN_NOT_OK(SpillPartition(pid));
            RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
          } else {
            RETURN_NOT_OK(CacheRecordBatch(pid, false));
            RETURN_NOT_OK(SpillPartition(pid));
          }
        }
      } else {
        RETURN_NOT_OK(CacheRecordBatch(pid, true));
#ifdef DEBUG
        std::cout << "Attempt to allocate partition buffer, partition id: " +
                         std::to_string(pid) + ", old buffer size: " +
                         std::to_string(partition_buffer_size_[pid]) +
                         ", new buffer size: " + std::to_string(new_size) +
                         ", input record batch size: " + std::to_string(rb.num_rows())
                  << std::endl;
#endif
        RETURN_NOT_OK(AllocateNew(pid, new_size));
      }
    }
  }
#ifdef DEBUG
  std::cout << "Total bytes allocated: " << options_.memory_pool->bytes_allocated()
            << std::endl;
#endif

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
  RETURN_NOT_OK(SplitFixedWidthValueBufferAVX(rb));
#else
  RETURN_NOT_OK(SplitFixedWidthValueBuffer(rb));
#endif
  RETURN_NOT_OK(SplitFixedWidthValidityBuffer(rb));
  RETURN_NOT_OK(SplitBinaryArray(rb));
  RETURN_NOT_OK(SplitLargeBinaryArray(rb));
  RETURN_NOT_OK(SplitListArray(rb));

  // update partition buffer base
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    partition_buffer_idx_base_[pid] += partition_id_cnt_[pid];
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitFixedWidthValueBuffer(const arrow::RecordBatch& rb) {
  const auto num_rows = rb.num_rows();
  for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
    std::fill(std::begin(partition_buffer_idx_offset_),
              std::end(partition_buffer_idx_offset_), 0);
    auto col_idx = fixed_width_array_idx_[col];
    auto src_addr = const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[1]->data());
    const auto& dst_addrs = partition_fixed_width_value_addrs_[col];
    switch (column_type_id_[col_idx]->id()) {
#define PROCESS(SHUFFLE_TYPE, _CTYPE)                                               \
  case SHUFFLE_TYPE::type_id:                                                       \
    for (auto row = 0; row < num_rows; ++row) {                                     \
      auto pid = partition_id_[row];                                                \
      auto dst_offset =                                                             \
          partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];      \
      reinterpret_cast<arrow::TypeTraits<SHUFFLE_TYPE>::CType*>(                    \
          dst_addrs[pid])[dst_offset] =                                             \
          reinterpret_cast<arrow::TypeTraits<SHUFFLE_TYPE>::CType*>(src_addr)[row]; \
      partition_buffer_idx_offset_[pid]++;                                          \
      _mm_prefetch(&reinterpret_cast<arrow::TypeTraits<SHUFFLE_TYPE>::CType*>(      \
                       dst_addrs[pid])[dst_offset + 1],                             \
                   _MM_HINT_T0);                                                    \
    }                                                                               \
    break;
      PROCESS(arrow::Int8Type, uint8_t)
      PROCESS(arrow::UInt8Type, uint8_t)
      PROCESS(arrow::Int16Type, uint16_t)
      PROCESS(arrow::UInt16Type, uint16_t)
      PROCESS(arrow::Int32Type, uint32_t)
      PROCESS(arrow::UInt32Type, uint32_t)
      PROCESS(arrow::FloatType, uint32_t)
      PROCESS(arrow::Date32Type, uint32_t)
      PROCESS(arrow::Time32Type, uint32_t)
      PROCESS(arrow::Int64Type, uint64_t)
      PROCESS(arrow::UInt64Type, uint64_t)
      PROCESS(arrow::DoubleType, uint64_t)
      PROCESS(arrow::Date64Type, uint64_t)
      PROCESS(arrow::Time64Type, uint64_t)
      PROCESS(arrow::TimestampType, uint64_t)
#undef PROCESS
      case arrow::Decimal128Type::type_id:
        for (auto row = 0; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          auto dst_offset =
              (partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid]) << 1;
          reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset] =
              reinterpret_cast<uint64_t*>(src_addr)[row << 1];
          reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset | 1] =
              reinterpret_cast<uint64_t*>(src_addr)[row << 1 | 1];
          partition_buffer_idx_offset_[pid]++;
          _mm_prefetch(&reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset + 2],
                       _MM_HINT_T0);
        }
        break;
      case arrow::BooleanType::type_id:
        for (auto row = 0; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          auto dst_offset =
              partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
          dst_addrs[pid][dst_offset >> 3] ^=
              (dst_addrs[pid][dst_offset >> 3] >> (dst_offset & 7) ^
               src_addr[row >> 3] >> (row & 7))
              << (dst_offset & 7);
          partition_buffer_idx_offset_[pid]++;
        }
        break;
      default:
        return arrow::Status::Invalid("Column type " +
                                      schema_->field(col_idx)->type()->ToString() +
                                      " is not fixed width");
    }
  }
  return arrow::Status::OK();
}

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
arrow::Status Splitter::SplitFixedWidthValueBufferAVX(const arrow::RecordBatch& rb) {
  __m256i inc_one = _mm256_load_si256((__m256i*)(ONES));

  const auto num_rows = rb.num_rows();
  for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
    std::fill(std::begin(partition_buffer_idx_offset_),
              std::end(partition_buffer_idx_offset_), 0);
    auto col_idx = fixed_width_array_idx_[col];
    auto src_addr = const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[1]->data());
    const auto& dst_addrs = partition_fixed_width_value_addrs_[col];

    switch (column_type_id_[col_idx]) {
#define PROCESS(SHUFFLE_TYPE, CTYPE)                                           \
  case Type::SHUFFLE_TYPE:                                                     \
    for (auto row = 0; row < num_rows; ++row) {                                \
      auto pid = partition_id_[row];                                           \
      auto dst_offset =                                                        \
          partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid]; \
      reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset] =                   \
          reinterpret_cast<CTYPE*>(src_addr)[row];                             \
      partition_buffer_idx_offset_[pid]++;                                     \
      _mm_prefetch(&reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset + 1],  \
                   _MM_HINT_T0);                                               \
    }                                                                          \
    break;
      PROCESS(SHUFFLE_1BYTE, uint8_t)
      PROCESS(SHUFFLE_2BYTE, uint16_t)
#undef PROCESS
      case Type::SHUFFLE_4BYTE: {
        auto rows = num_rows - num_rows % 8;
        auto src_addr_32 = reinterpret_cast<uint32_t*>(src_addr);
        for (auto row = 0; row < rows; row += 8) {
          __m256i partid_cnt_8x = CountPartitionIdOccurrence(partition_id_, row);

          // partition id is 32 bit, 8 partition id
          __m256i partid_8x = _mm256_loadu_si256((__m256i*)(partition_id_.data() + row));

          // dst_base and dst_offset are 32 bit
          __m256i dst_idx_base_8x =
              _mm256_i32gather_epi32(partition_buffer_idx_base_.data(), partid_8x, 4);
          __m256i dst_idx_offset_8x =
              _mm256_i32gather_epi32(partition_buffer_idx_offset_.data(), partid_8x, 4);
          dst_idx_offset_8x = _mm256_add_epi32(dst_idx_offset_8x, partid_cnt_8x);
          __m256i dst_idx_8x = _mm256_add_epi32(dst_idx_base_8x, dst_idx_offset_8x);

          // dst base address is 64 bit
          __m512i dst_addr_base_8x =
              _mm512_i32gather_epi64(partid_8x, dst_addrs.data(), 8);

          // calculate dst address, dst_addr = dst_base_addr + dst_idx*4
          //_mm512_cvtepu32_epi64: zero extend dst_offset 32bit -> 64bit
          //_mm512_slli_epi64(_, 2): each 64bit dst_offset << 2
          __m512i dst_addr_offset_8x =
              _mm512_slli_epi64(_mm512_cvtepu32_epi64(dst_idx_8x), 2);
          __m512i dst_addr_8x = _mm512_add_epi64(dst_addr_base_8x, dst_addr_offset_8x);

          // source value is 32 bit
          __m256i src_val_8x = _mm256_loadu_si256((__m256i*)(src_addr_32 + row));

          // scatter
          _mm512_i64scatter_epi32(nullptr, dst_addr_8x, src_val_8x, 1);

          // update partition_buffer_idx_offset_
          partid_cnt_8x = _mm256_add_epi32(partid_cnt_8x, inc_one);
          for (int i = 0; i < 8; ++i) {
            partition_buffer_idx_offset_[partition_id_[row + i]]++;
          }

          PrefetchDstAddr(dst_addr_8x, 4);
        }
        for (auto row = rows; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          reinterpret_cast<uint32_t*>(dst_addrs[pid])[partition_buffer_idx_base_[pid] +
                                                      partition_buffer_idx_offset_[pid]] =
              (src_addr_32)[row];
          partition_buffer_idx_offset_[pid]++;
        }
      } break;
      case Type::SHUFFLE_8BYTE: {
        auto rows = num_rows - num_rows % 8;
        auto src_addr_64 = reinterpret_cast<uint64_t*>(src_addr);
        for (auto row = 0; row < rows; row += 8) {
          __m256i partid_cnt_8x = CountPartitionIdOccurrence(partition_id_, row);

          // partition id is 32 bit, 8 partition id
          __m256i partid_8x = _mm256_loadu_si256((__m256i*)(partition_id_.data() + row));

          // dst_base and dst_offset are 32 bit
          __m256i dst_idx_base_8x =
              _mm256_i32gather_epi32(partition_buffer_idx_base_.data(), partid_8x, 4);
          __m256i dst_idx_offset_8x =
              _mm256_i32gather_epi32(partition_buffer_idx_offset_.data(), partid_8x, 4);
          dst_idx_offset_8x = _mm256_add_epi32(dst_idx_offset_8x, partid_cnt_8x);
          __m256i dst_idx_8x = _mm256_add_epi32(dst_idx_base_8x, dst_idx_offset_8x);

          // dst base address is 64 bit
          __m512i dst_addr_base_8x =
              _mm512_i32gather_epi64(partid_8x, dst_addrs.data(), 8);

          // calculate dst address, dst_addr = dst_base_addr + dst_idx*8
          //_mm512_cvtepu32_epi64: zero extend dst_offset 32bit -> 64bit
          //_mm512_slli_epi64(_, 3): each 64bit dst_offset << 3
          __m512i dst_addr_offset_8x =
              _mm512_slli_epi64(_mm512_cvtepu32_epi64(dst_idx_8x), 3);
          __m512i dst_addr_8x = _mm512_add_epi64(dst_addr_base_8x, dst_addr_offset_8x);

          // source value is 64 bit
          __m512i src_val_8x = _mm512_loadu_si512((__m512i*)(src_addr_64 + row));

          // scatter
          _mm512_i64scatter_epi64(nullptr, dst_addr_8x, src_val_8x, 1);

          // update partition_buffer_idx_offset_
          partid_cnt_8x = _mm256_add_epi32(partid_cnt_8x, inc_one);
          for (int i = 0; i < 8; ++i) {
            partition_buffer_idx_offset_[partition_id_[row + i]]++;
          }

          PrefetchDstAddr(dst_addr_8x, 8);
        }
        // handle the rest
        for (auto row = rows; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          reinterpret_cast<uint64_t*>(dst_addrs[pid])[partition_buffer_idx_base_[pid] +
                                                      partition_buffer_idx_offset_[pid]] =
              (src_addr_64)[row];
          partition_buffer_idx_offset_[pid]++;
        }
      } break;
      case Type::SHUFFLE_DECIMAL128:
        for (auto row = 0; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          auto dst_offset =
              (partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid]) << 1;
          reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset] =
              reinterpret_cast<uint64_t*>(src_addr)[row << 1];
          reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset | 1] =
              reinterpret_cast<uint64_t*>(src_addr)[row << 1 | 1];
          partition_buffer_idx_offset_[pid]++;
          _mm_prefetch(&reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset + 2],
                       _MM_HINT_T0);
        }
        break;
      case Type::SHUFFLE_BIT:
        for (auto row = 0; row < num_rows; ++row) {
          auto pid = partition_id_[row];
          auto dst_offset =
              partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
          dst_addrs[pid][dst_offset >> 3] ^=
              (dst_addrs[pid][dst_offset >> 3] >> (dst_offset & 7) ^
               src_addr[row >> 3] >> (row & 7))
              << (dst_offset & 7);
          partition_buffer_idx_offset_[pid]++;
        }
        break;
      default:
        return arrow::Status::Invalid("Column type " +
                                      schema_->field(col_idx)->type()->ToString() +
                                      " is not fixed width");
    }
  }
  return arrow::Status::OK();
}
#endif

arrow::Status Splitter::SplitFixedWidthValidityBuffer(const arrow::RecordBatch& rb) {
  const auto num_rows = rb.num_rows();
  for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
    auto col_idx = fixed_width_array_idx_[col];
    auto& dst_addrs = partition_fixed_width_validity_addrs_[col];
    if (rb.column_data(col_idx)->GetNullCount() == 0) {
      for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (partition_id_cnt_[pid] > 0 && dst_addrs[pid] != nullptr) {
          arrow::BitUtil::SetBitsTo(dst_addrs[pid], partition_buffer_idx_base_[pid],
                                    partition_id_cnt_[pid], true);
        }
      }
    } else {
      for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (partition_id_cnt_[pid] > 0 && dst_addrs[pid] == nullptr) {
          // init bitmap if it's null
          auto new_size = partition_id_cnt_[pid] > options_.buffer_size
                              ? partition_id_cnt_[pid]
                              : options_.buffer_size;
          ARROW_ASSIGN_OR_RAISE(
              auto validity_buffer,
              arrow::AllocateResizableBuffer(arrow::BitUtil::BytesForBits(new_size),
                                             options_.memory_pool));
          dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data());
          arrow::BitUtil::SetBitsTo(dst_addrs[pid], 0, partition_buffer_idx_base_[pid],
                                    true);
          partition_fixed_width_buffers_[col][pid][0] = std::move(validity_buffer);
        }
      }

      auto src_addr = const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[0]->data());
      std::fill(std::begin(partition_buffer_idx_offset_),
                std::end(partition_buffer_idx_offset_), 0);
      for (auto row = 0; row < num_rows; ++row) {
        auto pid = partition_id_[row];
        auto dst_offset =
            partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
        dst_addrs[pid][dst_offset >> 3] ^=
            (dst_addrs[pid][dst_offset >> 3] >> (dst_offset & 7) ^
             src_addr[row >> 3] >> (row & 7))
            << (dst_offset & 7);
        partition_buffer_idx_offset_[pid]++;
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitBinaryArray(const arrow::RecordBatch& rb) {
  for (int i = 0; i < binary_array_idx_.size(); ++i) {
    RETURN_NOT_OK(AppendBinary<arrow::BinaryType>(
        std::static_pointer_cast<arrow::BinaryArray>(rb.column(binary_array_idx_[i])),
        partition_binary_builders_[i], rb.num_rows()));
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitLargeBinaryArray(const arrow::RecordBatch& rb) {
  for (int i = 0; i < large_binary_array_idx_.size(); ++i) {
    RETURN_NOT_OK(AppendBinary<arrow::LargeBinaryType>(
        std::static_pointer_cast<arrow::LargeBinaryArray>(
            rb.column(large_binary_array_idx_[i])),
        partition_large_binary_builders_[i], rb.num_rows()));
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
arrow::Status Splitter::SplitListArray(const arrow::RecordBatch& rb) {
  for (int i = 0; i < list_array_idx_.size(); ++i) {
    auto src_arr =
        std::static_pointer_cast<arrow::ListArray>(rb.column(list_array_idx_[i]));
    auto status = AppendList(rb.column(list_array_idx_[i]), partition_list_builders_[i],
                             rb.num_rows());
    if (!status.ok()) return status;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

template <typename T, typename ArrayType, typename BuilderType>
arrow::Status Splitter::AppendBinary(
    const std::shared_ptr<ArrayType>& src_arr,
    const std::vector<std::shared_ptr<BuilderType>>& dst_builders, int64_t num_rows) {
  using offset_type = typename T::offset_type;
  if (src_arr->null_count() == 0) {
    for (auto row = 0; row < num_rows; ++row) {
      offset_type length;
      auto value = src_arr->GetValue(row, &length);
      const auto& builder = dst_builders[partition_id_[row]];
      RETURN_NOT_OK(builder->Reserve(1));
      RETURN_NOT_OK(builder->ReserveData(length));
      builder->UnsafeAppend(value, length);
    }
  } else {
    for (auto row = 0; row < num_rows; ++row) {
      if (src_arr->IsValid(row)) {
        offset_type length;
        auto value = src_arr->GetValue(row, &length);
        const auto& builder = dst_builders[partition_id_[row]];
        RETURN_NOT_OK(builder->Reserve(1));
        RETURN_NOT_OK(builder->ReserveData(length));
        builder->UnsafeAppend(value, length);
      } else {
        dst_builders[partition_id_[row]]->AppendNull();
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::AppendList(
    const std::shared_ptr<arrow::Array>& src_arr,
    const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dst_builders,
    int64_t num_rows) {
  for (auto row = 0; row < num_rows; ++row) {
    RETURN_NOT_OK(dst_builders[partition_id_[row]]->AppendArraySlice(
        *(src_arr->data().get()), row, 1));
  }
  return arrow::Status::OK();
}

std::string Splitter::NextSpilledFileDir() {
  auto spilled_file_dir = GetSpilledShuffleFileDir(configured_dirs_[dir_selection_],
                                                   sub_dir_selection_[dir_selection_]);
  sub_dir_selection_[dir_selection_] =
      (sub_dir_selection_[dir_selection_] + 1) % options_.num_sub_dirs;
  dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
  return spilled_file_dir;
}

arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> Splitter::GetSchemaPayload() {
  if (schema_payload_ != nullptr) {
    return schema_payload_;
  }
  schema_payload_ = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::ipc::DictionaryFieldMapper dict_file_mapper;  // unused
  RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(*schema_, options_.ipc_write_options,
                                             dict_file_mapper, schema_payload_.get()));
  return schema_payload_;
}

// ----------------------------------------------------------------------
// RoundRobinSplitter

arrow::Result<std::shared_ptr<RoundRobinSplitter>> RoundRobinSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema, SplitOptions options) {
  std::shared_ptr<RoundRobinSplitter> res(
      new RoundRobinSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status RoundRobinSplitter::ComputeAndCountPartitionId(
    const arrow::RecordBatch& rb) {
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  partition_id_.resize(rb.num_rows());
  for (auto& pid : partition_id_) {
    pid = pid_selection_;
    partition_id_cnt_[pid_selection_]++;
    pid_selection_ = (pid_selection_ + 1) % num_partitions_;
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// HashSplitter

arrow::Result<std::shared_ptr<HashSplitter>> HashSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
    const gandiva::ExpressionVector& expr_vector, SplitOptions options) {
  std::shared_ptr<HashSplitter> res(
      new HashSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  RETURN_NOT_OK(res->CreateProjector(expr_vector));
  return res;
}

arrow::Status HashSplitter::CreateProjector(
    const gandiva::ExpressionVector& expr_vector) {
  // same seed as spark's
  auto hash = gandiva::TreeExprBuilder::MakeLiteral((int32_t)42);
  for (const auto& expr : expr_vector) {
    switch (expr->root()->return_type()->id()) {
      case arrow::NullType::type_id:
        break;
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::Int32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash32_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::Int64Type::type_id:
      case arrow::DoubleType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash64_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::StringType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hashbuf_spark", {expr->root(), hash}, arrow::int32());
        break;
      default:
        hash = gandiva::TreeExprBuilder::MakeFunction("hash32", {expr->root(), hash},
                                                      arrow::int32());
        /*return arrow::Status::NotImplemented("HashSplitter::CreateProjector
           doesn't support type ", expr->result()->type()->ToString());*/
    }
  }
  auto hash_expr =
      gandiva::TreeExprBuilder::MakeExpression(hash, arrow::field("pid", arrow::int32()));
  return gandiva::Projector::Make(schema_, {hash_expr}, &projector_);
}

arrow::Status HashSplitter::ComputeAndCountPartitionId(const arrow::RecordBatch& rb) {
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);

  arrow::ArrayVector outputs;
  TIME_NANO_OR_RAISE(total_compute_pid_time_,
                     projector_->Evaluate(rb, options_.memory_pool, &outputs));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("Projector result should have one field, actual is ",
                                  std::to_string(outputs.size()));
  }
  auto pid_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  if (pid_arr == nullptr) {
    return arrow::Status::Invalid("failed to cast outputs.at(0)");
  }
  for (auto i = 0; i < num_rows; ++i) {
    // positive mod
    auto pid = pid_arr->Value(i) % num_partitions_;
    if (pid < 0) pid = (pid + num_partitions_) % num_partitions_;
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// FallBackRangeSplitter

arrow::Result<std::shared_ptr<FallbackRangeSplitter>> FallbackRangeSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema, SplitOptions options) {
  auto res = std::shared_ptr<FallbackRangeSplitter>(
      new FallbackRangeSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status FallbackRangeSplitter::Init() {
  input_schema_ = std::move(schema_);
  ARROW_ASSIGN_OR_RAISE(schema_, input_schema_->RemoveField(0))
  return Splitter::Init();
}

arrow::Status FallbackRangeSplitter::Split(const arrow::RecordBatch& rb) {
  EVAL_START("split", options_.thread_id)
  RETURN_NOT_OK(ComputeAndCountPartitionId(rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb.RemoveColumn(0));
  RETURN_NOT_OK(DoSplit(*remove_pid));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status FallbackRangeSplitter::ComputeAndCountPartitionId(
    const arrow::RecordBatch& rb) {
  if (rb.column(0)->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("RecordBatch field 0 should be ",
                                  arrow::int32()->ToString(), ", actual is ",
                                  rb.column(0)->type()->ToString());
  }

  auto pid_arr = reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i];
    if (pid >= num_partitions_) {
      return arrow::Status::Invalid("Partition id ", std::to_string(pid),
                                    " is equal or greater than ",
                                    std::to_string(num_partitions_));
    }
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
