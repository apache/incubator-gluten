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

#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/projector.h>

#include <random>
#include <utility>

#include "shuffle/type.h"
#include "shuffle/utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

class Splitter {
 public:
  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions, const gandiva::ExpressionVector& expr_vector,
      SplitOptions options = SplitOptions::Defaults());

  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions, SplitOptions options = SplitOptions::Defaults());

  virtual const std::shared_ptr<arrow::Schema>& input_schema() const { return schema_; }

  /**
   * Split input record batch into partition buffers according to the computed
   * partition id. The largest partition buffer will be spilled if memory
   * allocation failure occurs.
   */
  virtual arrow::Status Split(const arrow::RecordBatch&);

  /**
   * Compute the compresse size of record batch.
   */
  virtual int64_t CompressedSize(const arrow::RecordBatch&);

  /**
   * For each partition, merge spilled file into shuffle data file and write any
   * cached record batch to shuffle data file. Close all resources and collect
   * metrics.
   */
  arrow::Status Stop();

  /**
   * Spill specified partition
   */
  arrow::Status SpillPartition(int32_t partition_id);

  arrow::Status SetCompressType(arrow::Compression::type compressed_type);

  /**
   * Spill for fixed size of partition data
   */
  arrow::Status SpillFixedSize(int64_t size, int64_t* actual);

  /**
   * Spill the largest partition buffer
   * @return partition id. If no partition to spill, return -1
   */
  arrow::Result<int32_t> SpillLargestPartition(int64_t* size);

  int64_t TotalBytesWritten() const { return total_bytes_written_; }

  int64_t TotalBytesSpilled() const { return total_bytes_spilled_; }

  int64_t TotalWriteTime() const { return total_write_time_; }

  int64_t TotalSpillTime() const { return total_spill_time_; }

  int64_t TotalCompressTime() const { return total_compress_time_; }

  int64_t TotalComputePidTime() const { return total_compute_pid_time_; }

  const std::vector<int64_t>& PartitionLengths() const { return partition_lengths_; }

  const std::vector<int64_t>& RawPartitionLengths() const {
    return raw_partition_lengths_;
  }

  // for testing
  const std::string& DataFile() const { return options_.data_file; }

 protected:
  Splitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
           SplitOptions options)
      : num_partitions_(num_partitions),
        schema_(std::move(schema)),
        options_(std::move(options)) {}

  virtual arrow::Status Init();

  virtual arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) = 0;

  arrow::Status DoSplit(const arrow::RecordBatch& rb);

  arrow::Status SplitFixedWidthValueBuffer(const arrow::RecordBatch& rb);

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
  arrow::Status SplitFixedWidthValueBufferAVX(const arrow::RecordBatch& rb);
#endif

  arrow::Status SplitFixedWidthValidityBuffer(const arrow::RecordBatch& rb);

  arrow::Status SplitBinaryArray(const arrow::RecordBatch& rb);

  arrow::Status SplitLargeBinaryArray(const arrow::RecordBatch& rb);

  arrow::Status SplitListArray(const arrow::RecordBatch& rb);

  template <typename T, typename ArrayType = typename arrow::TypeTraits<T>::ArrayType,
            typename BuilderType = typename arrow::TypeTraits<T>::BuilderType>
  arrow::Status AppendBinary(
      const std::shared_ptr<ArrayType>& src_arr,
      const std::vector<std::shared_ptr<BuilderType>>& dst_builders, int64_t num_rows);

  arrow::Status AppendList(
      const std::shared_ptr<arrow::Array>& src_arr,
      const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dst_builders,
      int64_t num_rows);

  // Cache the partition buffer/builder as compressed record batch. If reset
  // buffers, the partition buffer/builder will be set to nullptr. Two cases for
  // caching the partition buffers as record batch:
  // 1. Split record batch. It first calculate whether the partition
  // buffer can hold all data according to partition id. If not, call this
  // method and allocate new buffers. Spill will happen if OOM.
  // 2. Stop the splitter. The record batch will be written to disk immediately.
  arrow::Status CacheRecordBatch(int32_t partition_id, bool reset_buffers);

  // Allocate new partition buffer/builder.
  // If successful, will point partition buffer/builder to new ones, otherwise
  // will spill the largest partition and retry
  arrow::Status AllocateNew(int32_t partition_id, int32_t new_size);

  // Allocate new partition buffer/builder. May return OOM status.
  arrow::Status AllocatePartitionBuffers(int32_t partition_id, int32_t new_size);

  std::string NextSpilledFileDir();

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> GetSchemaPayload();

  class PartitionWriter;

  std::vector<int32_t> partition_buffer_size_;
  std::vector<int32_t> partition_buffer_idx_base_;
  std::vector<int32_t> partition_buffer_idx_offset_;

  std::vector<std::shared_ptr<PartitionWriter>> partition_writer_;
  std::vector<std::vector<uint8_t*>> partition_fixed_width_validity_addrs_;
  std::vector<std::vector<uint8_t*>> partition_fixed_width_value_addrs_;
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>>
      partition_fixed_width_buffers_;
  std::vector<std::vector<std::shared_ptr<arrow::BinaryBuilder>>>
      partition_binary_builders_;
  std::vector<std::vector<std::shared_ptr<arrow::LargeBinaryBuilder>>>
      partition_large_binary_builders_;
  std::vector<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> partition_list_builders_;
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>>
      partition_cached_recordbatch_;
  std::vector<int64_t> partition_cached_recordbatch_size_;  // in bytes

  std::vector<int32_t> fixed_width_array_idx_;
  std::vector<int32_t> binary_array_idx_;
  std::vector<int32_t> large_binary_array_idx_;
  std::vector<int32_t> list_array_idx_;

  bool empirical_size_calculated_ = false;
  std::vector<int32_t> binary_array_empirical_size_;
  std::vector<int32_t> large_binary_array_empirical_size_;

  std::vector<bool> input_fixed_width_has_null_;

  // updated for each input record batch
  std::vector<int32_t> partition_id_;
  std::vector<int32_t> partition_id_cnt_;

  int32_t num_partitions_;
  std::shared_ptr<arrow::Schema> schema_;
  SplitOptions options_;

  // write options for tiny batches
  arrow::ipc::IpcWriteOptions tiny_bach_write_options_;

  int64_t total_bytes_written_ = 0;
  int64_t total_bytes_spilled_ = 0;
  int64_t total_write_time_ = 0;
  int64_t total_spill_time_ = 0;
  int64_t total_compress_time_ = 0;
  int64_t total_compute_pid_time_ = 0;
  int64_t peak_memory_allocated_ = 0;

  std::vector<int64_t> partition_lengths_;
  std::vector<int64_t> raw_partition_lengths_;

  std::vector<std::shared_ptr<arrow::DataType>> column_type_id_;

  // configured local dirs for spilled file
  int32_t dir_selection_ = 0;
  std::vector<int32_t> sub_dir_selection_;
  std::vector<std::string> configured_dirs_;

  std::shared_ptr<arrow::io::OutputStream> data_file_os_;

  // shared by all partition writers
  std::shared_ptr<arrow::ipc::IpcPayload> schema_payload_;
};

class RoundRobinSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<RoundRobinSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      SplitOptions options);

 private:
  RoundRobinSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
                     SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  int32_t pid_selection_ = 0;
};

class HashSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<HashSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      const gandiva::ExpressionVector& expr_vector, SplitOptions options);

 private:
  HashSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
               SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  arrow::Status CreateProjector(const gandiva::ExpressionVector& expr_vector);

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  std::shared_ptr<gandiva::Projector> projector_;
};

class FallbackRangeSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<FallbackRangeSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      SplitOptions options);

  arrow::Status Split(const arrow::RecordBatch& rb) override;

  const std::shared_ptr<arrow::Schema>& input_schema() const override {
    return input_schema_;
  }

 private:
  FallbackRangeSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
                        SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  arrow::Status Init() override;

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  std::shared_ptr<arrow::Schema> input_schema_;
};

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
