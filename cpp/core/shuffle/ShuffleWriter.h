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

#include <utility>

#include "memory/ColumnarBatch.h"
#include "shuffle/type.h"

namespace gluten {

class ShuffleWriter {
 public:
  /**
   * Evict fixed size of partition data from memory
   */
  virtual arrow::Status EvictFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status Split(ColumnarBatch* cb) = 0;

  // Cache the partition buffer/builder as compressed record batch. If reset
  // buffers, the partition buffer/builder will be set to nullptr. Two cases for
  // caching the partition buffers as record batch:
  // 1. Split record batch. It first calculate whether the partition
  // buffer can hold all data according to partition id. If not, call this
  // method and allocate new buffers. Spill will happen if OOM.
  // 2. Stop the shuffle writer. The record batch will be written to disk immediately.
  virtual arrow::Status CreateRecordBatchFromBuffer(uint32_t partition_id, bool reset_buffers) = 0;

  virtual arrow::Status Stop() = 0;

  int64_t TotalBytesWritten() const {
    return total_bytes_written_;
  }

  int64_t TotalBytesEvicted() const {
    return total_bytes_evicted_;
  }

  int64_t TotalWriteTime() const {
    return total_write_time_;
  }

  int64_t TotalEvictTime() const {
    return total_evict_time_;
  }

  int64_t TotalCompressTime() const {
    return total_compress_time_;
  }

  const std::vector<int64_t>& PartitionLengths() const {
    return partition_lengths_;
  }

  const std::vector<int64_t>& RawPartitionLengths() const {
    return raw_partition_lengths_;
  }

  const std::vector<int64_t>& PartitionCachedRecordbatchSize() const {
    return partition_cached_recordbatch_size_;
  }

  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>>& PartitionCachedRecordbatch() {
    return partition_cached_recordbatch_;
  }

  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>>& PartitionBuffer() {
    return partition_buffers_;
  }

  std::shared_ptr<arrow::ResizableBuffer>& CombineBuffer() {
    return combine_buffer_;
  }

  SplitOptions& Options() {
    return options_;
  }

  std::shared_ptr<arrow::Schema>& Schema() {
    return schema_;
  }

  void SetPartitionLengths(int32_t index, int64_t length) {
    partition_lengths_[index] = length;
  }

  void SetTotalWriteTime(int64_t total_write_time) {
    total_write_time_ = total_write_time;
  }

  void SetTotalBytesWritten(int64_t total_bytes_written) {
    total_bytes_written_ = total_bytes_written;
  }

  void SetTotalEvictTime(int64_t total_evict_time) {
    total_evict_time_ = total_evict_time;
  }

  void SetTotalBytesEvicted(int64_t total_bytes_evicted) {
    total_bytes_evicted_ = total_bytes_evicted;
  }

  void SetPartitionCachedRecordbatchSize(int32_t index, int64_t size) {
    partition_cached_recordbatch_size_[index] = size;
  }

  class PartitionWriter;

  class Partitioner;

 protected:
  ShuffleWriter(int32_t num_partitions, SplitOptions options)
      : num_partitions_(num_partitions), options_(std::move(options)) {}
  virtual ~ShuffleWriter() = default;

  int32_t num_partitions_;
  // options
  SplitOptions options_;

  int64_t total_bytes_written_ = 0;
  int64_t total_bytes_evicted_ = 0;
  int64_t total_write_time_ = 0;
  int64_t total_evict_time_ = 0;
  int64_t total_compress_time_ = 0;
  int64_t peak_memory_allocated_ = 0;

  std::vector<int64_t> partition_lengths_;
  std::vector<int64_t> raw_partition_lengths_;

  std::shared_ptr<arrow::Schema> schema_;

  std::vector<int64_t> partition_cached_recordbatch_size_; // in bytes
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>> partition_cached_recordbatch_;

  // col partid
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>> partition_buffers_;

  // slice the buffer for each reducer's column, in this way we can combine into
  // large page
  std::shared_ptr<arrow::ResizableBuffer> combine_buffer_;

  std::shared_ptr<PartitionWriter> partition_writer_;

  std::shared_ptr<Partitioner> partitioner_;
};

} // namespace gluten
