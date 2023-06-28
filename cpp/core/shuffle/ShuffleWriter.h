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

#include <numeric>
#include <utility>

#include "memory/ColumnarBatch.h"
#include "shuffle/type.h"

namespace gluten {

class ShuffleWriter {
 public:
  /**
   * Evict fixed size of partition data from memory
   */
  virtual arrow::Status evictFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status split(ColumnarBatch* cb) = 0;

  // Cache the partition buffer/builder as compressed record batch. If reset
  // buffers, the partition buffer/builder will be set to nullptr. Two cases for
  // caching the partition buffers as record batch:
  // 1. Split record batch. It first calculates whether the partition
  // buffer can hold all data according to partition id. If not, call this
  // method and allocate new buffers. Spill will happen if OOM.
  // 2. Stop the shuffle writer. The record batch will be written to disk immediately.
  virtual arrow::Status createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) = 0;

  virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> createArrowRecordBatchFromBuffer(
      uint32_t partitionId,
      bool resetBuffers) = 0;

  virtual arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> createArrowIpcPayload(
      const arrow::RecordBatch& rb,
      bool reuseBuffers) = 0;

  virtual arrow::Status stop() = 0;

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t totalBytesWritten() const {
    return totalBytesWritten_;
  }

  int64_t totalBytesEvicted() const {
    return totalBytesEvicted_;
  }

  int64_t totalWriteTime() const {
    return totalWriteTime_;
  }

  int64_t totalEvictTime() const {
    return totalEvictTime_;
  }

  int64_t totalCompressTime() const {
    return totalCompressTime_;
  }

  const std::vector<int64_t>& partitionLengths() const {
    return partitionLengths_;
  }

  const std::vector<int64_t>& rawPartitionLengths() const {
    return rawPartitionLengths_;
  }

  const std::vector<int64_t>& partitionCachedRecordbatchSize() const {
    return partitionCachedRecordbatchSize_;
  }

  const int64_t totalCachedPayloadSize() const {
    return std::accumulate(partitionCachedRecordbatchSize_.begin(), partitionCachedRecordbatchSize_.end(), 0);
  }

  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>>& partitionCachedRecordbatch() {
    return partitionCachedRecordbatch_;
  }

  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>>& partitionBuffer() {
    return partitionBuffers_;
  }

  std::shared_ptr<arrow::ResizableBuffer>& combineBuffer() {
    return combineBuffer_;
  }

  ShuffleWriterOptions& options() {
    return options_;
  }

  std::shared_ptr<arrow::Schema>& schema() {
    return schema_;
  }

  void setPartitionLengths(int32_t index, int64_t length) {
    partitionLengths_[index] = length;
  }

  void setRawPartitionLength(int32_t index, int64_t length) {
    rawPartitionLengths_[index] = length;
  }

  void setTotalWriteTime(int64_t totalWriteTime) {
    totalWriteTime_ = totalWriteTime;
  }

  void setTotalBytesWritten(int64_t totalBytesWritten) {
    totalBytesWritten_ = totalBytesWritten;
  }

  void setTotalEvictTime(int64_t totalEvictTime) {
    totalEvictTime_ = totalEvictTime;
  }

  void setTotalBytesEvicted(int64_t totalBytesEvicted) {
    totalBytesEvicted_ = totalBytesEvicted;
  }

  void setPartitionCachedRecordbatchSize(int32_t index, int64_t size) {
    partitionCachedRecordbatchSize_[index] = size;
  }

  class PartitionWriter;

  class Partitioner;

  class PartitionWriterCreator;

 protected:
  ShuffleWriter(
      int32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options)
      : numPartitions_(numPartitions),
        partitionWriterCreator_(std::move(partitionWriterCreator)),
        options_(std::move(options)) {}
  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  std::shared_ptr<PartitionWriterCreator> partitionWriterCreator_;
  // options
  ShuffleWriterOptions options_;

  int64_t totalBytesWritten_ = 0;
  int64_t totalBytesEvicted_ = 0;
  int64_t totalWriteTime_ = 0;
  int64_t totalEvictTime_ = 0;
  int64_t totalCompressTime_ = 0;
  int64_t peakMemoryAllocated_ = 0;

  std::vector<int64_t> partitionLengths_;
  std::vector<int64_t> rawPartitionLengths_;

  std::shared_ptr<arrow::Schema> schema_;

  std::vector<int64_t> partitionCachedRecordbatchSize_; // in bytes
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>> partitionCachedRecordbatch_;

  // col partid
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>> partitionBuffers_;

  // slice the buffer for each reducer's column, in this way we can combine into
  // large page
  std::shared_ptr<arrow::ResizableBuffer> combineBuffer_;

  std::shared_ptr<PartitionWriter> partitionWriter_;

  std::shared_ptr<Partitioner> partitioner_;
};

} // namespace gluten
