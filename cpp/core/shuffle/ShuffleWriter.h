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

#include <arrow/ipc/writer.h>
#include <numeric>
#include <utility>

#include "memory/ArrowMemoryPool.h"
#include "memory/ColumnarBatch.h"
#include "utils/compression.h"

namespace gluten {

namespace {
static constexpr int32_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultBufferCompressThreshold = 1024;
static constexpr int32_t kDefaultBufferAlignment = 64;
} // namespace

struct ShuffleWriterOptions {
  int64_t offheap_per_task = 0;
  int32_t buffer_size = kDefaultShuffleWriterBufferSize;
  int32_t push_buffer_max_size = kDefaultShuffleWriterBufferSize;
  int32_t num_sub_dirs = kDefaultNumSubDirs;
  int32_t buffer_compress_threshold = kDefaultBufferCompressThreshold;
  arrow::Compression::type compression_type = arrow::Compression::LZ4_FRAME;
  CodecBackend codec_backend = CodecBackend::NONE;
  CompressionMode compression_mode = CompressionMode::BUFFER;
  bool prefer_evict = false;
  bool buffered_write = false;
  bool write_eos = true;

  std::string data_file;
  std::string partition_writer_type = "local";

  int64_t thread_id = -1;
  int64_t task_attempt_id = -1;

  std::shared_ptr<arrow::MemoryPool> memory_pool;

  // For tests.
  std::shared_ptr<arrow::MemoryPool> ipc_memory_pool;

  arrow::ipc::IpcWriteOptions ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();

  std::string partitioning_name;

  static ShuffleWriterOptions defaults();
};

class ShuffleBufferPool {
 public:
  explicit ShuffleBufferPool(std::shared_ptr<arrow::MemoryPool> pool);

  arrow::Status init();

  arrow::Status allocate(std::shared_ptr<arrow::Buffer>& buffer, int64_t size);

  arrow::Status allocateDirectly(std::shared_ptr<arrow::ResizableBuffer>& buffer, int64_t size);

  int64_t bytesAllocated() const;

  void reset() {
    if (combineBuffer_ != nullptr) {
      combineBuffer_.reset();
    }
  }

 private:
  class MemoryPoolWrapper;

  std::shared_ptr<MemoryPoolWrapper> pool_;
  // slice the buffer for each reducer's column, in this way we can combine into
  // large page
  std::shared_ptr<arrow::ResizableBuffer> combineBuffer_;
};

class ShuffleWriter {
 public:
  /**
   * Evict fixed size of partition data from memory
   */
  virtual arrow::Status evictFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb) = 0;

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

  virtual std::shared_ptr<arrow::Schema> writeSchema();

  virtual std::shared_ptr<arrow::Schema> compressWriteSchema();

  virtual std::shared_ptr<arrow::Schema>& schema() {
    return schema_;
  }

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t totalBytesWritten() const {
    return totalBytesWritten_;
  }

  int64_t totalBytesEvicted() const {
    return totalBytesEvicted_;
  }

  int64_t splitBufferSize() const {
    return splitBufferSize_;
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

  std::shared_ptr<ShuffleBufferPool>& pool() {
    return pool_;
  }

  ShuffleWriterOptions& options() {
    return options_;
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

  void setSplitBufferSize(int64_t splitBufferSize) {
    splitBufferSize_ = splitBufferSize;
  }

  class PartitionWriter;

  class Partitioner;

  class PartitionWriterCreator;

 protected:
  ShuffleWriter(
      int32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options)
      : numPartitions_(numPartitions),
        partitionWriterCreator_(std::move(partitionWriterCreator)),
        options_(std::move(options)),
        codec_(createArrowIpcCodec(options_.compression_type, options_.codec_backend)),
        pool_(std::make_shared<ShuffleBufferPool>(options_.memory_pool)) {}
  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  std::shared_ptr<PartitionWriterCreator> partitionWriterCreator_;
  // options
  ShuffleWriterOptions options_;

  int64_t totalBytesWritten_ = 0;
  int64_t totalBytesEvicted_ = 0;
  int64_t splitBufferSize_ = 0;
  int64_t totalWriteTime_ = 0;
  int64_t totalEvictTime_ = 0;
  int64_t totalCompressTime_ = 0;
  int64_t peakMemoryAllocated_ = 0;

  std::vector<int64_t> partitionLengths_;
  std::vector<int64_t> rawPartitionLengths_;

  std::unique_ptr<arrow::util::Codec> codec_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::Schema> writeSchema_;
  std::shared_ptr<arrow::Schema> compressWriteSchema_;

  std::vector<int64_t> partitionCachedRecordbatchSize_; // in bytes
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>> partitionCachedRecordbatch_;

  // col partid
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>> partitionBuffers_;

  std::shared_ptr<PartitionWriter> partitionWriter_;

  std::shared_ptr<Partitioner> partitioner_;

  std::shared_ptr<ShuffleBufferPool> pool_;
};

} // namespace gluten
