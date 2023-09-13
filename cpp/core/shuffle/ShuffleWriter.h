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
static constexpr double kDefaultBufferReallocThreshold = 0.25;
} // namespace

struct ShuffleWriterOptions {
  int32_t buffer_size = kDefaultShuffleWriterBufferSize;
  int32_t push_buffer_max_size = kDefaultShuffleWriterBufferSize;
  int32_t num_sub_dirs = kDefaultNumSubDirs;
  int32_t buffer_compress_threshold = kDefaultBufferCompressThreshold;
  double buffer_realloc_threshold = kDefaultBufferReallocThreshold;
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

  arrow::ipc::IpcWriteOptions ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();

  std::string partitioning_name;

  static ShuffleWriterOptions defaults();
};

class ShuffleMemoryPool : public arrow::MemoryPool {
 public:
  ShuffleMemoryPool(std::shared_ptr<arrow::MemoryPool> pool) : pool_(pool) {}

  arrow::MemoryPool* delegated() {
    return pool_.get();
  }

  arrow::Status Allocate(int64_t size, uint8_t** out) override {
    auto status = pool_->Allocate(size, out);
    if (status.ok()) {
      bytesAllocated_ += size;
    }
    return status;
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    auto status = pool_->Reallocate(old_size, new_size, ptr);
    if (status.ok()) {
      bytesAllocated_ += (new_size - old_size);
    }
    return status;
  }

  void Free(uint8_t* buffer, int64_t size) override {
    pool_->Free(buffer, size);
    bytesAllocated_ -= size;
  }

  int64_t bytes_allocated() const override {
    return bytesAllocated_;
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return pool_->backend_name();
  }

 private:
  std::shared_ptr<arrow::MemoryPool> pool_;
  uint64_t bytesAllocated_ = 0;
};

class ShuffleWriter {
 public:
  static constexpr int64_t kMinMemLimit = 128LL * 1024 * 1024;
  /**
   * Evict fixed size of partition data from memory
   */
  virtual arrow::Status evictFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) = 0;

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

  virtual arrow::Status cacheRecordBatch(uint32_t partitionId, const arrow::RecordBatch& rb, bool reuseBuffers) = 0;

  virtual arrow::Status stop() = 0;

  virtual std::shared_ptr<arrow::Schema> writeSchema();

  virtual std::shared_ptr<arrow::Schema> compressWriteSchema();

  virtual std::shared_ptr<arrow::Schema>& schema() {
    return schema_;
  }

  void clearCachedPayloads(uint32_t partitionId);

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t totalBytesWritten() const {
    return totalBytesWritten_;
  }

  int64_t totalBytesEvicted() const {
    return totalBytesEvicted_;
  }

  int64_t partitionBufferSize() const {
    return partitionBufferPool_->bytes_allocated();
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

  virtual const uint64_t cachedPayloadSize() const {
    return std::accumulate(partitionCachedRecordbatchSize_.begin(), partitionCachedRecordbatchSize_.end(), 0);
  }

  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>>& partitionCachedRecordbatch() {
    return partitionCachedRecordbatch_;
  }

  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>>& partitionBuffer() {
    return partitionBuffers_;
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
        partitionBufferPool_(std::make_shared<ShuffleMemoryPool>(options_.memory_pool)),
        codec_(createArrowIpcCodec(options_.compression_type, options_.codec_backend)) {}

  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  std::shared_ptr<PartitionWriterCreator> partitionWriterCreator_;

  ShuffleWriterOptions options_;
  // Memory Pool used to track memory usage of partition buffers.
  // The actual allocation is delegated to options_.memory_pool.
  std::shared_ptr<ShuffleMemoryPool> partitionBufferPool_;

  int64_t totalBytesWritten_ = 0;
  int64_t totalBytesEvicted_ = 0;
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
};

} // namespace gluten
