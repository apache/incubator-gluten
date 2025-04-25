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

#include <arrow/io/api.h>
#include <arrow/memory_pool.h>

#include "shuffle/PartitionWriter.h"
#include "shuffle/rss/RssClient.h"
#include "utils/Macros.h"

namespace gluten {

class RssPartitionWriterOutputStream;

class RssPartitionWriter final : public PartitionWriter {
 public:
  RssPartitionWriter(
      uint32_t numPartitions,
      PartitionWriterOptions options,
      MemoryManager* memoryManager,
      std::shared_ptr<RssClient> rssClient)
      : PartitionWriter(numPartitions, std::move(options), memoryManager), rssClient_(rssClient) {
    init();
  }

  arrow::Status hashEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers) override;

  arrow::Status sortEvict(uint32_t partitionId, std::unique_ptr<InMemoryPayload> inMemoryPayload, bool isFinal)
      override;

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool stop) override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status stop(ShuffleWriterMetrics* metrics) override;

 private:
  void init();

  arrow::Status doEvict(uint32_t partitionId, std::unique_ptr<InMemoryPayload> inMemoryPayload);

  std::shared_ptr<RssClient> rssClient_;

  std::vector<int64_t> bytesEvicted_;
  std::vector<int64_t> rawPartitionLengths_;

  int32_t lastEvictedPartitionId_{-1};
  std::shared_ptr<RssPartitionWriterOutputStream> rssOs_;
  std::shared_ptr<ShuffleCompressedOutputStream> compressedOs_;
};

class RssPartitionWriterOutputStream final : public arrow::io::OutputStream {
 public:
  RssPartitionWriterOutputStream(int32_t partitionId, RssClient* rssClient, int64_t pushBufferSize)
      : partitionId_(partitionId), rssClient_(rssClient), bufferSize_(pushBufferSize) {}

  arrow::Status init() {
    ARROW_ASSIGN_OR_RAISE(pushBuffer_, arrow::AllocateBuffer(bufferSize_, arrow::default_memory_pool()));
    pushBufferPtr_ = pushBuffer_->mutable_data();
    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    RETURN_NOT_OK(Flush());
    pushBuffer_.reset();
    return arrow::Status::OK();
  }

  bool closed() const override {
    return pushBuffer_ == nullptr;
  }

  arrow::Result<int64_t> Tell() const override {
    return bytesEvicted_ + bufferPos_;
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    auto dataPtr = static_cast<const char*>(data);
    if (nbytes < 0) {
      return arrow::Status::Invalid("write count should be >= 0");
    }
    if (nbytes == 0) {
      return arrow::Status::OK();
    }

    if (nbytes + bufferPos_ <= bufferSize_) {
      std::memcpy(pushBufferPtr_ + bufferPos_, dataPtr, nbytes);
      bufferPos_ += nbytes;
      return arrow::Status::OK();
    }

    int64_t bytesWritten = 0;
    while (bytesWritten < nbytes) {
      auto remaining = nbytes - bytesWritten;
      if (remaining <= bufferSize_ - bufferPos_) {
        std::memcpy(pushBufferPtr_ + bufferPos_, dataPtr + bytesWritten, remaining);
        bufferPos_ += remaining;
        return arrow::Status::OK();
      }
      auto toWrite = bufferSize_ - bufferPos_;
      std::memcpy(pushBufferPtr_ + bufferPos_, dataPtr + bytesWritten, toWrite);
      bytesWritten += toWrite;
      bufferPos_ += toWrite;
      RETURN_NOT_OK(Flush());
    }
    return arrow::Status::OK();
  }

  arrow::Status Flush() override {
    if (bufferPos_ > 0) {
      bytesEvicted_ += rssClient_->pushPartitionData(partitionId_, reinterpret_cast<char*>(pushBufferPtr_), bufferPos_);
      bufferPos_ = 0;
    }
    return arrow::Status::OK();
  }

 private:
  int32_t partitionId_;
  RssClient* rssClient_;
  int64_t bufferSize_{kDefaultPushMemoryThreshold};

  std::shared_ptr<arrow::Buffer> pushBuffer_;
  uint8_t* pushBufferPtr_{nullptr};
  int64_t bufferPos_{0};
  int64_t bytesEvicted_{0};
};
} // namespace gluten
