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

class RssPartitionWriter final : public PartitionWriter {
 public:
  RssPartitionWriter(
      const uint32_t numPartitions,
      std::unique_ptr<arrow::util::Codec> codec,
      MemoryManager* memoryManager,
      const std::shared_ptr<RssPartitionWriterOptions>& options,
      const std::shared_ptr<RssClient>& rssClient)
      : PartitionWriter(numPartitions, std::move(codec), memoryManager), options_(options), rssClient_(rssClient) {
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

  std::shared_ptr<RssPartitionWriterOptions> options_;
  std::shared_ptr<RssClient> rssClient_;

  std::vector<int64_t> bytesEvicted_;
  std::vector<int64_t> rawPartitionLengths_;

  bool shouldInitializeOs_{true};
  std::shared_ptr<arrow::io::BufferOutputStream> rssOs_{nullptr};
  std::shared_ptr<ShuffleCompressedOutputStream> compressedOs_{nullptr};
};

} // namespace gluten
