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

#include "shuffle/rss/RemotePartitionWriter.h"

#include "jni/JniCommon.h"
#include "shuffle/PartitionWriterCreator.h"
#include "utils/macros.h"

namespace gluten {

class CelebornPartitionWriter final : public RemotePartitionWriter {
 public:
  CelebornPartitionWriter(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      std::shared_ptr<RssClient> celebornClient)
      : RemotePartitionWriter(numPartitions, options) {
    celebornClient_ = celebornClient;
  }

  arrow::Status evict(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      Evictor::Type evictType /* unused */) override;

  arrow::Status finishEvict() override;

  arrow::Status init() override;

  arrow::Status stop(ShuffleWriterMetrics* metrics) override;

  arrow::Status evictFixedSize(int64_t size, int64_t* actual) override;

 private:
  std::shared_ptr<RssClient> celebornClient_;

  std::shared_ptr<Evictor> evictor_;

  std::vector<int64_t> bytesEvicted_;
  std::vector<int64_t> rawPartitionLengths_;
};

class CelebornPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  explicit CelebornPartitionWriterCreator(std::shared_ptr<RssClient> client);

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(
      uint32_t numPartitions,
      ShuffleWriterOptions* options) override;

 private:
  std::shared_ptr<RssClient> client_;
};

} // namespace gluten
