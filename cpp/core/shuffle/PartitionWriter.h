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

#include "shuffle/Options.h"
#include "shuffle/ShuffleWriter.h"

namespace gluten {

class Evictor {
 public:
  enum Type { kCache, kFlush, kStop };

  Evictor(ShuffleWriterOptions* options) : options_(options) {}

  virtual ~Evictor() = default;

  virtual arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) = 0;

  virtual arrow::Status finish() = 0;

  int64_t getEvictTime() {
    return evictTime_;
  }

 protected:
  ShuffleWriterOptions* options_;

  int64_t evictTime_{0};
};

class ShuffleWriter::PartitionWriter : public Evictable {
 public:
  PartitionWriter(uint32_t numPartitions, ShuffleWriterOptions* options)
      : numPartitions_(numPartitions), options_(options) {}

  virtual ~PartitionWriter() = default;

  virtual arrow::Status init() = 0;

  virtual arrow::Status stop(ShuffleWriterMetrics* metrics) = 0;

  /// Evict buffers for `partitionId` partition.
  /// \param flush Whether to flush the evicted data immediately. If it's false,
  /// the data can be cached first.
  virtual arrow::Status evict(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      Evictor::Type evictType) = 0;

  virtual arrow::Status finishEvict() = 0;

 protected:
  arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> createPayloadFromBuffers(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers);

  uint32_t numPartitions_;
  ShuffleWriterOptions* options_;

  int64_t compressTime_{0};
  int64_t evictTime_{0};
  int64_t writeTime_{0};
};

} // namespace gluten
