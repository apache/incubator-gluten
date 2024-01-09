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
#include "memory/Reclaimable.h"
#include "shuffle/Options.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/Partitioning.h"
#include "shuffle/ShuffleMemoryPool.h"
#include "utils/Compression.h"

namespace gluten {

class ShuffleWriter : public Reclaimable {
 public:
  static constexpr int64_t kMinMemLimit = 128LL * 1024 * 1024;

  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) = 0;

  virtual arrow::Status stop() = 0;

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t partitionBufferSize() const {
    return partitionBufferPool_->bytes_allocated();
  }

  int64_t maxPartitionBufferSize() const {
    return partitionBufferPool_->max_memory();
  }

  int64_t totalBytesWritten() const {
    return metrics_.totalBytesWritten;
  }

  int64_t totalBytesEvicted() const {
    return metrics_.totalBytesEvicted;
  }

  int64_t totalWriteTime() const {
    return metrics_.totalWriteTime;
  }

  int64_t totalEvictTime() const {
    return metrics_.totalEvictTime;
  }

  int64_t totalCompressTime() const {
    return metrics_.totalCompressTime;
  }

  const std::vector<int64_t>& partitionLengths() const {
    return metrics_.partitionLengths;
  }

  const std::vector<int64_t>& rawPartitionLengths() const {
    return metrics_.rawPartitionLengths;
  }

  virtual const uint64_t cachedPayloadSize() const = 0;

 protected:
  ShuffleWriter(
      int32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      arrow::MemoryPool* pool)
      : numPartitions_(numPartitions),
        options_(std::move(options)),
        pool_(pool),
        partitionBufferPool_(std::make_unique<ShuffleMemoryPool>(pool)),
        partitionWriter_(std::move(partitionWriter)) {}

  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  ShuffleWriterOptions options_;

  arrow::MemoryPool* pool_;
  // Memory Pool used to track memory usage of partition buffers.
  // The actual allocation is delegated to options_.memoryPool.
  std::unique_ptr<ShuffleMemoryPool> partitionBufferPool_;

  std::unique_ptr<PartitionWriter> partitionWriter_;

  std::shared_ptr<arrow::Schema> schema_;

  // Column index, partition id, buffers.
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>> partitionBuffers_;

  std::shared_ptr<Partitioner> partitioner_;

  ShuffleWriterMetrics metrics_{};
};

} // namespace gluten
