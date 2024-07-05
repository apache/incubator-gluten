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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

#include <arrow/array/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include "VeloxShuffleWriter.h"
#include "memory/BufferOutputStream.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"

namespace gluten {

enum SortState { kSortInit, kSort, kSortStop };

class VeloxSortBasedShuffleWriter final : public VeloxShuffleWriter {
 public:
  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* arrowPool);

  arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status evictRowVector(uint32_t partitionId) override;

 private:
  VeloxSortBasedShuffleWriter(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* pool)
      : VeloxShuffleWriter(numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), pool) {}

  arrow::Status init();

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  void setSortState(SortState state);

  arrow::Status doSort(facebook::velox::RowVectorPtr rv, int64_t /* memLimit */);

  arrow::Status evictBatch(uint32_t partitionId);

  void stat() const;

  facebook::velox::RowTypePtr rowType_;

  std::unique_ptr<facebook::velox::VectorStreamGroup> batch_;
  std::unique_ptr<BufferOutputStream> bufferOutputStream_;

  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<facebook::velox::serializer::presto::PrestoVectorSerde>();

  std::vector<facebook::velox::RowVectorPtr> batches_;

  std::unordered_map<int32_t, std::vector<int64_t>> rowVectorIndexMap_;

  uint32_t currentInputColumnBytes_ = 0;

  SortState sortState_{kSortInit};
}; // class VeloxSortBasedShuffleWriter

} // namespace gluten
