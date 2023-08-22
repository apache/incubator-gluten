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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "WholeStageResultIterator.h"
#include "compute/Backend.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "operators/writer/VeloxParquetDatasource.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/reader.h"

namespace gluten {

// This class is used to convert the Substrait plan into Velox plan.
class VeloxBackend final : public Backend {
 public:
  explicit VeloxBackend(const std::unordered_map<std::string, std::string>& confMap);

  inline std::shared_ptr<velox::memory::MemoryPool> getAggregateVeloxPool(MemoryManager* memoryManager) {
    if (auto veloxMemoryManager = dynamic_cast<VeloxMemoryManager*>(memoryManager)) {
      return veloxMemoryManager->getAggregateMemoryPool();
    } else {
      GLUTEN_CHECK(false, "Should use VeloxMemoryManager here.");
    }
  }

  inline std::shared_ptr<velox::memory::MemoryPool> getLeafVeloxPool(MemoryManager* memoryManager) {
    if (auto veloxMemoryManager = dynamic_cast<VeloxMemoryManager*>(memoryManager)) {
      return veloxMemoryManager->getLeafMemoryPool();
    } else {
      GLUTEN_CHECK(false, "Should use VeloxMemoryManager here.");
    }
  }

  MemoryManager* getMemoryManager(
      std::string name,
      std::shared_ptr<MemoryAllocator> allocator,
      std::shared_ptr<AllocationListener> listener) override {
    auto veloxMemoryManager = new VeloxMemoryManager(name, allocator, listener);
    return veloxMemoryManager;
  }

  // FIXME This is not thread-safe?
  std::shared_ptr<ResultIterator> getResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs = {},
      const std::unordered_map<std::string, std::string>& sessionConf = {}) override;

  std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(MemoryManager* memoryManager) override;

  std::shared_ptr<RowToColumnarConverter> getRowToColumnarConverter(
      MemoryManager* memoryManager,
      struct ArrowSchema* cSchema) override;

  std::shared_ptr<ShuffleWriter> makeShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options) override;

  std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  std::shared_ptr<Datasource> getDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override {
    auto veloxPool = getAggregateVeloxPool(memoryManager);
    auto ctxVeloxPool = veloxPool->addAggregateChild("velox_parquet_writer");
    return std::make_shared<VeloxParquetDatasource>(filePath, ctxVeloxPool, schema);
  }

  std::shared_ptr<Reader> getShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool,
      MemoryManager* memoryManager) override {
    auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
    return std::make_shared<VeloxShuffleReader>(schema, options, pool, ctxVeloxPool);
  }

  std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) override;

  std::shared_ptr<const facebook::velox::core::PlanNode> getVeloxPlan() {
    return veloxPlan_;
  }

  static void getInfoAndIds(
      const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
      const std::unordered_set<facebook::velox::core::PlanNodeId>& leafPlanNodeIds,
      std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      std::vector<facebook::velox::core::PlanNodeId>& scanIds,
      std::vector<facebook::velox::core::PlanNodeId>& streamIds);

 private:
  std::vector<std::shared_ptr<ResultIterator>> inputIters_;
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;
};

} // namespace gluten
