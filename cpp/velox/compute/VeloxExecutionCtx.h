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

#include "WholeStageResultIterator.h"
#include "compute/ExecutionCtx.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "operators/writer/VeloxParquetDatasource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "utils/ResourceMap.h"

namespace gluten {

class VeloxExecutionCtx final : public ExecutionCtx {
 public:
  explicit VeloxExecutionCtx(const std::unordered_map<std::string, std::string>& confMap);

  static std::shared_ptr<facebook::velox::memory::MemoryPool> getAggregateVeloxPool(MemoryManager* memoryManager) {
    if (auto veloxMemoryManager = dynamic_cast<VeloxMemoryManager*>(memoryManager)) {
      return veloxMemoryManager->getAggregateMemoryPool();
    } else {
      GLUTEN_CHECK(false, "Should use VeloxMemoryManager here.");
    }
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> getLeafVeloxPool(MemoryManager* memoryManager) {
    if (auto veloxMemoryManager = dynamic_cast<VeloxMemoryManager*>(memoryManager)) {
      return veloxMemoryManager->getLeafMemoryPool();
    } else {
      GLUTEN_CHECK(false, "Should use VeloxMemoryManager here.");
    }
  }

  MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator> allocator,
      std::unique_ptr<AllocationListener> listener) override {
    return new VeloxMemoryManager(name, allocator, std::move(listener));
  }

  // FIXME This is not thread-safe?
  ResourceHandle createResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs = {},
      const std::unordered_map<std::string, std::string>& sessionConf = {}) override;
  ResourceHandle addResultIterator(std::shared_ptr<ResultIterator> ptr) override;
  std::shared_ptr<ResultIterator> getResultIterator(ResourceHandle handle) override;
  void releaseResultIterator(ResourceHandle handle) override;

  ResourceHandle createColumnar2RowConverter(MemoryManager* memoryManager) override;
  std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(ResourceHandle handle) override;
  void releaseColumnar2RowConverter(ResourceHandle handle) override;

  ResourceHandle addBatch(std::shared_ptr<ColumnarBatch> ptr) override;
  std::shared_ptr<ColumnarBatch> getBatch(ResourceHandle handle) override;
  void releaseBatch(ResourceHandle handle) override;
  ResourceHandle select(MemoryManager* memoryManager, ResourceHandle batch, std::vector<int32_t> columnIndices)
      override;

  ResourceHandle createRow2ColumnarConverter(MemoryManager* memoryManager, struct ArrowSchema* cSchema) override;
  std::shared_ptr<RowToColumnarConverter> getRow2ColumnarConverter(ResourceHandle handle) override;
  void releaseRow2ColumnarConverter(ResourceHandle handle) override;

  ResourceHandle createShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      MemoryManager* memoryManager) override;
  std::shared_ptr<ShuffleWriter> getShuffleWriter(ResourceHandle handle) override;
  void releaseShuffleWriter(ResourceHandle handle) override;

  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  ResourceHandle createDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override;
  std::shared_ptr<Datasource> getDatasource(ResourceHandle handle) override;
  void releaseDatasource(ResourceHandle handle) override;

  ResourceHandle createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      arrow::MemoryPool* pool,
      MemoryManager* memoryManager) override;
  std::shared_ptr<ShuffleReader> getShuffleReader(ResourceHandle handle) override;
  void releaseShuffleReader(ResourceHandle handle) override;

  std::unique_ptr<ColumnarBatchSerializer> createTempColumnarBatchSerializer(
      MemoryManager* memoryManager,
      arrow::MemoryPool* arrowPool,
      struct ArrowSchema* cSchema) override;
  ResourceHandle createColumnarBatchSerializer(
      MemoryManager* memoryManager,
      arrow::MemoryPool* arrowPool,
      struct ArrowSchema* cSchema) override;
  std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(ResourceHandle handle) override;
  void releaseColumnarBatchSerializer(ResourceHandle handle) override;

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
  ResourceMap<std::shared_ptr<ColumnarBatch>> columnarBatchHolder_;
  ResourceMap<std::shared_ptr<Datasource>> datasourceHolder_;
  ResourceMap<std::shared_ptr<ColumnarToRowConverter>> columnarToRowConverterHolder_;
  ResourceMap<std::shared_ptr<ShuffleReader>> shuffleReaderHolder_;
  ResourceMap<std::shared_ptr<ShuffleWriter>> shuffleWriterHolder_;
  ResourceMap<std::shared_ptr<ColumnarBatchSerializer>> columnarBatchSerializerHolder_;
  ResourceMap<std::shared_ptr<RowToColumnarConverter>> rowToColumnarConverterHolder_;
  ResourceMap<std::shared_ptr<ResultIterator>> resultIteratorHolder_;

  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;
};

} // namespace gluten
