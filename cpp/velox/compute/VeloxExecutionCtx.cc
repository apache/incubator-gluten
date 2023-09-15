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

#include "VeloxExecutionCtx.h"
#include <filesystem>

#include "arrow/c/bridge.h"
#include "compute/ExecutionCtx.h"
#include "compute/ResultIterator.h"
#include "compute/VeloxPlanConverter.h"
#include "config/GlutenConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleWriter.h"

using namespace facebook;

namespace gluten {

namespace {

#ifdef GLUTEN_PRINT_DEBUG
void printSessionConf(const std::unordered_map<std::string, std::string>& conf) {
  std::ostringstream oss;
  oss << "session conf = {\n";
  for (auto& [k, v] : conf) {
    oss << " {" << k << " = " << v << "}\n";
  }
  oss << "}\n";
  LOG(INFO) << oss.str();
}
#endif

} // namespace

VeloxExecutionCtx::VeloxExecutionCtx(const std::unordered_map<std::string, std::string>& confMap)
    : ExecutionCtx(confMap) {}

void VeloxExecutionCtx::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    if (splitInfo->isStream) {
      streamIds.emplace_back(leafPlanNodeId);
    } else {
      scanInfos.emplace_back(splitInfo);
      scanIds.emplace_back(leafPlanNodeId);
    }
  }
}

ResourceHandle VeloxExecutionCtx::createResultIterator(
    MemoryManager* memoryManager,
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
#ifdef GLUTEN_PRINT_DEBUG
  printSessionConf(sessionConf);
#endif
  if (inputs.size() > 0) {
    inputIters_ = std::move(inputs);
  }

  auto veloxPool = getAggregateVeloxPool(memoryManager);

  VeloxPlanConverter veloxPlanConverter(inputIters_, getLeafVeloxPool(memoryManager).get(), sessionConf);
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_unique<WholeStageResultIteratorMiddleStage>(
        veloxPool, veloxPlan_, streamIds, spillDir, sessionConf, taskInfo_);
    auto resultIter = std::make_shared<ResultIterator>(std::move(wholestageIter), this);
    return resultIteratorHolder_.insert(resultIter);
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        veloxPool, veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
    auto resultIter = std::make_shared<ResultIterator>(std::move(wholestageIter), this);
    return resultIteratorHolder_.insert(resultIter);
  }
}

ResourceHandle VeloxExecutionCtx::addResultIterator(std::shared_ptr<ResultIterator> iterator) {
  return resultIteratorHolder_.insert(iterator);
}

std::shared_ptr<ResultIterator> VeloxExecutionCtx::getResultIterator(ResourceHandle iterHandle) {
  auto instance = resultIteratorHolder_.lookup(iterHandle);
  if (!instance) {
    std::string errorMessage = "invalid handle for ResultIterator " + std::to_string(iterHandle);
    throw gluten::GlutenException(errorMessage);
  }
  return instance;
}

void VeloxExecutionCtx::releaseResultIterator(ResourceHandle iterHandle) {
  resultIteratorHolder_.erase(iterHandle);
}

ResourceHandle VeloxExecutionCtx::createColumnar2RowConverter(MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto converter = std::make_shared<VeloxColumnarToRowConverter>(ctxVeloxPool);
  return columnarToRowConverterHolder_.insert(converter);
}

std::shared_ptr<ColumnarToRowConverter> VeloxExecutionCtx::getColumnar2RowConverter(ResourceHandle handle) {
  return columnarToRowConverterHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseColumnar2RowConverter(ResourceHandle handle) {
  columnarToRowConverterHolder_.erase(handle);
}

ResourceHandle VeloxExecutionCtx::addBatch(std::shared_ptr<ColumnarBatch> batch) {
  return columnarBatchHolder_.insert(batch);
}

std::shared_ptr<ColumnarBatch> VeloxExecutionCtx::getBatch(ResourceHandle handle) {
  return columnarBatchHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseBatch(ResourceHandle handle) {
  columnarBatchHolder_.erase(handle);
}

ResourceHandle VeloxExecutionCtx::createRow2ColumnarConverter(
    MemoryManager* memoryManager,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto converter = std::make_shared<VeloxRowToColumnarConverter>(cSchema, ctxVeloxPool);
  return rowToColumnarConverterHolder_.insert(converter);
}

std::shared_ptr<RowToColumnarConverter> VeloxExecutionCtx::getRow2ColumnarConverter(ResourceHandle handle) {
  return rowToColumnarConverterHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseRow2ColumnarConverter(ResourceHandle handle) {
  rowToColumnarConverterHolder_.erase(handle);
}

ResourceHandle VeloxExecutionCtx::createShuffleWriter(
    int numPartitions,
    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
    const ShuffleWriterOptions& options,
    MemoryManager* memoryManager) {
  auto ctxPool = getLeafVeloxPool(memoryManager);
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffle_writer,
      VeloxShuffleWriter::create(numPartitions, std::move(partitionWriterCreator), std::move(options), ctxPool));
  return shuffleWriterHolder_.insert(shuffle_writer);
}

std::shared_ptr<ShuffleWriter> VeloxExecutionCtx::getShuffleWriter(ResourceHandle handle) {
  return shuffleWriterHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseShuffleWriter(ResourceHandle handle) {
  shuffleWriterHolder_.erase(handle);
}

ResourceHandle VeloxExecutionCtx::createDatasource(
    const std::string& filePath,
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::Schema> schema) {
  auto veloxPool = getAggregateVeloxPool(memoryManager);
  auto datasource = std::make_shared<VeloxParquetDatasource>(filePath, veloxPool, schema);
  return datasourceHolder_.insert(datasource);
}

std::shared_ptr<Datasource> VeloxExecutionCtx::getDatasource(ResourceHandle handle) {
  return datasourceHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseDatasource(ResourceHandle handle) {
  datasourceHolder_.erase(handle);
}

ResourceHandle VeloxExecutionCtx::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ReaderOptions options,
    std::shared_ptr<arrow::MemoryPool> pool,
    MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto shuffleReader = std::make_shared<VeloxShuffleReader>(schema, options, pool, ctxVeloxPool);
  return shuffleReaderHolder_.insert(shuffleReader);
}

std::shared_ptr<ShuffleReader> VeloxExecutionCtx::getShuffleReader(ResourceHandle handle) {
  return shuffleReaderHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseShuffleReader(ResourceHandle handle) {
  shuffleReaderHolder_.erase(handle);
}

std::unique_ptr<ColumnarBatchSerializer> VeloxExecutionCtx::createTempColumnarBatchSerializer(
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::MemoryPool> arrowPool,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
}

ResourceHandle VeloxExecutionCtx::createColumnarBatchSerializer(
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::MemoryPool> arrowPool,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
  return columnarBatchSerializerHolder_.insert(serializer);
}

std::shared_ptr<ColumnarBatchSerializer> VeloxExecutionCtx::getColumnarBatchSerializer(ResourceHandle handle) {
  return columnarBatchSerializerHolder_.lookup(handle);
}

void VeloxExecutionCtx::releaseColumnarBatchSerializer(ResourceHandle handle) {
  columnarBatchSerializerHolder_.erase(handle);
}

} // namespace gluten
