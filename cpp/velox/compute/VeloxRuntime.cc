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

#include "VeloxRuntime.h"

#include <filesystem>

#include "VeloxBackend.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxPlanConverter.h"
#include "config/GlutenConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/ConfigExtractor.h"

using namespace facebook;

namespace gluten {

VeloxRuntime::VeloxRuntime(const std::unordered_map<std::string, std::string>& confMap) : Runtime(confMap) {}

void VeloxRuntime::parsePlan(const uint8_t* data, int32_t size, SparkTaskInfo taskInfo) {
  taskInfo_ = taskInfo;
  if (debugModeEnabled(confMap_)) {
    try {
      auto jsonPlan = substraitFromPbToJson("Plan", data, size);
      LOG(INFO) << std::string(50, '#') << " received substrait::Plan:";
      LOG(INFO) << taskInfo_ << std::endl << jsonPlan;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan to JSON: " << e.what();
    }
  }

  GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

void VeloxRuntime::getInfoAndIds(
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

std::string VeloxRuntime::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  VeloxPlanConverter veloxPlanConverter(inputs, veloxMemoryPool.get(), sessionConf, true);
  auto veloxPlan = veloxPlanConverter.toVeloxPlan(substraitPlan_);
  return veloxPlan->toString(details, true);
}

std::shared_ptr<ResultIterator> VeloxRuntime::createResultIterator(
    MemoryManager* memoryManager,
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  if (debugModeEnabled(confMap_)) {
    LOG(INFO) << "VeloxRuntime session config:" << printConfig(confMap_);
  }

  VeloxPlanConverter veloxPlanConverter(inputs, getLeafVeloxPool(memoryManager).get(), sessionConf);
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto* vmm = toVeloxMemoryManager(memoryManager);
  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_unique<WholeStageResultIteratorMiddleStage>(
        vmm, veloxPlan_, streamIds, spillDir, sessionConf, taskInfo_);
    auto resultIter = std::make_shared<ResultIterator>(std::move(wholestageIter), this);
    return resultIter;
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        vmm, veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
    auto resultIter = std::make_shared<ResultIterator>(std::move(wholestageIter), this);
    return resultIter;
  }
}

std::shared_ptr<ColumnarToRowConverter> VeloxRuntime::createColumnar2RowConverter(MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxColumnarToRowConverter>(ctxVeloxPool);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    const std::shared_ptr<ColumnarBatch>& batch = gluten::createZeroColumnBatch(numRows);
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::select(
    MemoryManager* memoryManager,
    std::shared_ptr<ColumnarBatch> batch,
    std::vector<int32_t> columnIndices) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto veloxBatch = gluten::VeloxColumnarBatch::from(ctxVeloxPool.get(), batch);
  auto outputBatch = veloxBatch->select(ctxVeloxPool.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> VeloxRuntime::createRow2ColumnarConverter(
    MemoryManager* memoryManager,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, ctxVeloxPool);
}

std::shared_ptr<ShuffleWriter> VeloxRuntime::createShuffleWriter(
    int numPartitions,
    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
    const ShuffleWriterOptions& options,
    MemoryManager* memoryManager) {
  auto ctxPool = getLeafVeloxPool(memoryManager);
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffle_writer,
      VeloxShuffleWriter::create(numPartitions, std::move(partitionWriterCreator), std::move(options), ctxPool));
  return shuffle_writer;
}

std::shared_ptr<Datasource> VeloxRuntime::createDatasource(
    const std::string& filePath,
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::Schema> schema) {
  auto veloxPool = getAggregateVeloxPool(memoryManager);
  // Pass a dedicate pool for S3 sink as can't share veloxPool
  // with parquet writer.
  auto s3SinkPool = getLeafVeloxPool(memoryManager);

  return std::make_shared<VeloxParquetDatasource>(filePath, veloxPool, s3SinkPool, schema);
}

std::shared_ptr<ShuffleReader> VeloxRuntime::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options,
    arrow::MemoryPool* pool,
    MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxShuffleReader>(schema, options, pool, ctxVeloxPool);
}

std::unique_ptr<ColumnarBatchSerializer> VeloxRuntime::createColumnarBatchSerializer(
    MemoryManager* memoryManager,
    arrow::MemoryPool* arrowPool,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
}

} // namespace gluten
