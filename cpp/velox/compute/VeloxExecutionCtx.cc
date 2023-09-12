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
#include "velox/common/file/FileSystems.h"

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

std::shared_ptr<ResultIterator> VeloxExecutionCtx::getResultIterator(
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
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        veloxPool, veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  }
}

std::shared_ptr<ColumnarToRowConverter> VeloxExecutionCtx::getColumnar2RowConverter(MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxColumnarToRowConverter>(ctxVeloxPool);
}

std::shared_ptr<RowToColumnarConverter> VeloxExecutionCtx::getRowToColumnarConverter(
    MemoryManager* memoryManager,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, ctxVeloxPool);
}

std::shared_ptr<ShuffleWriter> VeloxExecutionCtx::createShuffleWriter(
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

std::shared_ptr<ColumnarBatchSerializer> VeloxExecutionCtx::getColumnarBatchSerializer(
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::MemoryPool> arrowPool,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
}

} // namespace gluten
