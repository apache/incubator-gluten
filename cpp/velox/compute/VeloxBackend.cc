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

#include "VeloxBackend.h"
#include <filesystem>

#include "arrow/c/bridge.h"
#include "compute/Backend.h"
#include "compute/ResultIterator.h"
#include "compute/VeloxPlanConverter.h"
#include "config/GlutenConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/TaskContext.h"
#include "velox/common/file/FileSystems.h"

using namespace facebook;

namespace gluten {

namespace {

void printSessionConf(const std::unordered_map<std::string, std::string>& conf) {
  std::ostringstream oss;
  oss << "session conf = {\n";
  for (auto& [k, v] : conf) {
    oss << " {" << k << " = " << v << "}\n";
  }
  oss << "}\n";
  LOG(INFO) << oss.str();
}

} // namespace

VeloxBackend::VeloxBackend(const std::unordered_map<std::string, std::string>& confMap) : Backend(confMap) {}

void VeloxBackend::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  if (splitInfoMap.size() == 0) {
    throw std::runtime_error("At least one data source info is required. Can be scan or stream info.");
  }
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

std::shared_ptr<ResultIterator> VeloxBackend::getResultIterator(
    MemoryAllocator* allocator,
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
#ifdef GLUTEN_PRINT_DEBUG
  printSessionConf(sessionConf);
#endif
  if (inputs.size() > 0) {
    inputIters_ = std::move(inputs);
  }

  auto veloxPool = asAggregateVeloxMemoryPool(allocator);
  auto ctxPool = veloxPool->addAggregateChild("result_iterator", facebook::velox::memory::MemoryReclaimer::create());
  auto veloxPlanConverter = std::make_unique<VeloxPlanConverter>(inputIters_, sessionConf);
  veloxPlan_ = veloxPlanConverter->toVeloxPlan(substraitPlan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter->splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_unique<WholeStageResultIteratorMiddleStage>(
        ctxPool, veloxPlan_, streamIds, spillDir, sessionConf, taskInfo_);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        ctxPool, veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  }
}

arrow::Result<std::shared_ptr<ColumnarToRowConverter>> VeloxBackend::getColumnar2RowConverter(
    MemoryAllocator* allocator) {
  auto arrowPool = asArrowMemoryPool(allocator);
  auto veloxPool = asAggregateVeloxMemoryPool(allocator);
  auto ctxVeloxPool = veloxPool->addLeafChild("columnar_to_row_velox");
  return std::make_shared<VeloxColumnarToRowConverter>(arrowPool, ctxVeloxPool);
}

std::shared_ptr<RowToColumnarConverter> VeloxBackend::getRowToColumnarConverter(
    MemoryAllocator* allocator,
    struct ArrowSchema* cSchema) {
  auto veloxAggregatePool = asAggregateVeloxMemoryPool(allocator);
  auto veloxPool = veloxAggregatePool->addLeafChild("row_to_columnar");
  gluten::bindToTask(veloxPool);
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, veloxPool);
}

std::shared_ptr<ShuffleWriter> VeloxBackend::makeShuffleWriter(
    int numPartitions,
    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
    const ShuffleWriterOptions& options) {
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffle_writer,
      VeloxShuffleWriter::create(numPartitions, std::move(partitionWriterCreator), std::move(options)));
  return shuffle_writer;
}

std::shared_ptr<ColumnarBatchSerializer> VeloxBackend::getColumnarBatchSerializer(
    MemoryAllocator* allocator,
    struct ArrowSchema* cSchema) {
  auto arrowPool = asArrowMemoryPool(allocator);
  auto veloxPool = asAggregateVeloxMemoryPool(allocator);
  auto ctxVeloxPool = veloxPool->addLeafChild("velox_columnar_batch_serializer");
  return std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
}

} // namespace gluten
