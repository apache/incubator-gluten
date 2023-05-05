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

#include "ArrowTypeUtils.h"
#include "VeloxBridge.h"
#include "compute/Backend.h"
#include "compute/ResultIterator.h"
#include "compute/RowVectorStream.h"
#include "compute/VeloxPlanConverter.h"
#include "compute/VeloxRowToColumnarConverter.h"
#include "config/GlutenConfig.h"
#include "include/arrow/c/bridge.h"
#include "shuffle/ArrowShuffleWriter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "velox/common/file/FileSystems.h"

using namespace facebook;

namespace gluten {

namespace {
// Velox configs
const std::string kMemoryCapRatio = "spark.gluten.sql.columnar.backend.velox.memoryCapRatio";
} // namespace

VeloxBackend::VeloxBackend(const std::unordered_map<std::string, std::string>& confMap) : Backend(confMap) {
  // mem cap ratio
  float_t memCapRatio;
  {
    auto got = confMap_.find(kMemoryCapRatio);
    if (got == confMap_.end()) {
      // not found
      memCapRatio = 0.75;
    } else {
      memCapRatio = std::stof(got->second);
    }
  }

  // mem tracker
  int64_t maxMemory;
  {
    auto got = confMap_.find(kSparkTaskOffHeapMemory); // per task, for creating iterator
    if (got == confMap_.end()) {
      // not found
      maxMemory = facebook::velox::memory::kMaxMemory;
    } else {
      maxMemory = (long)(memCapRatio * (double)std::stol(got->second));
    }
  }

  memPoolOptions_ = {facebook::velox::memory::MemoryAllocator::kMaxAlignment, maxMemory};
}

void VeloxBackend::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<velox::substrait::SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<velox::substrait::SplitInfo>>& scanInfos,
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

std::shared_ptr<ResultIterator> VeloxBackend::GetResultIterator(
    MemoryAllocator* allocator,
    const std::string& spill_dir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  if (inputs.size() > 0) {
    inputIters_ = std::move(inputs);
  }

  auto veloxPool = AsWrappedVeloxAggregateMemoryPool(allocator, memPoolOptions_);
  auto ctxPool = veloxPool->addAggregateChild("result_iterator");
  // TODO: wait shuffle split velox to velox, then the input ColumnBatch is RowVector, no need pool to convert
  // https://github.com/oap-project/gluten/issues/1434
  auto resultPool = GetDefaultLeafWrappedVeloxMemoryPool();
  // auto resultPool = veloxPool->addLeafChild("input_row_vector_pool");
  auto veloxPlanConverter = std::make_unique<VeloxPlanConverter>(inputIters_, resultPool);
  veloxPlan_ = veloxPlanConverter->toVeloxPlan(substraitPlan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<velox::substrait::SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter->splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_unique<WholeStageResultIteratorMiddleStage>(
        ctxPool, resultPool, veloxPlan_, streamIds, spill_dir, sessionConf);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        ctxPool, resultPool, veloxPlan_, scanIds, scanInfos, streamIds, spill_dir, sessionConf);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  }
}

arrow::Result<std::shared_ptr<ColumnarToRowConverter>> VeloxBackend::getColumnar2RowConverter(
    MemoryAllocator* allocator,
    std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  if (veloxBatch != nullptr) {
    auto arrowPool = AsWrappedArrowMemoryPool(allocator);
    auto veloxPool = AsWrappedVeloxAggregateMemoryPool(allocator, memPoolOptions_);
    auto ctxVeloxPool = veloxPool->addLeafChild("columnar_to_row_velox");
    return std::make_shared<VeloxColumnarToRowConverter>(veloxBatch->getFlattenedRowVector(), arrowPool, ctxVeloxPool);
  } else {
    return Backend::getColumnar2RowConverter(allocator, cb);
  }
}

std::shared_ptr<RowToColumnarConverter> VeloxBackend::getRowToColumnarConverter(
    MemoryAllocator* allocator,
    struct ArrowSchema* cSchema) {
  // TODO: wait to fix task memory pool
  auto veloxPool = GetDefaultLeafWrappedVeloxMemoryPool();
  // AsWrappedVeloxAggregateMemoryPool(allocator)->addChild("row_to_columnar", velox::memory::MemoryPool::Kind::kLeaf);
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, veloxPool);
}

std::shared_ptr<ShuffleWriter>
VeloxBackend::makeShuffleWriter(int num_partitions, const SplitOptions& options, const std::string& batchType) {
  if (batchType == "velox") {
    GLUTEN_ASSIGN_OR_THROW(auto shuffle_writer, VeloxShuffleWriter::Create(num_partitions, std::move(options)));
    return shuffle_writer;
  } else {
    GLUTEN_ASSIGN_OR_THROW(auto shuffle_writer, ArrowShuffleWriter::Create(num_partitions, std::move(options)));
    return shuffle_writer;
  }
}

} // namespace gluten
