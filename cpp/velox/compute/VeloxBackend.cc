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
#include "config/GlutenConfig.h"
#include "include/arrow/c/bridge.h"
#include "operators/shuffle/CelebornSplitter.h"
#include "operators/shuffle/splitter.h"
#include "shuffle/VeloxSplitter.h"
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
    auto got = confMap_.find(kSparkOffHeapMemory);
    if (got == confMap_.end()) {
      // not found
      maxMemory = facebook::velox::memory::kMaxMemory;
    } else {
      maxMemory = (long)(memCapRatio * (double)std::stol(got->second));
    }
  }

  try {
    // 1/2 of offheap size.
    memUsageTracker_ = velox::memory::MemoryUsageTracker::create(maxMemory);
  } catch (const std::invalid_argument&) {
    throw std::runtime_error("Invalid off-heap memory size: " + std::to_string(maxMemory));
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::FetchRel& fetchRel) {
  if (fetchRel.has_input()) {
    setInputPlanNode(fetchRel.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::ExpandRel& sexpand) {
  if (sexpand.has_input()) {
    setInputPlanNode(sexpand.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::SortRel& ssort) {
  if (ssort.has_input()) {
    setInputPlanNode(ssort.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::WindowRel& swindow) {
  if (swindow.has_input()) {
    setInputPlanNode(swindow.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    setInputPlanNode(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::JoinRel& sjoin) {
  if (sjoin.has_left()) {
    setInputPlanNode(sjoin.left());
  } else {
    throw std::runtime_error("Left child expected");
  }

  if (sjoin.has_right()) {
    setInputPlanNode(sjoin.right());
  } else {
    throw std::runtime_error("Right child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::WriteRel& swrite) {
  if (swrite.has_input()) {
    setInputPlanNode(swrite.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->streamIsInput(sread);
  if (iterIdx == -1) {
    return;
  }
  if (arrowInputIters_.size() == 0) {
    throw std::runtime_error("Invalid input iterator.");
  }

  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<std::shared_ptr<velox::substrait::SubstraitParser::SubstraitType>> subTypeList;
  if (sread.has_base_schema()) {
    const auto& baseSchema = sread.base_schema();
    // Input names is not used. Instead, new input/output names will be created
    // because the Arrow Stream node in Velox does not support name change.
    colNum = baseSchema.names().size();
    subTypeList = subParser_->parseNamedStruct(baseSchema);
  }

  // Get the Arrow fields and output names for this plan node.
  std::vector<std::shared_ptr<arrow::Field>> arrowFields;
  arrowFields.reserve(colNum);
  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    auto colName = subParser_->makeNodeName(planNodeId_, idx);
    arrowFields.emplace_back(arrow::field(colName, toArrowTypeFromName(subTypeList[idx]->type)));
    outNames.emplace_back(colName);
  }

  // Create Arrow reader.
  std::shared_ptr<arrow::Schema> schema = arrow::schema(arrowFields);
  auto arrayIter = std::move(arrowInputIters_[iterIdx]);
  // Create ArrowArrayStream.
  struct ArrowArrayStream veloxArrayStream;
  GLUTEN_THROW_NOT_OK(ExportArrowArray(schema, arrayIter->ToArrowArrayIterator(), &veloxArrayStream));
  auto arrowStream = std::make_shared<ArrowArrayStream>(veloxArrayStream);

  // Create Velox ArrowStream node.
  std::vector<velox::TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(velox::substrait::toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto arrowStreamNode = std::make_shared<velox::core::ArrowStreamNode>(nextPlanNodeId(), outputType, arrowStream);
  subVeloxPlanConverter_->insertInputNode(iterIdx, arrowStreamNode, planNodeId_);
}

void VeloxBackend::setInputPlanNode(const ::substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    setInputPlanNode(srel.aggregate());
  } else if (srel.has_project()) {
    setInputPlanNode(srel.project());
  } else if (srel.has_filter()) {
    setInputPlanNode(srel.filter());
  } else if (srel.has_read()) {
    setInputPlanNode(srel.read());
  } else if (srel.has_join()) {
    setInputPlanNode(srel.join());
  } else if (srel.has_sort()) {
    setInputPlanNode(srel.sort());
  } else if (srel.has_expand()) {
    setInputPlanNode(srel.expand());
  } else if (srel.has_fetch()) {
    setInputPlanNode(srel.fetch());
  } else if (srel.has_window()) {
    setInputPlanNode(srel.window());
  } else if (srel.has_write()) {
    setInputPlanNode(srel.write());
  } else {
    throw std::runtime_error("Rel is not supported: " + srel.DebugString());
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::RelRoot& sroot) {
  // Output names can be got from RelRoot, but are not used currently.
  if (sroot.has_input()) {
    setInputPlanNode(sroot.input());
  } else {
    throw std::runtime_error("Input is expected in RelRoot.");
  }
}

void VeloxBackend::toVeloxPlan() {
  // In fact, only one RelRoot is expected here.
  for (auto& srel : substraitPlan_.relations()) {
    if (srel.has_root()) {
      setInputPlanNode(srel.root());
    }
    if (srel.has_rel()) {
      setInputPlanNode(srel.rel());
    }
  }
  veloxPlan_ = subVeloxPlanConverter_->toVeloxPlan(substraitPlan_);
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Plan Node: " << std::endl << veloxPlan_->toString(true, true) << std::endl;
#endif
}

std::string VeloxBackend::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

static void getInfoAndIds(
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
    arrowInputIters_ = std::move(inputs);
  }

  toVeloxPlan();

  // Scan node can be required.
  std::vector<std::shared_ptr<velox::substrait::SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto veloxPool = AsWrappedVeloxMemoryPool(allocator);
  auto ctxPool = veloxPool->addChild("result_iterator_spill", velox::memory::MemoryPool::Kind::kAggregate);
  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter =
        std::make_unique<WholeStageResultIteratorMiddleStage>(ctxPool, veloxPlan_, streamIds, spill_dir, sessionConf);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        ctxPool, veloxPlan_, scanIds, scanInfos, streamIds, spill_dir, sessionConf);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  }
}

std::shared_ptr<ResultIterator> VeloxBackend::GetResultIterator(
    MemoryAllocator* allocator,
    const std::vector<std::shared_ptr<velox::substrait::SplitInfo>>& setScanInfos) {
  toVeloxPlan();

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<velox::substrait::SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto veloxPool = AsWrappedVeloxMemoryPool(allocator);
  auto ctxPool = veloxPool->addChild("result_iterator", velox::memory::MemoryPool::Kind::kLeaf);
  auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
      ctxPool, veloxPlan_, scanIds, setScanInfos, streamIds, "/tmp/test-spill", confMap_);
  return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
}

arrow::Result<std::shared_ptr<ColumnarToRowConverter>> VeloxBackend::getColumnar2RowConverter(
    MemoryAllocator* allocator,
    std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  if (veloxBatch != nullptr) {
    auto arrowPool = AsWrappedArrowMemoryPool(allocator);
    auto veloxPool = AsWrappedVeloxMemoryPool(allocator);
    auto ctxVeloxPool = veloxPool->addChild("columnar_to_row_velox", velox::memory::MemoryPool::Kind::kLeaf);
    return std::make_shared<VeloxColumnarToRowConverter>(veloxBatch->getFlattenedRowVector(), arrowPool, ctxVeloxPool);
  } else {
    return Backend::getColumnar2RowConverter(allocator, cb);
  }
}

std::shared_ptr<SplitterBase> VeloxBackend::makeSplitter(
    const std::string& partitioning_name,
    int num_partitions,
    const SplitOptions& options,
    const std::string& batchType) {
  if (options.is_celeborn) {
    GLUTEN_ASSIGN_OR_THROW(
        auto splitter, CelebornSplitter::Make(partitioning_name, num_partitions, std::move(options)));
    return splitter;
  } else if (batchType == "velox") {
    GLUTEN_ASSIGN_OR_THROW(auto splitter, VeloxSplitter::Make(partitioning_name, num_partitions, std::move(options)));
    return splitter;
  } else {
    GLUTEN_ASSIGN_OR_THROW(auto splitter, Splitter::Make(partitioning_name, num_partitions, std::move(options)));
    return splitter;
  }
}

std::shared_ptr<arrow::Schema> VeloxBackend::GetOutputSchema() {
  if (outputSchema_ == nullptr) {
    cacheOutputSchema(veloxPlan_);
  }
  return outputSchema_;
}

std::shared_ptr<facebook::velox::memory::MemoryUsageTracker> VeloxBackend::getMemoryUsageTracker() {
  return memUsageTracker_;
}

void VeloxBackend::cacheOutputSchema(const std::shared_ptr<const velox::core::PlanNode>& planNode) {
  ArrowSchema arrowSchema{};
  exportToArrow(
      velox::BaseVector::create(planNode->outputType(), 0, GetDefaultWrappedVeloxMemoryPool().get()), arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(outputSchema_, arrow::ImportSchema(&arrowSchema));
}

} // namespace gluten
