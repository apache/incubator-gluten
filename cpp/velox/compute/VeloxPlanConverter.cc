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

#include "VeloxPlanConverter.h"

#include <arrow/c/bridge.h>
#include <arrow/type_fwd.h>
#include <arrow/util/iterator.h>

#include <string>

#include "ArrowTypeUtils.h"
#include "arrow/c/Bridge.h"
#include "arrow/c/bridge.h"
#include "bridge.h"
#include "jni/exec_backend.h"
#include "velox/buffer/Buffer.h"
#include "velox/functions/prestosql/aggregates/AverageAggregate.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"
#include "velox/functions/prestosql/aggregates/MinMaxAggregates.h"
#include "velox/functions/sparksql/Register.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace velox {
namespace compute {

namespace {
const std::string kHiveConnectorId = "test-hive";
std::atomic<int32_t> taskSerial;
} // namespace
std::shared_ptr<core::QueryCtx> createNewVeloxQueryCtx() {
  return std::make_shared<core::QueryCtx>();
}

// The Init will be called per executor.
void VeloxInitializer::Init() {
  // Setup and register.
  filesystems::registerLocalFileSystem();
  std::unique_ptr<folly::IOThreadPoolExecutor> executor =
      std::make_unique<folly::IOThreadPoolExecutor>(1);
  // auto hiveConnectorFactory = std::make_shared<hive::HiveConnectorFactory>();
  // registerConnectorFactory(hiveConnectorFactory);
  auto hiveConnector =
      getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  registerConnector(hiveConnector);
  parquet::registerParquetReaderFactory(ParquetReaderType::DUCKDB);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  functions::prestosql::registerAllScalarFunctions();
  functions::sparksql::registerFunctions("");
  aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
  aggregate::registerAverageAggregate("avg");
  aggregate::registerCountAggregate("count");
  aggregate::registerMinMaxAggregate<
      aggregate::MinAggregate,
      aggregate::NonNumericMinAggregate>("min");
  aggregate::registerMinMaxAggregate<
      aggregate::MaxAggregate,
      aggregate::NonNumericMaxAggregate>("max");
}

void VeloxPlanConverter::setInputPlanNode(
    const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(
    const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(
    const ::substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    setInputPlanNode(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::JoinRel& sjoin) {
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

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->streamIsInput(sread);
  if (iterIdx == -1) {
    return;
  }
  if (arrowInputIters_.size() == 0) {
    throw std::runtime_error("Invalid input iterator.");
  }

  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<std::shared_ptr<
      facebook::velox::substrait::SubstraitParser::SubstraitType>>
      subTypeList;
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
    arrowFields.emplace_back(
        arrow::field(colName, toArrowTypeFromName(subTypeList[idx]->type)));
    outNames.emplace_back(colName);
  }

  // Create Arrow reader.
  std::shared_ptr<arrow::Schema> schema = arrow::schema(arrowFields);
  auto arrayIter = std::move(arrowInputIters_[iterIdx]);
  // Create ArrowArrayStream.
  struct ArrowArrayStream veloxArrayStream;
  GLUTEN_THROW_NOT_OK(ExportArrowArray(
      schema, arrayIter->ToArrowArrayIterator(), &veloxArrayStream));
  auto arrowStream = std::make_shared<ArrowArrayStream>(veloxArrayStream);

  // Create Velox ArrowStream node.
  std::vector<TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(
        facebook::velox::substrait::toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto arrowStreamNode = std::make_shared<core::ArrowStreamNode>(
      nextPlanNodeId(), outputType, arrowStream, pool_);
  subVeloxPlanConverter_->insertInputNode(
      iterIdx, arrowStreamNode, planNodeId_);
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::Rel& srel) {
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
  } else {
    throw std::runtime_error("Rel is not supported: " + srel.DebugString());
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::RelRoot& sroot) {
  // Output names can be got from RelRoot, but are not used currently.
  if (sroot.has_input()) {
    setInputPlanNode(sroot.input());
  } else {
    throw std::runtime_error("Input is expected in RelRoot.");
  }
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::getVeloxPlanNode(
    const ::substrait::Plan& splan) {
  // In fact, only one RelRoot is expected here.
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      setInputPlanNode(srel.root());
    }
    if (srel.has_rel()) {
      setInputPlanNode(srel.rel());
    }
  }
  auto planNode = subVeloxPlanConverter_->toVeloxPlan(splan);
#ifdef DEBUG
  std::cout << "Plan Node: " << std::endl
            << planNode->toString(true, true) << std::endl;
#endif
  return planNode;
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

void VeloxPlanConverter::getInfoAndIds(
    std::unordered_map<
        core::PlanNodeId,
        std::shared_ptr<facebook::velox::substrait::SplitInfo>> splitInfoMap,
    std::unordered_set<core::PlanNodeId> leafPlanNodeIds,
    std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
        scanInfos,
    std::vector<core::PlanNodeId>& scanIds,
    std::vector<core::PlanNodeId>& streamIds) {
  if (splitInfoMap.size() == 0) {
    throw std::runtime_error(
        "At least one data source info is required. Can be scan or stream info.");
  }
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    if (splitInfoMap.find(leafPlanNodeId) == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = splitInfoMap[leafPlanNodeId];
    if (splitInfo->isStream) {
      streamIds.emplace_back(leafPlanNodeId);
    } else {
      scanInfos.emplace_back(splitInfo);
      scanIds.emplace_back(leafPlanNodeId);
    }
  }
}

std::shared_ptr<gluten::ArrowArrayResultIterator>
VeloxPlanConverter::GetResultIterator() {
  std::vector<std::shared_ptr<gluten::ArrowArrayResultIterator>> inputs = {};
  return GetResultIterator(inputs);
}

std::shared_ptr<gluten::ArrowArrayResultIterator>
VeloxPlanConverter::GetResultIterator(
    std::vector<std::shared_ptr<gluten::ArrowArrayResultIterator>> inputs) {
  if (inputs.size() > 0) {
    arrowInputIters_ = std::move(inputs);
  }
  const std::shared_ptr<const core::PlanNode> planNode =
      getVeloxPlanNode(plan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(
      subVeloxPlanConverter_->splitInfos(),
      planNode->leafPlanNodeIds(),
      scanInfos,
      scanIds,
      streamIds);

  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_shared<WholeStageResIterMiddleStage>(
        pool_, planNode, streamIds);
    return std::make_shared<gluten::ArrowArrayResultIterator>(
        std::move(wholestageIter));
  }
  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      pool_, planNode, scanIds, scanInfos, streamIds);
  return std::make_shared<gluten::ArrowArrayResultIterator>(
      std::move(wholestageIter));
}

std::shared_ptr<gluten::ArrowArrayResultIterator>
VeloxPlanConverter::GetResultIterator(
    const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
        setScanInfos) {
  const std::shared_ptr<const core::PlanNode> planNode =
      getVeloxPlanNode(plan_);

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(
      subVeloxPlanConverter_->splitInfos(),
      planNode->leafPlanNodeIds(),
      scanInfos,
      scanIds,
      streamIds);

  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      pool_, planNode, scanIds, setScanInfos, streamIds);
  return std::make_shared<gluten::ArrowArrayResultIterator>(
      std::move(wholestageIter));
}

void WholeStageResIter::toArrowArray(const RowVectorPtr& rv, ArrowArray& out) {
  // Make sure to load lazy vector if not loaded already.
  for (auto& child : rv->children()) {
    child->loadedVector();
  }

  RowVectorPtr copy = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(rv->type(), rv->size(), pool_));
  copy->copy(rv.get(), 0, 0, rv->size());
  exportToArrow(copy, out, pool_);
}

arrow::Result<std::shared_ptr<ArrowArray>> WholeStageResIter::Next() {
  addSplits_(task_.get());
  if (task_->isFinished()) {
    return nullptr;
  }
  RowVectorPtr vector = task_->next();
  if (vector == nullptr) {
    return nullptr;
  }
  uint64_t numRows = vector->size();
  if (numRows == 0) {
    return nullptr;
  }

  ArrowArray out;
  toArrowArray(vector, out);
  return std::make_shared<ArrowArray>(out);
}

class VeloxPlanConverter::WholeStageResIterFirstStage
    : public WholeStageResIter {
 public:
  WholeStageResIterFirstStage(
      memory::MemoryPool* pool,
      const std::shared_ptr<const core::PlanNode>& planNode,
      const std::vector<core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
          scanInfos,
      const std::vector<core::PlanNodeId>& streamIds)
      : WholeStageResIter(pool, planNode),
        scanNodeIds_(scanNodeIds),
        scanInfos_(scanInfos),
        streamIds_(streamIds) {
    // Generate splits for all scan nodes.
    splits_.reserve(scanInfos.size());
    if (scanNodeIds.size() != scanInfos.size()) {
      throw std::runtime_error("Invalid scan information.");
    }
    for (const auto& scanInfo : scanInfos) {
      // Get the information for TableScan.
      // Partition index in scan info is not used.
      const auto& paths = scanInfo->paths;
      const auto& starts = scanInfo->starts;
      const auto& lengths = scanInfo->lengths;
      const auto& format = scanInfo->format;

      std::vector<std::shared_ptr<ConnectorSplit>> connectorSplits;
      connectorSplits.reserve(paths.size());
      for (int idx = 0; idx < paths.size(); idx++) {
        auto split = std::make_shared<hive::HiveConnectorSplit>(
            kHiveConnectorId, paths[idx], format, starts[idx], lengths[idx]);
        connectorSplits.emplace_back(split);
      }

      std::vector<exec::Split> scanSplits;
      scanSplits.reserve(connectorSplits.size());
      for (const auto& connectorSplit : connectorSplits) {
        // Bucketed group id (-1 means 'none').
        int32_t groupId = -1;
        scanSplits.emplace_back(
            exec::Split(folly::copy(connectorSplit), groupId));
      }
      splits_.emplace_back(scanSplits);
    }

    // Set task parameters.
    core::PlanFragment planFragment{
        planNode, core::ExecutionStrategy::kUngrouped, 1};
    std::shared_ptr<core::QueryCtx> queryCtx = createNewVeloxQueryCtx();
    task_ = std::make_shared<exec::Task>(
        fmt::format("gluten task {}", ++taskSerial),
        std::move(planFragment),
        0,
        std::move(queryCtx));
    if (!task_->supportsSingleThreadedExecution()) {
      throw std::runtime_error(
          "Task doesn't support single thread execution: " +
          planNode->toString());
    }
    addSplits_ = [&](Task* task) {
      if (noMoreSplits_) {
        return;
      }
      for (int idx = 0; idx < scanNodeIds_.size(); idx++) {
        for (auto& split : splits_[idx]) {
          task->addSplit(scanNodeIds_[idx], std::move(split));
        }
        task->noMoreSplits(scanNodeIds_[idx]);
      }
      for (const auto& streamId : streamIds_) {
        task->noMoreSplits(streamId);
      }
      noMoreSplits_ = true;
    };
  }

 private:
  std::vector<core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>
      scanInfos_;
  std::vector<core::PlanNodeId> streamIds_;
  std::vector<std::vector<exec::Split>> splits_;
  bool noMoreSplits_ = false;
};

class VeloxPlanConverter::WholeStageResIterMiddleStage
    : public WholeStageResIter {
 public:
  WholeStageResIterMiddleStage(
      memory::MemoryPool* pool,
      const std::shared_ptr<const core::PlanNode>& planNode,
      const std::vector<core::PlanNodeId>& streamIds)
      : WholeStageResIter(pool, planNode), streamIds_(streamIds) {
    core::PlanFragment planFragment{
        planNode, core::ExecutionStrategy::kUngrouped, 1};
    std::shared_ptr<core::QueryCtx> queryCtx = createNewVeloxQueryCtx();
    task_ = std::make_shared<exec::Task>(
        fmt::format("gluten task {}", ++taskSerial),
        std::move(planFragment),
        0,
        std::move(queryCtx));
    if (!task_->supportsSingleThreadedExecution()) {
      throw std::runtime_error(
          "Task doesn't support single thread execution: " +
          planNode->toString());
    }
    addSplits_ = [&](Task* task) {
      if (noMoreSplits_) {
        return;
      }
      for (const auto& streamId : streamIds_) {
        task->noMoreSplits(streamId);
      }
      noMoreSplits_ = true;
    };
  }

 private:
  bool noMoreSplits_ = false;
  std::vector<core::PlanNodeId> streamIds_;
};

} // namespace compute
} // namespace velox
