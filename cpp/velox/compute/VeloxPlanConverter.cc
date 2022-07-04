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
int64_t GetJavaThreadId();  // from jni_common.h

namespace velox {
namespace compute {

namespace {
const std::string kHiveConnectorId = "test-hive";
}
std::shared_ptr<core::QueryCtx> createNewVeloxQueryCtx() {
  int64_t jParentThreadId = GetJavaThreadId();
  if (jParentThreadId == -1L) {
    // In the case of unit testing
    return core::QueryCtx::createForTest();
  }
  // Gluten's restriction of thread naming. See
  // org.apache.spark.sql.execution.datasources.v2.arrow.SparkThreadUtils Note that the
  // thread name should not exceed 15 character limitation
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      24, std::make_shared<folly::NamedThreadFactory>(
              "G-" + std::to_string(jParentThreadId) + "-"));
  std::shared_ptr<Config> config = std::make_shared<core::MemConfig>();
  return std::make_shared<core::QueryCtx>(executor, std::move(config));
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
      getConnectorFactory(connector::hive::HiveConnectorFactory::kHiveConnectorName)
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
  aggregate::registerMinMaxAggregate<aggregate::MinAggregate,
                                     aggregate::NonNumericMinAggregate>("min");
  aggregate::registerMinMaxAggregate<aggregate::MaxAggregate,
                                     aggregate::NonNumericMaxAggregate>("max");
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::FilterRel& sfilter) {
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
  std::vector<std::shared_ptr<facebook::velox::substrait::SubstraitParser::SubstraitType>>
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
  auto rbIter = std::move(arrowInputIters_[iterIdx]);
  // TODO: manage the iter well.
  auto maybeReader = arrow::RecordBatchReader::Make(
      std::move(*(rbIter->ToArrowRecordBatchIterator())), schema);
  if (!maybeReader.status().ok()) {
    throw std::runtime_error("Reader is not created.");
  }
  auto reader = maybeReader.ValueOrDie();

  // Create ArrowArrayStream.
  struct ArrowArrayStream veloxArrayStream;
  GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatchReader(reader, &veloxArrayStream));
  auto arrowStream = std::make_shared<ArrowArrayStream>(veloxArrayStream);

  // Create Velox ArrowStream node.
  std::vector<TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(facebook::velox::substrait::toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto arrowStreamNode = std::make_shared<core::ArrowStreamNode>(
      nextPlanNodeId(), outputType, arrowStream, pool_);
  subVeloxPlanConverter_->insertInputNode(iterIdx, arrowStreamNode, planNodeId_);
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
  std::cout << "Plan Node: " << std::endl << planNode->toString(true, true) << std::endl;
#endif
  return planNode;
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

void VeloxPlanConverter::getInfoAndIds(
    std::unordered_map<core::PlanNodeId,
                       std::shared_ptr<facebook::velox::substrait::SplitInfo>>
        splitInfoMap,
    std::unordered_set<core::PlanNodeId> leafPlanNodeIds,
    std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& scanInfos,
    std::vector<core::PlanNodeId>& scanIds, std::vector<core::PlanNodeId>& streamIds) {
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

std::shared_ptr<gluten::RecordBatchResultIterator>
VeloxPlanConverter::GetResultIterator() {
  std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputs = {};
  return GetResultIterator(inputs);
}

std::shared_ptr<gluten::RecordBatchResultIterator> VeloxPlanConverter::GetResultIterator(
    std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputs) {
  if (inputs.size() > 0) {
    arrowInputIters_ = std::move(inputs);
  }
  const std::shared_ptr<const core::PlanNode> planNode = getVeloxPlanNode(plan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), planNode->leafPlanNodeIds(),
                scanInfos, scanIds, streamIds);

  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter =
        std::make_shared<WholeStageResIterMiddleStage>(pool_, planNode, streamIds);
    return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
  }
  auto wholestageIter =
      std::make_shared<WholeStageResIterFirstStage>(pool_, planNode, scanIds, scanInfos);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

std::shared_ptr<gluten::RecordBatchResultIterator> VeloxPlanConverter::GetResultIterator(
    const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
        setScanInfos) {
  const std::shared_ptr<const core::PlanNode> planNode = getVeloxPlanNode(plan_);

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), planNode->leafPlanNodeIds(),
                scanInfos, scanIds, streamIds);

  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      pool_, planNode, scanIds, setScanInfos);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

class VeloxPlanConverter::WholeStageResIter {
 public:
  WholeStageResIter(memory::MemoryPool* pool,
                    std::shared_ptr<const core::PlanNode> planNode)
      : pool_(pool), planNode_(planNode) {}

  virtual ~WholeStageResIter() {
    auto task = cursor_->task();
    if (task == nullptr) {
      return;
    }
    if (mayHaveNext_) {
      task->requestCancel();
    }
    auto* executor = task->queryCtx()->executor();
    auto* threadPool = dynamic_cast<folly::ThreadPoolExecutor*>(executor);
    if (threadPool == nullptr) {
      return;
    }
    folly::ThreadPoolExecutor::PoolStats stats = threadPool->getPoolStats();
#ifdef DEBUG
    int64_t threadId = GetJavaThreadId();
    std::cout << "Thread stats: "
              << " threadId: " << threadId << " threadCount: " << stats.threadCount
              << " idleThreadCount: " << stats.idleThreadCount
              << " activeThreadCount: " << stats.activeThreadCount
              << " pendingTaskCount: " << stats.pendingTaskCount
              << " totalTaskCount: " << stats.totalTaskCount << std::endl;
#endif
    if (stats.activeThreadCount != 0 || stats.pendingTaskCount != 0) {
#ifdef DEBUG
      std::cout << "Unfinished thread count is not zero. Joining...." << std::endl;
#endif
      threadPool->join();
#ifdef DEBUG
      std::cout << "Velox thread pool is now idle."
                << " threadId: " << threadId << std::endl;
#endif
    }
  }
  /// This method converts Velox RowVector into Arrow RecordBatch based on Velox's
  /// Arrow conversion implementation, in which memcopy is not needed for fixed-width data
  /// types, but is conducted in String conversion. The output batch will be the input of
  /// Columnar Shuffle.
  void toArrowBatch(const RowVectorPtr& rv, uint64_t numRows, const RowTypePtr& outTypes,
                    std::shared_ptr<arrow::RecordBatch>* out) {
    ArrowArray cArray{};
    ArrowSchema cSchema{};
    exportToArrow(rv, cArray, pool_);
    exportToArrow(outTypes, cSchema);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch =
        arrow::ImportRecordBatch(&cArray, &cSchema);

    if (!batch.status().ok()) {
      throw std::runtime_error("Failed to import to Arrow record batch");
    }
    *out = batch.ValueOrDie();
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    std::shared_ptr<arrow::RecordBatch> out = nullptr;
    if (!mayHaveNext_) {
      return out;
    }
    addSplits_(cursor_->task().get());
    if (cursor_->moveNext()) {
      RowVectorPtr result = cursor_->current();
      uint64_t numRows = result->size();
      if (numRows == 0) {
        return out;
      }
      auto outTypes = planNode_->outputType();
      toArrowBatch(result, numRows, outTypes, &out);
      // arrow::PrettyPrint(*out, 2, &std::cout);
      return out;
    }
    mayHaveNext_ = false;
    return out;
  }

  test::CursorParameters params_;
  std::unique_ptr<test::TaskCursor> cursor_;
  std::function<void(exec::Task*)> addSplits_;

 private:
  memory::MemoryPool* pool_;
  std::shared_ptr<const core::PlanNode> planNode_;
  bool mayHaveNext_ = true;
  // TODO: use the setted one.
  uint64_t batchSize_ = 10000;
};

class VeloxPlanConverter::WholeStageResIterFirstStage : public WholeStageResIter {
 public:
  WholeStageResIterFirstStage(
      memory::MemoryPool* pool, const std::shared_ptr<const core::PlanNode>& planNode,
      const std::vector<core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos)
      : WholeStageResIter(pool, planNode),
        scanNodeIds_(scanNodeIds),
        scanInfos_(scanInfos) {
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
        scanSplits.emplace_back(exec::Split(folly::copy(connectorSplit), groupId));
      }
      splits_.emplace_back(scanSplits);
    }

    // Set cursor parameters.
    params_.planNode = planNode;
    params_.queryCtx = createNewVeloxQueryCtx();
    cursor_ = std::make_unique<test::TaskCursor>(params_);
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
      noMoreSplits_ = true;
    };
  }

 private:
  std::vector<core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos_;
  std::vector<std::vector<exec::Split>> splits_;
  bool noMoreSplits_ = false;
};

class VeloxPlanConverter::WholeStageResIterMiddleStage : public WholeStageResIter {
 public:
  WholeStageResIterMiddleStage(memory::MemoryPool* pool,
                               const std::shared_ptr<const core::PlanNode>& planNode,
                               const std::vector<core::PlanNodeId>& streamIds)
      : WholeStageResIter(pool, planNode), streamIds_(streamIds) {
    params_.planNode = planNode;
    params_.queryCtx = createNewVeloxQueryCtx();
    cursor_ = std::make_unique<test::TaskCursor>(params_);
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

}  // namespace compute
}  // namespace velox
