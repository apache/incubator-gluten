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
#include "RegistrationAllFunctions.cc"
#include "arrow/c/Bridge.h"
#include "arrow/c/bridge.h"
#include "bridge.h"
#include "compute/exec_backend.h"
#include "velox/buffer/Buffer.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace velox {
namespace compute {

namespace {
const std::string kHiveConnectorId = "test-hive";
const std::string kSparkBatchSizeKey =
    "spark.sql.execution.arrow.maxRecordsPerBatch";
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows =
    "replacedWithDynamicFilterRows";
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";
std::atomic<int32_t> taskSerial;
} // namespace

std::shared_ptr<core::QueryCtx> createNewVeloxQueryCtx(
    memory::MemoryPool* memoryPool) {
  std::unique_ptr<memory::MemoryPool> ctxRoot =
      memoryPool->addScopedChild("ctx_root");
  static const auto kUnlimited = std::numeric_limits<int64_t>::max();
  ctxRoot->setMemoryUsageTracker(
      memory::MemoryUsageTracker::create(kUnlimited, kUnlimited, kUnlimited));
  std::shared_ptr<core::QueryCtx> ctx = std::make_shared<core::QueryCtx>(
      nullptr,
      std::make_shared<facebook::velox::core::MemConfig>(),
      std::unordered_map<std::string, std::shared_ptr<Config>>(),
      memory::MappedMemory::getInstance(),
      std::move(ctxRoot),
      nullptr,
      "");
  return ctx;
}

// The Init will be called per executor.
void VeloxInitializer::Init() {
  // Setup and register.
  filesystems::registerLocalFileSystem();
  filesystems::registerHdfsFileSystem();
  std::unique_ptr<folly::IOThreadPoolExecutor> executor =
      std::make_unique<folly::IOThreadPoolExecutor>(1);

  // TODO(yuan): should read hdfs client conf from hdfs-client.xml from
  // LIBHDFS3_CONF
  std::string hdfsUri = "localhost:9000";
  const char* envHdfsUri = std::getenv("VELOX_HDFS");
  if (envHdfsUri != nullptr) {
    hdfsUri = std::string(envHdfsUri);
  }
  auto hdfsPort = hdfsUri.substr(hdfsUri.find(":") + 1);
  auto hdfsHost = hdfsUri.substr(0, hdfsUri.find(":"));
  static const std::unordered_map<std::string, std::string> configurationValues(
      {{"hive.hdfs.host", hdfsHost}, {"hive.hdfs.port", hdfsPort}});
  auto properties =
      std::make_shared<const core::MemConfig>(configurationValues);
  auto hiveConnector =
      getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, nullptr);
  registerConnector(hiveConnector);
  parquet::registerParquetReaderFactory(ParquetReaderType::NATIVE);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  registerAllFunctions();
}

void VeloxPlanConverter::setInputPlanNode(
    const ::substrait::ExpandRel& sexpand) {
  if (sexpand.has_input()) {
    setInputPlanNode(sexpand.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::SortRel& ssort) {
  if (ssort.has_input()) {
    setInputPlanNode(ssort.input());
  } else {
    throw std::runtime_error("Child expected");
  }
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
      nextPlanNodeId(),
      outputType,
      arrowStream,
      gluten::memory::GetDefaultWrappedVeloxMemoryPool().get());
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
  } else if (srel.has_sort()) {
    setInputPlanNode(srel.sort());
  } else if (srel.has_expand()) {
    setInputPlanNode(srel.expand());
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
#ifdef GLUTEN_PRINT_DEBUG
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

std::shared_ptr<gluten::GlutenResultIterator>
VeloxPlanConverter::GetResultIterator(
    gluten::memory::MemoryAllocator* allocator) {
  std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs = {};
  return GetResultIterator(allocator, inputs);
}

std::shared_ptr<gluten::GlutenResultIterator>
VeloxPlanConverter::GetResultIterator(
    gluten::memory::MemoryAllocator* allocator,
    std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs) {
  if (inputs.size() > 0) {
    arrowInputIters_ = std::move(inputs);
  }
  planNode_ = getVeloxPlanNode(plan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(
      subVeloxPlanConverter_->splitInfos(),
      planNode_->leafPlanNodeIds(),
      scanInfos,
      scanIds,
      streamIds);

  auto veloxPool = gluten::memory::AsWrappedVeloxMemoryPool(allocator);
  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter = std::make_shared<WholeStageResIterMiddleStage>(
        veloxPool, planNode_, streamIds, confMap_);
    return std::make_shared<gluten::GlutenResultIterator>(
        std::move(wholestageIter), shared_from_this());
  }
  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      veloxPool, planNode_, scanIds, scanInfos, streamIds, confMap_);
  return std::make_shared<gluten::GlutenResultIterator>(
      std::move(wholestageIter), shared_from_this());
}

std::shared_ptr<gluten::GlutenResultIterator>
VeloxPlanConverter::GetResultIterator(
    gluten::memory::MemoryAllocator* allocator,
    const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
        setScanInfos) {
  planNode_ = getVeloxPlanNode(plan_);

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(
      subVeloxPlanConverter_->splitInfos(),
      planNode_->leafPlanNodeIds(),
      scanInfos,
      scanIds,
      streamIds);

  auto veloxPool = gluten::memory::AsWrappedVeloxMemoryPool(allocator);
  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      veloxPool, planNode_, scanIds, setScanInfos, streamIds, confMap_);
  return std::make_shared<gluten::GlutenResultIterator>(
      std::move(wholestageIter), shared_from_this());
}

std::shared_ptr<arrow::Schema> VeloxPlanConverter::GetOutputSchema() {
  if (output_schema_ == nullptr) {
    cacheOutputSchema(planNode_);
  }
  return output_schema_;
}

void VeloxPlanConverter::cacheOutputSchema(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  ArrowSchema arrowSchema{};
  exportToArrow(
      BaseVector::create(
          planNode->outputType(),
          0,
          gluten::memory::GetDefaultWrappedVeloxMemoryPool().get()),
      arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(output_schema_, arrow::ImportSchema(&arrowSchema));
}

arrow::Result<std::shared_ptr<GlutenVeloxColumnarBatch>>
WholeStageResIter::Next() {
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
  return std::make_shared<GlutenVeloxColumnarBatch>(vector);
}

memory::MemoryPool* WholeStageResIter::getPool() const {
  return pool_.get();
}

void WholeStageResIter::getOrderedNodeIds(
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::vector<core::PlanNodeId>& nodeIds) {
  bool isProjectNode = false;
  if (std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
    isProjectNode = true;
  }

  const auto& sourceNodes = planNode->sources();
  for (const auto& sourceNode : sourceNodes) {
    // Filter over Project are mapped into FilterProject operator in Velox.
    // Metrics are all applied on Project node, and the metrics for Filter node
    // does not exist.
    if (isProjectNode &&
        std::dynamic_pointer_cast<const core::FilterNode>(sourceNode)) {
      omittedNodeIds_.insert(sourceNode->id());
    }
    getOrderedNodeIds(sourceNode, nodeIds);
  }
  nodeIds.emplace_back(planNode->id());
}

void WholeStageResIter::collectMetrics() {
  if (metrics_) {
    // The metrics has already been created.
    return;
  }

  auto planStats = toPlanStats(task_->taskStats());
  // Calculate the total number of metrics.
  int numOfStats = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
#ifdef DEBUG
        std::cout << "Not found node id: " << nodeId << std::endl;
        std::cout << "Plan Node: " << std::endl
                  << planNode_->toString(true, true) << std::endl;
#endif
        throw std::runtime_error("Node id cannot be found in plan status.");
      }
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      numOfStats += 1;
      continue;
    }
    numOfStats += planStats.at(nodeId).operatorStats.size();
  }

  metrics_ = std::make_shared<Metrics>(numOfStats);
  int metricsIdx = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      metricsIdx += 1;
      continue;
    }
    const auto& status = planStats.at(nodeId);
    // Add each operator status into metrics.
    for (const auto& entry : status.operatorStats) {
      metrics_->inputRows[metricsIdx] = entry.second->inputRows;
      metrics_->inputVectors[metricsIdx] = entry.second->inputVectors;
      metrics_->inputBytes[metricsIdx] = entry.second->inputBytes;
      metrics_->rawInputRows[metricsIdx] = entry.second->rawInputRows;
      metrics_->rawInputBytes[metricsIdx] = entry.second->rawInputBytes;
      metrics_->outputRows[metricsIdx] = entry.second->outputRows;
      metrics_->outputVectors[metricsIdx] = entry.second->outputVectors;
      metrics_->outputBytes[metricsIdx] = entry.second->outputBytes;
      metrics_->count[metricsIdx] = entry.second->cpuWallTiming.count;
      metrics_->wallNanos[metricsIdx] = entry.second->cpuWallTiming.wallNanos;
      metrics_->peakMemoryBytes[metricsIdx] = entry.second->peakMemoryBytes;
      metrics_->numMemoryAllocations[metricsIdx] =
          entry.second->numMemoryAllocations;
      metrics_->numDynamicFiltersProduced[metricsIdx] = sumOfRuntimeMetric(
          entry.second->customStats, kDynamicFiltersProduced);
      metrics_->numDynamicFiltersAccepted[metricsIdx] = sumOfRuntimeMetric(
          entry.second->customStats, kDynamicFiltersAccepted);
      metrics_->numReplacedWithDynamicFilterRows[metricsIdx] =
          sumOfRuntimeMetric(
              entry.second->customStats, kReplacedWithDynamicFilterRows);
      metricsIdx += 1;
    }
  }
}

int64_t WholeStageResIter::sumOfRuntimeMetric(
    const std::unordered_map<std::string, RuntimeMetric>& runtimeStats,
    const std::string& metricId) const {
  if (runtimeStats.size() == 0 ||
      runtimeStats.find(metricId) == runtimeStats.end()) {
    return 0;
  }
  return runtimeStats.at(metricId).sum;
}

void WholeStageResIter::setConfToQueryContext(
    const std::shared_ptr<core::QueryCtx>& queryCtx) {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set it to Velox query context.
  auto got = confMap_.find(kSparkBatchSizeKey);
  if (got != confMap_.end()) {
    configs[core::QueryConfig::kPreferredOutputBatchSize] = got->second;
  }
  // To align with Spark's behavior, set casting to int to be truncating.
  configs[core::QueryConfig::kCastIntByTruncate] = std::to_string(true);
  queryCtx->setConfigOverridesUnsafe(std::move(configs));
}

class VeloxPlanConverter::WholeStageResIterFirstStage
    : public WholeStageResIter {
 public:
  WholeStageResIterFirstStage(
      std::shared_ptr<memory::MemoryPool> pool,
      const std::shared_ptr<const core::PlanNode>& planNode,
      const std::vector<core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
          scanInfos,
      const std::vector<core::PlanNodeId>& streamIds,
      const std::unordered_map<std::string, std::string>& confMap)
      : WholeStageResIter(pool, planNode, confMap),
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
        auto partitionKeys = extractPartitionColumnAndValue(paths[idx]);
        auto split = std::make_shared<hive::HiveConnectorSplit>(
            kHiveConnectorId,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys);
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
    std::shared_ptr<core::QueryCtx> queryCtx =
        createNewVeloxQueryCtx(getPool());
    // Set customized confs to query context.
    setConfToQueryContext(queryCtx);
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

  // Extract the partition column and value from a path of split.
  // The split path is like .../my_dataset/year=2022/month=July/split_file.
  std::unordered_map<std::string, std::optional<std::string>>
  extractPartitionColumnAndValue(const std::string& filePath) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;

    // Column name with '=' is not supported now.
    std::string delimiter = "=";
    std::string str = filePath;
    std::size_t pos = str.find(delimiter);
    while (pos != std::string::npos) {
      // Split the string with delimiter.
      std::string prePart = str.substr(0, pos);
      std::string latterPart = str.substr(pos + 1, str.size() - 1);
      // Extract the partition column.
      pos = prePart.find_last_of("/");
      std::string partitionColumn = prePart.substr(pos + 1, prePart.size() - 1);
      // Extract the partition value.
      pos = latterPart.find("/");
      std::string partitionValue = latterPart.substr(0, pos);
      if (partitionValue == kHiveDefaultPartition) {
        partitionKeys[partitionColumn] = std::nullopt;
      } else {
        // Set to the map of partition keys.
        partitionKeys[partitionColumn] = partitionValue;
      }
      // For processing the remaining keys.
      str = latterPart.substr(pos + 1, latterPart.size() - 1);
      pos = str.find(delimiter);
    }
    return partitionKeys;
  }
};

class VeloxPlanConverter::WholeStageResIterMiddleStage
    : public WholeStageResIter {
 public:
  WholeStageResIterMiddleStage(
      std::shared_ptr<memory::MemoryPool> pool,
      const std::shared_ptr<const core::PlanNode>& planNode,
      const std::vector<core::PlanNodeId>& streamIds,
      const std::unordered_map<std::string, std::string>& confMap)
      : WholeStageResIter(pool, planNode, confMap), streamIds_(streamIds) {
    core::PlanFragment planFragment{
        planNode, core::ExecutionStrategy::kUngrouped, 1};
    std::shared_ptr<core::QueryCtx> queryCtx =
        createNewVeloxQueryCtx(getPool());
    // Set customized confs to query context.
    setConfToQueryContext(queryCtx);
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

GlutenVeloxColumnarBatch::~GlutenVeloxColumnarBatch() = default;

std::string GlutenVeloxColumnarBatch::GetType() {
  return "velox";
}

void GlutenVeloxColumnarBatch::EnsureFlattened() {
  if (flattened_ != nullptr) {
    return;
  }
  auto startTime = std::chrono::steady_clock::now();
  // Make sure to load lazy vector if not loaded already.
  for (auto& child : rowVector_->children()) {
    child->loadedVector();
  }

  // Perform copy to flatten dictionary vectors.
  RowVectorPtr copy = std::dynamic_pointer_cast<RowVector>(BaseVector::create(
      rowVector_->type(), rowVector_->size(), rowVector_->pool()));
  copy->copy(rowVector_.get(), 0, 0, rowVector_->size());
  flattened_ = copy;
  auto endTime = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime)
          .count();
  exportNanos_ += duration;
}

std::shared_ptr<ArrowSchema> GlutenVeloxColumnarBatch::exportArrowSchema() {
  std::shared_ptr<ArrowSchema> out = std::make_shared<ArrowSchema>();
  EnsureFlattened();
  facebook::velox::exportToArrow(flattened_, *out);
  return out;
}

std::shared_ptr<ArrowArray> GlutenVeloxColumnarBatch::exportArrowArray() {
  std::shared_ptr<ArrowArray> out = std::make_shared<ArrowArray>();
  EnsureFlattened();
  facebook::velox::exportToArrow(
      flattened_,
      *out,
      gluten::memory::GetDefaultWrappedVeloxMemoryPool().get());
  return out;
}

RowVectorPtr GlutenVeloxColumnarBatch::getRowVector() const {
  return rowVector_;
}

RowVectorPtr GlutenVeloxColumnarBatch::getFlattenedRowVector() {
  EnsureFlattened();
  return flattened_;
}

} // namespace compute
} // namespace velox
