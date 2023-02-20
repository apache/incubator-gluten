#include "WholeStageResultIterator.h"
#include "config/GlutenConfig.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/PlanNodeStats.h"

using namespace facebook;
using namespace facebook::velox;

namespace gluten {

namespace {
const std::string kSparkBatchSizeKey = "spark.sql.execution.arrow.maxRecordsPerBatch";
const std::string kSparkOffHeapSizeKey = "spark.memory.offHeap.size";
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows = "replacedWithDynamicFilterRows";
const std::string kFlushRowCount = "flushRowCount";
const std::string kTotalScanTime = "totalScanTime";
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";
std::atomic<int32_t> taskSerial;
} // namespace

std::shared_ptr<velox::core::QueryCtx> createNewVeloxQueryCtx(
    std::shared_ptr<velox::Config> connectorConfig,
    velox::memory::MemoryPool* memoryPool) {
  std::shared_ptr<velox::memory::MemoryPool> ctxRoot = memoryPool->addChild("ctx_root");
  ctxRoot->setMemoryUsageTracker(velox::memory::MemoryUsageTracker::create());
  std::unordered_map<std::string, std::shared_ptr<velox::Config>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = connectorConfig;
  std::shared_ptr<velox::core::QueryCtx> ctx = std::make_shared<velox::core::QueryCtx>(
      nullptr,
      std::make_shared<velox::core::MemConfig>(),
      connectorConfigs,
      velox::memory::MemoryAllocator::getInstance(),
      std::move(ctxRoot),
      nullptr,
      "");
  return ctx;
}

arrow::Result<std::shared_ptr<VeloxColumnarBatch>> WholeStageResultIterator::Next() {
  addSplits_(task_.get());
  if (task_->isFinished()) {
    return nullptr;
  }
  velox::RowVectorPtr vector = task_->next();
  if (vector == nullptr) {
    return nullptr;
  }
  uint64_t numRows = vector->size();
  if (numRows == 0) {
    return nullptr;
  }
  return std::make_shared<VeloxColumnarBatch>(vector);
}

void WholeStageResultIterator::getOrderedNodeIds(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    std::vector<velox::core::PlanNodeId>& nodeIds) {
  bool isProjectNode = false;
  if (std::dynamic_pointer_cast<const velox::core::ProjectNode>(planNode)) {
    isProjectNode = true;
  }

  const auto& sourceNodes = planNode->sources();
  for (const auto& sourceNode : sourceNodes) {
    // Filter over Project are mapped into FilterProject operator in Velox.
    // Metrics are all applied on Project node, and the metrics for Filter node
    // do not exist.
    if (isProjectNode && std::dynamic_pointer_cast<const velox::core::FilterNode>(sourceNode)) {
      omittedNodeIds_.insert(sourceNode->id());
    }
    getOrderedNodeIds(sourceNode, nodeIds);
  }
  nodeIds.emplace_back(planNode->id());
}

void WholeStageResultIterator::collectMetrics() {
  if (metrics_) {
    // The metrics has already been created.
    return;
  }

  auto planStats = velox::exec::toPlanStats(task_->taskStats());
  // Calculate the total number of metrics.
  int numOfStats = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
#ifdef DEBUG
        std::cout << "Not found node id: " << nodeId << std::endl;
        std::cout << "Plan Node: " << std::endl << planNode_->toString(true, true) << std::endl;
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
      metrics_->cpuNanos[metricsIdx] = entry.second->cpuWallTiming.cpuNanos;
      metrics_->wallNanos[metricsIdx] = entry.second->cpuWallTiming.wallNanos;
      metrics_->peakMemoryBytes[metricsIdx] = entry.second->peakMemoryBytes;
      metrics_->numMemoryAllocations[metricsIdx] = entry.second->numMemoryAllocations;
      metrics_->spilledBytes[metricsIdx] = entry.second->spilledBytes;
      metrics_->spilledRows[metricsIdx] = entry.second->spilledRows;
      metrics_->spilledPartitions[metricsIdx] = entry.second->spilledPartitions;
      metrics_->spilledFiles[metricsIdx] = entry.second->spilledFiles;
      metrics_->numDynamicFiltersProduced[metricsIdx] =
          runtimeMetric("sum", entry.second->customStats, kDynamicFiltersProduced);
      metrics_->numDynamicFiltersAccepted[metricsIdx] =
          runtimeMetric("sum", entry.second->customStats, kDynamicFiltersAccepted);
      metrics_->numReplacedWithDynamicFilterRows[metricsIdx] =
          runtimeMetric("sum", entry.second->customStats, kReplacedWithDynamicFilterRows);
      metrics_->flushRowCount[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kFlushRowCount);
      metrics_->scanTime[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kTotalScanTime);
      metricsIdx += 1;
    }
  }
}

int64_t WholeStageResultIterator::runtimeMetric(
    const std::string& metricType,
    const std::unordered_map<std::string, velox::RuntimeMetric>& runtimeStats,
    const std::string& metricId) const {
  if (runtimeStats.size() == 0 || runtimeStats.find(metricId) == runtimeStats.end()) {
    return 0;
  }
  if (metricType == "sum") {
    return runtimeStats.at(metricId).sum;
  }
  if (metricType == "count") {
    return runtimeStats.at(metricId).count;
  }
  if (metricType == "min") {
    return runtimeStats.at(metricId).min;
  }
  if (metricType == "max") {
    return runtimeStats.at(metricId).max;
  }
  return 0;
}

void WholeStageResultIterator::setConfToQueryContext(const std::shared_ptr<velox::core::QueryCtx>& queryCtx) {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set it to Velox query context.
  auto got = confMap_.find(kSparkBatchSizeKey);
  if (got != confMap_.end()) {
    configs[velox::core::QueryConfig::kPreferredOutputBatchSize] = got->second;
  }
  // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
  got = confMap_.find(kSparkOffHeapSizeKey);
  if (got != confMap_.end()) {
    try {
      // Set the max memory of partial aggregation as 3/4 of offheap size.
      auto maxMemory = (long)(0.75 * std::stol(got->second));
      configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxMemory);
    } catch (const std::invalid_argument&) {
      throw std::runtime_error("Invalid offheap size.");
    }
  }
  // To align with Spark's behavior, set casting to int to be truncating.
  configs[velox::core::QueryConfig::kCastIntByTruncate] = std::to_string(true);
  configs[velox::core::QueryConfig::kSpillEnabled] = std::to_string(true);
  queryCtx->setConfigOverridesUnsafe(std::move(configs));
}

std::shared_ptr<velox::Config> WholeStageResultIterator::getConnectorConfig() {
  std::unordered_map<std::string, std::string> configs = {};
  auto got = confMap_.find(kCaseSensitive);
  if (got != confMap_.end()) {
    configs[velox::connector::hive::HiveConfig::kCaseSensitive] = got->second;
  }
  return std::make_shared<velox::core::MemConfig>(configs);
}

WholeStageResultIteratorFirstStage::WholeStageResultIteratorFirstStage(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& scanNodeIds,
    const std::vector<std::shared_ptr<velox::substrait::SplitInfo>>& scanInfos,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::unordered_map<std::string, std::string>& confMap)
    : WholeStageResultIterator(pool, planNode, confMap),
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

    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> connectorSplits;
    connectorSplits.reserve(paths.size());
    for (int idx = 0; idx < paths.size(); idx++) {
      auto partitionKeys = extractPartitionColumnAndValue(paths[idx]);
      auto split = std::make_shared<velox::connector::hive::HiveConnectorSplit>(
          kHiveConnectorId, paths[idx], format, starts[idx], lengths[idx], partitionKeys);
      connectorSplits.emplace_back(split);
    }

    std::vector<velox::exec::Split> scanSplits;
    scanSplits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      // Bucketed group id (-1 means 'none').
      int32_t groupId = -1;
      scanSplits.emplace_back(velox::exec::Split(folly::copy(connectorSplit), groupId));
    }
    splits_.emplace_back(scanSplits);
  }

  // Set task parameters.
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx(getConnectorConfig(), getPool());

  // Set customized confs to query context.
  setConfToQueryContext(queryCtx);
  task_ = std::make_shared<velox::exec::Task>(
      fmt::format("gluten task {}", ++taskSerial), std::move(planFragment), 0, std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  addSplits_ = [&](velox::exec::Task* task) {
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

std::unordered_map<std::string, std::optional<std::string>>
WholeStageResultIteratorFirstStage::extractPartitionColumnAndValue(const std::string& filePath) {
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;

  // Column name with '=' is not supported now.
  std::string delimiter = "=";
  std::string str = filePath;
  std::size_t pos = str.find(delimiter);

  while (pos != std::string::npos) {
    // Split the string with delimiter.
    std::string prePart = str.substr(0, pos);
    std::string latterPart = str.substr(pos + 1);
    // Extract the partition column.
    pos = prePart.find_last_of("/");
    std::string partitionColumn;
    if (pos == std::string::npos) {
      partitionColumn = prePart;
    } else {
      partitionColumn = prePart.substr(pos + 1);
    }
    // Extract the partition value.
    pos = latterPart.find("/");
    if (pos == std::string::npos) {
      throw std::runtime_error("No value found for partition key: " + partitionColumn + " in path: " + filePath);
    }
    std::string partitionValue = latterPart.substr(0, pos);
    if (!folly::to<bool>(confMap_[kCaseSensitive])) {
      folly::toLowerAscii(partitionColumn);
    }
    if (partitionValue == kHiveDefaultPartition) {
      partitionKeys[partitionColumn] = std::nullopt;
    } else {
      // Set to the map of partition keys.
      partitionKeys[partitionColumn] = partitionValue;
    }
    // For processing the remaining keys.
    str = latterPart.substr(pos + 1);
    pos = str.find(delimiter);
  }
  return partitionKeys;
}

WholeStageResultIteratorMiddleStage::WholeStageResultIteratorMiddleStage(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::unordered_map<std::string, std::string>& confMap)
    : WholeStageResultIterator(pool, planNode, confMap), streamIds_(streamIds) {
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx(getConnectorConfig(), getPool());
  // Set customized confs to query context.
  setConfToQueryContext(queryCtx);

  task_ = std::make_shared<velox::exec::Task>(
      fmt::format("gluten task {}", ++taskSerial), std::move(planFragment), 0, std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  addSplits_ = [&](velox::exec::Task* task) {
    if (noMoreSplits_) {
      return;
    }
    for (const auto& streamId : streamIds_) {
      task->noMoreSplits(streamId);
    }
    noMoreSplits_ = true;
  };
}

} // namespace gluten
