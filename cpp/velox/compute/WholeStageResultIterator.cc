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
#include "WholeStageResultIterator.h"
#include "VeloxBackend.h"
#include "VeloxInitializer.h"
#include "config/GlutenConfig.h"
#include "utils/URLDecoder.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/PlanNodeStats.h"

#include "utils/ConfigExtractor.h"

#ifdef ENABLE_HDFS
#include <hdfs/hdfs.h>
#endif

using namespace facebook;

namespace gluten {

namespace {
// Velox configs
const std::string kHiveConnectorId = "test-hive";

// memory
const std::string kSpillStrategy = "spark.gluten.sql.columnar.backend.velox.spillStrategy";
const std::string kAggregationSpillEnabled = "spark.gluten.sql.columnar.backend.velox.aggregationSpillEnabled";
const std::string kJoinSpillEnabled = "spark.gluten.sql.columnar.backend.velox.joinSpillEnabled";
const std::string kOrderBySpillEnabled = "spark.gluten.sql.columnar.backend.velox.orderBySpillEnabled";
const std::string kAggregationSpillMemoryThreshold =
    "spark.gluten.sql.columnar.backend.velox.aggregationSpillMemoryThreshold";
const std::string kJoinSpillMemoryThreshold = "spark.gluten.sql.columnar.backend.velox.joinSpillMemoryThreshold";
const std::string kOrderBySpillMemoryThreshold = "spark.gluten.sql.columnar.backend.velox.orderBySpillMemoryThreshold";
const std::string kMaxSpillLevel = "spark.gluten.sql.columnar.backend.velox.maxSpillLevel";
const std::string kMaxSpillFileSize = "spark.gluten.sql.columnar.backend.velox.maxSpillFileSize";
const std::string kMinSpillRunSize = "spark.gluten.sql.columnar.backend.velox.minSpillRunSize";
const std::string kSpillStartPartitionBit = "spark.gluten.sql.columnar.backend.velox.spillStartPartitionBit";
const std::string kSpillPartitionBits = "spark.gluten.sql.columnar.backend.velox.spillPartitionBits";
const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct";

// metrics
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows = "replacedWithDynamicFilterRows";
const std::string kFlushRowCount = "flushRowCount";
const std::string kTotalScanTime = "totalScanTime";
const std::string kSkippedSplits = "skippedSplits";
const std::string kProcessedSplits = "processedSplits";
const std::string kSkippedStrides = "skippedStrides";
const std::string kProcessedStrides = "processedStrides";

// others
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

} // namespace

WholeStageResultIterator::WholeStageResultIterator(
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
    const std::unordered_map<std::string, std::string>& confMap)
    : veloxPlan_(planNode), confMap_(confMap), pool_(pool) {
#ifdef ENABLE_HDFS
  updateHdfsTokens();
#endif
  spillStrategy_ = getConfigValue(confMap_, kSpillStrategy, "auto");
  getOrderedNodeIds(veloxPlan_, orderedNodeIds_);
}

std::shared_ptr<velox::core::QueryCtx> WholeStageResultIterator::createNewVeloxQueryCtx() {
  std::unordered_map<std::string, std::shared_ptr<velox::Config>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = createConnectorConfig();
  std::shared_ptr<velox::core::QueryCtx> ctx = std::make_shared<velox::core::QueryCtx>(
      nullptr,
      getQueryContextConf(),
      connectorConfigs,
      gluten::VeloxInitializer::get()->getAsyncDataCache(),
      pool_,
      nullptr,
      "");
  return ctx;
}

std::shared_ptr<ColumnarBatch> WholeStageResultIterator::next() {
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
  for (auto& child : vector->children()) {
    child->loadedVector();
  }

  return std::make_shared<VeloxColumnarBatch>(vector);
}

int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  std::string poolName{pool_->root()->name() + "/" + pool_->name()};
  std::string logPrefix{"Spill[" + poolName + "]: "};
  LOG(INFO) << logPrefix << "Trying to reclaim " << size << " bytes of data...";
  LOG(INFO) << logPrefix << "Pool has reserved " << pool_->currentBytes() << "/" << pool_->root()->reservedBytes()
            << "/" << pool_->root()->capacity() << "/" << pool_->root()->maxCapacity() << " bytes.";
  LOG(INFO) << logPrefix << "Shrinking...";
  int64_t shrunk = pool_->shrinkManaged(pool_.get(), size);
  LOG(INFO) << logPrefix << shrunk << " bytes released from shrinking.";

  // todo return the actual spilled size?
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunk;
    LOG(INFO) << logPrefix << "Trying to request spilling for remaining " << remaining << " bytes...";
    // if we are on one of the driver of the spilled task, suspend it
    velox::exec::Driver* thisDriver = nullptr;
    task_->testingVisitDrivers([&](velox::exec::Driver* driver) {
      if (driver->isOnThread()) {
        thisDriver = driver;
      }
    });
    if (thisDriver == nullptr) {
      // not the driver, no need to suspend
      uint64_t spilledOut = pool_->reclaim(remaining);
      LOG(INFO) << logPrefix << "Successfully spilled out " << spilledOut << " bytes.";
      uint64_t total = shrunk + spilledOut;
      LOG(INFO) << logPrefix << "Successfully reclaimed total " << total << " bytes.";
      return spilledOut;
    }
    // suspend since we are on driver
    velox::exec::SuspendedSection noCancel(thisDriver);
    uint64_t spilledOut = pool_->reclaim(remaining);
    LOG(INFO) << logPrefix << "Successfully spilled out " << spilledOut << " bytes.";
    uint64_t total = shrunk + spilledOut;
    LOG(INFO) << logPrefix << "Successfully reclaimed total " << total << " bytes.";
    return spilledOut;
  }

  LOG(INFO) << logPrefix << "Successfully reclaimed total " << shrunk << " bytes.";
  return 0;
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
#ifdef GLUTEN_PRINT_DEBUG
        std::cout << "Not found node id: " << nodeId << std::endl;
        std::cout << "Plan Node: " << std::endl << veloxPlan_->toString(true, true) << std::endl;
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
      metrics_->cpuCount[metricsIdx] = entry.second->cpuWallTiming.count;
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
      metrics_->skippedSplits[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kSkippedSplits);
      metrics_->processedSplits[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kProcessedSplits);
      metrics_->skippedStrides[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kSkippedStrides);
      metrics_->processedStrides[metricsIdx] = runtimeMetric("sum", entry.second->customStats, kProcessedStrides);
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

std::unordered_map<std::string, std::string> WholeStageResultIterator::getQueryContextConf() {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set the preferred and max batch size.
  configs[velox::core::QueryConfig::kPreferredOutputBatchRows] = getConfigValue(confMap_, kSparkBatchSize, "4096");
  configs[velox::core::QueryConfig::kMaxOutputBatchRows] = getConfigValue(confMap_, kSparkBatchSize, "4096");
  // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
  // FIXME this uses process-wise off-heap memory which is not for task
  try {
    // To align with Spark's behavior, set casting to int to be truncating.
    configs[velox::core::QueryConfig::kCastToIntByTruncate] = std::to_string(true);
    configs[velox::core::QueryConfig::kSessionTimezone] = getConfigValue(confMap_, kSessionTimezone, "");
    // Adjust timestamp according to the above configured session timezone.
    configs[velox::core::QueryConfig::kAdjustTimestampToTimezone] = std::to_string(true);

    // Set the max memory of partial aggregation as 3/4 of offheap size.
    auto maxMemory =
        (long)(0.75 * (double)std::stol(getConfigValue(confMap_, kSparkTaskOffHeapMemory, std::to_string(facebook::velox::memory::kMaxMemory))));
    configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxMemory);
    configs[velox::core::QueryConfig::kAbandonPartialAggregationMinPct] = std::to_string(90);
    // Spill configs
    if (spillStrategy_ == "none") {
      configs[velox::core::QueryConfig::kSpillEnabled] = "false";
    } else {
      configs[velox::core::QueryConfig::kSpillEnabled] = "true";
    }
    configs[velox::core::QueryConfig::kAggregationSpillEnabled] =
        getConfigValue(confMap_, kAggregationSpillEnabled, "true");
    configs[velox::core::QueryConfig::kJoinSpillEnabled] = getConfigValue(confMap_, kJoinSpillEnabled, "true");
    configs[velox::core::QueryConfig::kOrderBySpillEnabled] = getConfigValue(confMap_, kOrderBySpillEnabled, "true");
    configs[velox::core::QueryConfig::kAggregationSpillMemoryThreshold] =
        getConfigValue(confMap_, kAggregationSpillMemoryThreshold, "0"); // spill only when input doesn't fit
    configs[velox::core::QueryConfig::kJoinSpillMemoryThreshold] =
        getConfigValue(confMap_, kJoinSpillMemoryThreshold, "0"); // spill only when input doesn't fit
    configs[velox::core::QueryConfig::kOrderBySpillMemoryThreshold] =
        getConfigValue(confMap_, kOrderBySpillMemoryThreshold, "0"); // spill only when input doesn't fit
    configs[velox::core::QueryConfig::kMaxSpillLevel] = getConfigValue(confMap_, kMaxSpillLevel, "4");
    configs[velox::core::QueryConfig::kMaxSpillFileSize] = getConfigValue(confMap_, kMaxSpillFileSize, "0");
    configs[velox::core::QueryConfig::kMinSpillRunSize] =
        getConfigValue(confMap_, kMinSpillRunSize, std::to_string(256 << 20));
    configs[velox::core::QueryConfig::kSpillStartPartitionBit] =
        getConfigValue(confMap_, kSpillStartPartitionBit, "29");
    configs[velox::core::QueryConfig::kSpillPartitionBits] = getConfigValue(confMap_, kSpillPartitionBits, "2");
    configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
        getConfigValue(confMap_, kSpillableReservationGrowthPct, "25");
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

#ifdef ENABLE_HDFS
void WholeStageResultIterator::updateHdfsTokens() {
  const auto& username = confMap_[kUGIUserName];
  const auto& allTokens = confMap_[kUGITokens];

  if (username.empty() || allTokens.empty())
    return;

  hdfsSetDefautUserName(username.c_str());
  std::vector<folly::StringPiece> tokens;
  folly::split('\0', allTokens, tokens);
  for (auto& token : tokens)
    hdfsSetTokenForDefaultUser(token.data());
}
#endif

std::shared_ptr<velox::Config> WholeStageResultIterator::createConnectorConfig() {
  std::unordered_map<std::string, std::string> configs = {};
  // The semantics of reading as lower case is opposite with case-sensitive.
  configs[velox::connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCase] =
      getConfigValue(confMap_, kCaseSensitive, "false") == "false" ? "true" : "false";
  return std::make_shared<velox::core::MemConfig>(configs);
}

WholeStageResultIteratorFirstStage::WholeStageResultIteratorFirstStage(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& scanNodeIds,
    const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo taskInfo)
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
    const auto& partitionColumns = scanInfo->partitionColumns;

    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> connectorSplits;
    connectorSplits.reserve(paths.size());
    for (int idx = 0; idx < paths.size(); idx++) {
      auto partitionColumn = partitionColumns[idx];
      std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
      constructPartitionColumns(partitionKeys, partitionColumn);
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
  std::unordered_set<velox::core::PlanNodeId> emptySet;
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1, emptySet};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx();

  task_ = velox::exec::Task::create(
      fmt::format("Gluten stage-{} task-{}", taskInfo.stageId, taskInfo.taskId),
      std::move(planFragment),
      0,
      std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  task_->setSpillDirectory(spillDir);
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
    noMoreSplits_ = true;
  };
}

void WholeStageResultIteratorFirstStage::constructPartitionColumns(
    std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
    const std::unordered_map<std::string, std::string>& map) {
  for (const auto& partitionColumn : map) {
    auto key = partitionColumn.first;
    const auto value = partitionColumn.second;
    if (!folly::to<bool>(getConfigValue(confMap_, kCaseSensitive, "false"))) {
      folly::toLowerAscii(key);
    }
    if (value == kHiveDefaultPartition) {
      partitionKeys[key] = std::nullopt;
    } else {
      partitionKeys[key] = value;
    }
  }
}

WholeStageResultIteratorMiddleStage::WholeStageResultIteratorMiddleStage(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo taskInfo)
    : WholeStageResultIterator(pool, planNode, confMap), streamIds_(streamIds) {
  std::unordered_set<velox::core::PlanNodeId> emptySet;
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1, emptySet};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx();

  task_ = velox::exec::Task::create(
      fmt::format("Gluten stage-{} task-{}", taskInfo.stageId, taskInfo.taskId),
      std::move(planFragment),
      0,
      std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  task_->setSpillDirectory(spillDir);
  addSplits_ = [&](velox::exec::Task* task) {
    if (noMoreSplits_) {
      return;
    }
    noMoreSplits_ = true;
  };
}

} // namespace gluten
