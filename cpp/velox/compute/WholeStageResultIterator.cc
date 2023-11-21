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
#include "VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "velox/connectors/hive/HiveConfig.h"
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
const std::string kSpillStrategyDefaultValue = "auto";
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
const std::string kSpillCompressionKind = "spark.io.compression.codec";
const std::string kMaxPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio";
const std::string kMaxExtendedPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio";
const std::string kAbandonPartialAggregationMinPct =
    "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct";
const std::string kAbandonPartialAggregationMinRows =
    "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows";
const std::string kBloomFilterExpectedNumItems = "spark.gluten.sql.columnar.backend.velox.bloomFilter.expectedNumItems";
const std::string kBloomFilterNumBits = "spark.gluten.sql.columnar.backend.velox.bloomFilter.numBits";
const std::string kBloomFilterMaxNumBits = "spark.gluten.sql.columnar.backend.velox.bloomFilter.maxNumBits";

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
    VeloxMemoryManager* memoryManager,
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo& taskInfo)
    : veloxPlan_(planNode), confMap_(confMap), taskInfo_(taskInfo), memoryManager_(memoryManager) {
#ifdef ENABLE_HDFS
  updateHdfsTokens();
#endif
  spillStrategy_ = getConfigValue(confMap_, kSpillStrategy, kSpillStrategyDefaultValue);
  getOrderedNodeIds(veloxPlan_, orderedNodeIds_);
}

std::shared_ptr<velox::core::QueryCtx> WholeStageResultIterator::createNewVeloxQueryCtx() {
  std::unordered_map<std::string, std::shared_ptr<velox::Config>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = createConnectorConfig();
  std::shared_ptr<velox::core::QueryCtx> ctx = std::make_shared<velox::core::QueryCtx>(
      nullptr,
      facebook::velox::core::QueryConfig{getQueryContextConf()},
      connectorConfigs,
      gluten::VeloxBackend::get()->getAsyncDataCache(),
      memoryManager_->getAggregateMemoryPool(),
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

namespace {
class ConditionalSuspendedSection {
 public:
  ConditionalSuspendedSection(velox::exec::Driver* driver, bool condition) {
    if (condition) {
      section_ = new velox::exec::SuspendedSection(driver);
    }
  }

  virtual ~ConditionalSuspendedSection() {
    if (section_) {
      delete section_;
    }
  }

  // singleton
  ConditionalSuspendedSection(const ConditionalSuspendedSection&) = delete;
  ConditionalSuspendedSection(ConditionalSuspendedSection&&) = delete;
  ConditionalSuspendedSection& operator=(const ConditionalSuspendedSection&) = delete;
  ConditionalSuspendedSection& operator=(ConditionalSuspendedSection&&) = delete;

 private:
  velox::exec::SuspendedSection* section_ = nullptr;
};
} // namespace

int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  auto pool = memoryManager_->getAggregateMemoryPool();
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Spill[" + poolName + "]: "};
  int64_t shrunken = memoryManager_->shrink(size);
  // todo return the actual spilled size?
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    LOG(INFO) << logPrefix << "Trying to request spilling for " << remaining << " bytes...";
    // if we are on one of the driver of the spilled task, suspend it
    velox::exec::Driver* thisDriver = nullptr;
    task_->testingVisitDrivers([&](velox::exec::Driver* driver) {
      if (driver->isOnThread()) {
        thisDriver = driver;
      }
    });
    // suspend the driver when we are on it
    ConditionalSuspendedSection noCancel(thisDriver, thisDriver != nullptr);
    velox::exec::MemoryReclaimer::Stats status;
    auto* mm = memoryManager_->getMemoryManager();
    uint64_t spilledOut = mm->arbitrator()->shrinkMemory({pool}, remaining); // this conducts spilling
    LOG(INFO) << logPrefix << "Successfully spilled out " << spilledOut << " bytes.";
    uint64_t total = shrunken + spilledOut;
    VLOG(2) << logPrefix << "Successfully reclaimed total " << total << " bytes.";
    return total;
  } else {
    LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy << " was not configured.";
  }

  VLOG(2) << logPrefix << "Successfully reclaimed total " << shrunken << " bytes.";
  return shrunken;
}

void WholeStageResultIterator::getOrderedNodeIds(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    std::vector<velox::core::PlanNodeId>& nodeIds) {
  bool isProjectNode = (std::dynamic_pointer_cast<const velox::core::ProjectNode>(planNode) != nullptr);
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

  if (debugModeEnabled(confMap_)) {
    auto planWithStats = velox::exec::printPlanWithStats(*veloxPlan_.get(), task_->taskStats(), true);
    std::ostringstream oss;
    oss << "Native Plan with stats for: " << taskInfo_;
    oss << "\n" << planWithStats << std::endl;
    LOG(INFO) << oss.str();
  }

  auto planStats = velox::exec::toPlanStats(task_->taskStats());
  // Calculate the total number of metrics.
  int statsNum = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
        DEBUG_OUT << "Not found node id: " << nodeId << std::endl;
        DEBUG_OUT << "Plan Node: " << std::endl << veloxPlan_->toString(true, true) << std::endl;
        throw std::runtime_error("Node id cannot be found in plan status.");
      }
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      statsNum += 1;
      continue;
    }
    statsNum += planStats.at(nodeId).operatorStats.size();
  }

  metrics_ = std::make_unique<Metrics>(statsNum);

  int metricIndex = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      metricIndex += 1;
      continue;
    }

    const auto& status = planStats.at(nodeId);
    // Add each operator status into metrics.
    for (const auto& entry : status.operatorStats) {
      const auto& second = entry.second;
      metrics_->get(Metrics::kInputRows)[metricIndex] = second->inputRows;
      metrics_->get(Metrics::kInputVectors)[metricIndex] = second->inputVectors;
      metrics_->get(Metrics::kInputBytes)[metricIndex] = second->inputBytes;
      metrics_->get(Metrics::kRawInputRows)[metricIndex] = second->rawInputRows;
      metrics_->get(Metrics::kRawInputBytes)[metricIndex] = second->rawInputBytes;
      metrics_->get(Metrics::kOutputRows)[metricIndex] = second->outputRows;
      metrics_->get(Metrics::kOutputVectors)[metricIndex] = second->outputVectors;
      metrics_->get(Metrics::kOutputBytes)[metricIndex] = second->outputBytes;
      metrics_->get(Metrics::kCpuCount)[metricIndex] = second->cpuWallTiming.count;
      metrics_->get(Metrics::kWallNanos)[metricIndex] = second->cpuWallTiming.wallNanos;
      metrics_->get(Metrics::kPeakMemoryBytes)[metricIndex] = second->peakMemoryBytes;
      metrics_->get(Metrics::kNumMemoryAllocations)[metricIndex] = second->numMemoryAllocations;
      metrics_->get(Metrics::kSpilledBytes)[metricIndex] = second->spilledBytes;
      metrics_->get(Metrics::kSpilledRows)[metricIndex] = second->spilledRows;
      metrics_->get(Metrics::kSpilledPartitions)[metricIndex] = second->spilledPartitions;
      metrics_->get(Metrics::kSpilledFiles)[metricIndex] = second->spilledFiles;
      metrics_->get(Metrics::kNumDynamicFiltersProduced)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersProduced);
      metrics_->get(Metrics::kNumDynamicFiltersAccepted)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersAccepted);
      metrics_->get(Metrics::kNumReplacedWithDynamicFilterRows)[metricIndex] =
          runtimeMetric("sum", second->customStats, kReplacedWithDynamicFilterRows);
      metrics_->get(Metrics::kFlushRowCount)[metricIndex] = runtimeMetric("sum", second->customStats, kFlushRowCount);
      metrics_->get(Metrics::kScanTime)[metricIndex] = runtimeMetric("sum", second->customStats, kTotalScanTime);
      metrics_->get(Metrics::kSkippedSplits)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedSplits);
      metrics_->get(Metrics::kProcessedSplits)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedSplits);
      metrics_->get(Metrics::kSkippedStrides)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedStrides);
      metrics_->get(Metrics::kProcessedStrides)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedStrides);
      metricIndex += 1;
    }
  }
}

int64_t WholeStageResultIterator::runtimeMetric(
    const std::string& type,
    const std::unordered_map<std::string, velox::RuntimeMetric>& runtimeStats,
    const std::string& metricId) {
  if (runtimeStats.find(metricId) == runtimeStats.end()) {
    return 0;
  }

  if (type == "sum") {
    return runtimeStats.at(metricId).sum;
  } else if (type == "count") {
    return runtimeStats.at(metricId).count;
  } else if (type == "min") {
    return runtimeStats.at(metricId).min;
  } else if (type == "max") {
    return runtimeStats.at(metricId).max;
  } else {
    return 0;
  }
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
    // To align with Spark's behavior, unset to support non-ISO8601 standard strings.
    configs[velox::core::QueryConfig::kCastStringToDateIsIso8601] = std::to_string(false);
    auto defaultTimezone = getConfigValue(confMap_, kDefaultSessionTimezone, "");
    configs[velox::core::QueryConfig::kSessionTimezone] = getConfigValue(confMap_, kSessionTimezone, defaultTimezone);
    // Adjust timestamp according to the above configured session timezone.
    configs[velox::core::QueryConfig::kAdjustTimestampToTimezone] = std::to_string(true);
    // Align Velox size function with Spark.
    configs[velox::core::QueryConfig::kSparkLegacySizeOfNull] = getConfigValue(confMap_, kLegacySize, "true");

    {
      // partial aggregation memory config
      auto offHeapMemory = std::stol(
          getConfigValue(confMap_, kSparkTaskOffHeapMemory, std::to_string(facebook::velox::memory::kMaxMemory)));
      auto maxPartialAggregationMemory =
          (long)(std::stod(getConfigValue(confMap_, kMaxPartialAggregationMemoryRatio, "0.1")) * offHeapMemory);
      auto maxExtendedPartialAggregationMemory =
          (long)(std::stod(getConfigValue(confMap_, kMaxExtendedPartialAggregationMemoryRatio, "0.5")) * offHeapMemory);
      configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxPartialAggregationMemory);
      configs[velox::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
          std::to_string(maxExtendedPartialAggregationMemory);
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinPct] =
          getConfigValue(confMap_, kAbandonPartialAggregationMinPct, "90");
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinRows] =
          getConfigValue(confMap_, kAbandonPartialAggregationMinRows, "100000");
    }
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
    configs[velox::core::QueryConfig::kMaxSpillFileSize] =
        getConfigValue(confMap_, kMaxSpillFileSize, std::to_string(20L * 1024 * 1024));
    configs[velox::core::QueryConfig::kMinSpillRunSize] =
        getConfigValue(confMap_, kMinSpillRunSize, std::to_string(256 << 20));
    configs[velox::core::QueryConfig::kSpillStartPartitionBit] =
        getConfigValue(confMap_, kSpillStartPartitionBit, "29");
    configs[velox::core::QueryConfig::kJoinSpillPartitionBits] = getConfigValue(confMap_, kSpillPartitionBits, "2");
    configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
        getConfigValue(confMap_, kSpillableReservationGrowthPct, "25");
    configs[velox::core::QueryConfig::kSpillCompressionKind] = getConfigValue(confMap_, kSpillCompressionKind, "lz4");
    configs[velox::core::QueryConfig::kSparkBloomFilterExpectedNumItems] =
        getConfigValue(confMap_, kBloomFilterExpectedNumItems, "1000000");
    configs[velox::core::QueryConfig::kSparkBloomFilterNumBits] =
        getConfigValue(confMap_, kBloomFilterNumBits, "8388608");
    configs[velox::core::QueryConfig::kSparkBloomFilterMaxNumBits] =
        getConfigValue(confMap_, kBloomFilterMaxNumBits, "4194304");
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

#ifdef ENABLE_HDFS
void WholeStageResultIterator::updateHdfsTokens() {
  std::lock_guard lock{mutex};
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
    VeloxMemoryManager* memoryManager,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& scanNodeIds,
    const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo& taskInfo)
    : WholeStageResultIterator(memoryManager, planNode, confMap, taskInfo),
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
      fmt::format("Gluten {}", taskInfo_.toString()), std::move(planFragment), 0, std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  auto fileSystem = velox::filesystems::getFileSystem(spillDir, nullptr);
  GLUTEN_CHECK(fileSystem != nullptr, "File System for spilling is null!");
  fileSystem->mkdir(spillDir);
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
    VeloxMemoryManager* memoryManager,
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const std::vector<velox::core::PlanNodeId>& streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo& taskInfo)
    : WholeStageResultIterator(memoryManager, planNode, confMap, taskInfo), streamIds_(streamIds) {
  std::unordered_set<velox::core::PlanNodeId> emptySet;
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1, emptySet};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = createNewVeloxQueryCtx();

  task_ = velox::exec::Task::create(
      fmt::format("Gluten {}", taskInfo_.toString()), std::move(planFragment), 0, std::move(queryCtx));

  if (!task_->supportsSingleThreadedExecution()) {
    throw std::runtime_error("Task doesn't support single thread execution: " + planNode->toString());
  }
  auto fileSystem = velox::filesystems::getFileSystem(spillDir, nullptr);
  GLUTEN_CHECK(fileSystem != nullptr, "File System for spilling is null!");
  fileSystem->mkdir(spillDir);
  task_->setSpillDirectory(spillDir);
  addSplits_ = [&](velox::exec::Task* task) {
    if (noMoreSplits_) {
      return;
    }
    noMoreSplits_ = true;
  };
}

} // namespace gluten
