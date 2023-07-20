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
#pragma once

#include "compute/Backend.h"
#include "memory/ColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "utils/metrics.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"

namespace gluten {

class WholeStageResultIterator : public ColumnarBatchIterator {
 public:
  WholeStageResultIterator(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::unordered_map<std::string, std::string>& confMap);

  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
  };

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

  std::shared_ptr<Metrics> getMetrics(int64_t exportNanos) {
    collectMetrics();
    metrics_->veloxToArrow = exportNanos;
    return metrics_;
  }

  std::shared_ptr<facebook::velox::Config> createConnectorConfig();

  std::shared_ptr<facebook::velox::exec::Task> task_;

  std::function<void(facebook::velox::exec::Task*)> addSplits_;

  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;

 protected:
  std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx();

  /// A map of custom configs.
  std::unordered_map<std::string, std::string> confMap_;

 private:
  /// Get the Spark confs to Velox query context.
  std::unordered_map<std::string, std::string> getQueryContextConf();

#ifdef ENABLE_HDFS
  /// Set latest tokens to global HiveConnector
  void updateHdfsTokens();
#endif

  /// Get all the children plan node ids with postorder traversal.
  void getOrderedNodeIds(
      const std::shared_ptr<const facebook::velox::core::PlanNode>&,
      std::vector<facebook::velox::core::PlanNodeId>& nodeIds);

  /// Collect Velox metrics.
  void collectMetrics();

  /// Return a certain type of runtime metric. Supported metric types are: sum, count, min, max.
  int64_t runtimeMetric(
      const std::string& metricType,
      const std::unordered_map<std::string, facebook::velox::RuntimeMetric>& runtimeStats,
      const std::string& metricId) const;

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  // spill
  std::string spillStrategy_;

  std::shared_ptr<Metrics> metrics_ = nullptr;

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be ommited in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;
};

class WholeStageResultIteratorFirstStage final : public WholeStageResultIterator {
 public:
  WholeStageResultIteratorFirstStage(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap,
      const SparkTaskInfo taskInfo);

 private:
  std::vector<facebook::velox::core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<SplitInfo>> scanInfos_;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
  std::vector<std::vector<facebook::velox::exec::Split>> splits_;
  bool noMoreSplits_ = false;

  void constructPartitionColumns(
      std::unordered_map<std::string, std::optional<std::string>>&,
      const std::unordered_map<std::string, std::string>&);
};

class WholeStageResultIteratorMiddleStage final : public WholeStageResultIterator {
 public:
  WholeStageResultIteratorMiddleStage(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap,
      const SparkTaskInfo taskInfo);

 private:
  bool noMoreSplits_ = false;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
};

} // namespace gluten
