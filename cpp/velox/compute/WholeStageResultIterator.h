#pragma once

#include "memory/VeloxColumnarBatch.h"
#include "substrait/plan.pb.h"
#include "utils/metrics.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"

namespace gluten {

static const std::string kHiveConnectorId = "test-hive";

class WholeStageResultIterator {
 public:
  WholeStageResultIterator(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> resultLeafPool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::unordered_map<std::string, std::string>& confMap)
      : veloxPlan_(planNode), confMap_(confMap), pool_(pool), resultLeafPool_(resultLeafPool) {
    getOrderedNodeIds(veloxPlan_, orderedNodeIds_);
  }

  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
  };

  arrow::Result<std::shared_ptr<VeloxColumnarBatch>> Next();

  std::shared_ptr<Metrics> GetMetrics(int64_t exportNanos) {
    collectMetrics();
    metrics_->veloxToArrow = exportNanos;
    return metrics_;
  }

  facebook::velox::memory::MemoryPool* getPool() const {
    return pool_.get();
  }

  std::shared_ptr<facebook::velox::Config> createConnectorConfig();

  std::shared_ptr<facebook::velox::exec::Task> task_;

  std::function<void(facebook::velox::exec::Task*)> addSplits_;

  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;

 protected:
  /// Get config value by key.
  std::string getConfigValue(const std::string& key, const std::optional<std::string>& fallbackValue = std::nullopt);

  std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx();

 private:
  /// Set the Spark confs to Velox query context.
  void setConfToQueryContext(const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx);

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

  /// A map of custom configs.
  std::unordered_map<std::string, std::string> confMap_;

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> resultLeafPool_;

  std::shared_ptr<Metrics> metrics_ = nullptr;
  int64_t metricVeloxToArrowNanos_ = 0;

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be ommited in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;
};

class WholeStageResultIteratorFirstStage final : public WholeStageResultIterator {
 public:
  WholeStageResultIteratorFirstStage(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> resultLeafPool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& scanInfos,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap);

 private:
  std::vector<facebook::velox::core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos_;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
  std::vector<std::vector<facebook::velox::exec::Split>> splits_;
  bool noMoreSplits_ = false;

  // Extract the partition column and value from a path of split.
  // The split path is like .../my_dataset/year=2022/month=July/split_file.
  std::unordered_map<std::string, std::optional<std::string>> extractPartitionColumnAndValue(
      const std::string& filePath);
};

class WholeStageResultIteratorMiddleStage final : public WholeStageResultIterator {
 public:
  WholeStageResultIteratorMiddleStage(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> resultLeafPool,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap);

 private:
  bool noMoreSplits_ = false;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
};

} // namespace gluten
