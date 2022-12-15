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

#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "VeloxToRowConverter.h"
#include "arrow/c/abi.h"
#include "compute/Backend.h"
#include "include/arrow/c/bridge.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"
#include "utils/metrics.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#ifdef VELOX_ENABLE_HDFS
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#endif
#ifdef VELOX_ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#endif
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"

namespace gluten {

std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx(
    facebook::velox::memory::MemoryPool* memoryPool);

class VeloxInitializer {
 public:
  VeloxInitializer(std::unordered_map<std::string, std::string> conf) {
    Init(conf);
  }

  void Init(std::unordered_map<std::string, std::string> conf);
};

class WholeStageResIter {
 public:
  WholeStageResIter(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<const facebook::velox::core::PlanNode> planNode,
      const std::unordered_map<std::string, std::string>& confMap)
      : pool_(pool), planNode_(planNode), confMap_(confMap) {
    getOrderedNodeIds(planNode_, orderedNodeIds_);
  }

  virtual ~WholeStageResIter() = default;

  arrow::Result<std::shared_ptr<VeloxColumnarBatch>> Next();

  std::shared_ptr<Metrics> GetMetrics(int64_t exportNanos) {
    collectMetrics();
    metrics_->veloxToArrow = exportNanos;
    return metrics_;
  }

  facebook::velox::memory::MemoryPool* getPool() const;

  /// Set the Spark confs to Velox query context.
  void setConfToQueryContext(const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx);

  std::shared_ptr<facebook::velox::exec::Task> task_;

  std::function<void(facebook::velox::exec::Task*)> addSplits_;

  std::shared_ptr<const facebook::velox::core::PlanNode> planNode_;

 private:
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

  std::shared_ptr<Metrics> metrics_ = nullptr;
  int64_t metricVeloxToArrowNanos_ = 0;

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be ommited in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;

  /// A map of custome configs.
  std::unordered_map<std::string, std::string> confMap_;
};

// This class is used to convert the Substrait plan into Velox plan.
class VeloxBackend : public Backend {
 public:
  VeloxBackend(const std::unordered_map<std::string, std::string>& confMap) : Backend(confMap) {}

  std::shared_ptr<ResultIterator> GetResultIterator(
      MemoryAllocator* allocator,
      std::vector<std::shared_ptr<ResultIterator>> inputs = {}) override;

  // Used by unit test and benchmark.
  std::shared_ptr<ResultIterator> GetResultIterator(
      MemoryAllocator* allocator,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& scanInfos);

  arrow::Result<std::shared_ptr<ColumnarToRowConverter>> getColumnarConverter(
      MemoryAllocator* allocator,
      std::shared_ptr<ColumnarBatch> cb) override;

  /// Separate the scan ids and stream ids, and get the scan infos.
  void getInfoAndIds(
      std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<facebook::velox::substrait::SplitInfo>>
          splitInfoMap,
      std::unordered_set<facebook::velox::core::PlanNodeId> leafPlanNodeIds,
      std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& scanInfos,
      std::vector<facebook::velox::core::PlanNodeId>& scanIds,
      std::vector<facebook::velox::core::PlanNodeId>& streamIds);

  std::shared_ptr<Metrics> GetMetrics(void* raw_iter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResIter*>(raw_iter);
    return iter->GetMetrics(exportNanos);
  }

  std::shared_ptr<arrow::Schema> GetOutputSchema() override;

 private:
  void setInputPlanNode(const ::substrait::FetchRel& fetchRel);

  void setInputPlanNode(const ::substrait::ExpandRel& sExpand);

  void setInputPlanNode(const ::substrait::SortRel& sSort);

  void setInputPlanNode(const ::substrait::WindowRel& s);

  void setInputPlanNode(const ::substrait::AggregateRel& sagg);

  void setInputPlanNode(const ::substrait::ProjectRel& sproject);

  void setInputPlanNode(const ::substrait::FilterRel& sfilter);

  void setInputPlanNode(const ::substrait::JoinRel& sJoin);

  void setInputPlanNode(const ::substrait::ReadRel& sread);

  void setInputPlanNode(const ::substrait::Rel& srel);

  void setInputPlanNode(const ::substrait::RelRoot& sroot);

  std::shared_ptr<const facebook::velox::core::PlanNode> getVeloxPlanNode(const ::substrait::Plan& splan);

  std::string nextPlanNodeId();

  void cacheOutputSchema(const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode);

  /* Result Iterator */
  class WholeStageResIterFirstStage;

  class WholeStageResIterMiddleStage;

  int planNodeId_ = 0;

  std::vector<std::shared_ptr<ResultIterator>> arrowInputIters_;

  std::shared_ptr<facebook::velox::substrait::SubstraitParser> subParser_ =
      std::make_shared<facebook::velox::substrait::SubstraitParser>();

  std::shared_ptr<facebook::velox::substrait::SubstraitVeloxPlanConverter> subVeloxPlanConverter_ =
      std::make_shared<facebook::velox::substrait::SubstraitVeloxPlanConverter>(GetDefaultWrappedVeloxMemoryPool());

  // Cache for tests/benchmark purpose.
  std::shared_ptr<const facebook::velox::core::PlanNode> planNode_;
  std::shared_ptr<arrow::Schema> output_schema_;
};

} // namespace gluten
