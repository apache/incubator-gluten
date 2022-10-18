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
#include "arrow/c/bridge.h"
#include "compute/exec_backend.h"
#include "memory/columnar_batch.h"
#include "memory/velox_memory_pool.h"
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
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
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

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace velox {
namespace compute {

std::shared_ptr<core::QueryCtx> createNewVeloxQueryCtx(
    memory::MemoryPool* memoryPool);

class VeloxInitializer {
 public:
  VeloxInitializer() {
    Init();
  }
  void Init();
};

class GlutenVeloxColumnarBatch : public gluten::memory::GlutenColumnarBatch {
 public:
  GlutenVeloxColumnarBatch(RowVectorPtr rowVector)
      : gluten::memory::GlutenColumnarBatch(
            rowVector->childrenSize(),
            rowVector->size()),
        rowVector_(rowVector) {}

  ~GlutenVeloxColumnarBatch() override;

  std::string GetType() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;
  std::shared_ptr<ArrowArray> exportArrowArray() override;

  RowVectorPtr getRowVector() const;
  RowVectorPtr getFlattenedRowVector();

 private:
  void EnsureFlattened();

  RowVectorPtr rowVector_;
  RowVectorPtr flattened_ = nullptr;
};

class WholeStageResIter {
 public:
  WholeStageResIter(
      std::shared_ptr<memory::MemoryPool> pool,
      std::shared_ptr<const core::PlanNode> planNode,
      const std::unordered_map<std::string, std::string>& confMap)
      : pool_(pool), planNode_(planNode), confMap_(confMap) {
    getOrderedNodeIds(planNode_, orderedNodeIds_);
  }

  virtual ~WholeStageResIter() {}

  arrow::Result<std::shared_ptr<GlutenVeloxColumnarBatch>> Next();

  std::shared_ptr<Metrics> GetMetrics(int64_t exportNanos) {
    collectMetrics();
    metrics_->veloxToArrow = exportNanos;
    return metrics_;
  }

  memory::MemoryPool* getPool() const;

  /// Set the Spark confs to Velox query context.
  void setConfToQueryContext(const std::shared_ptr<core::QueryCtx>& queryCtx);

  std::shared_ptr<exec::Task> task_;

  std::function<void(exec::Task*)> addSplits_;

  std::shared_ptr<const core::PlanNode> planNode_;

 private:
  /// Get all the children plan node ids with postorder traversal.
  void getOrderedNodeIds(
      const std::shared_ptr<const core::PlanNode>& planNode,
      std::vector<core::PlanNodeId>& nodeIds);

  /// Collect Velox metrics.
  void collectMetrics();

  /// Return the sum of one runtime metric.
  int64_t sumOfRuntimeMetric(
      const std::unordered_map<std::string, RuntimeMetric>& runtimeStats,
      const std::string& metricId) const;

  std::shared_ptr<memory::MemoryPool> pool_;

  std::shared_ptr<Metrics> metrics_ = nullptr;
  int64_t metricVeloxToArrowNanos_ = 0;

  /// All the children plan node ids with postorder traversal.
  std::vector<core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be ommited in metrics.
  std::unordered_set<core::PlanNodeId> omittedNodeIds_;

  /// A map of custome configs.
  std::unordered_map<std::string, std::string> confMap_;
};

// This class is used to convert the Substrait plan into Velox plan.
class VeloxPlanConverter : public gluten::ExecBackendBase {
 public:
  VeloxPlanConverter(
      const std::unordered_map<std::string, std::string>& confMap)
      : confMap_(confMap) {}

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator) override;

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator,
      std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs)
      override;

  // Used by unit test and benchmark.
  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator,
      const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
          scanInfos);

  arrow::Result<
      std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>>
  getColumnarConverter(
      gluten::memory::MemoryAllocator* allocator,
      std::shared_ptr<gluten::memory::GlutenColumnarBatch> cb) override {
    auto arrowPool = gluten::memory::AsWrappedArrowMemoryPool(allocator);
    auto veloxPool = gluten::memory::AsWrappedVeloxMemoryPool(allocator);
    std::shared_ptr<GlutenVeloxColumnarBatch> veloxBatch =
        std::dynamic_pointer_cast<GlutenVeloxColumnarBatch>(cb);
    if (veloxBatch != nullptr) {
      return std::make_shared<VeloxToRowConverter>(
          veloxBatch->getFlattenedRowVector(), arrowPool, veloxPool);
    }
    // If the child is not Velox output, use Arrow-to-Row conversion instead.
    std::shared_ptr<ArrowSchema> c_schema = cb->exportArrowSchema();
    std::shared_ptr<ArrowArray> c_array = cb->exportArrowArray();
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::RecordBatch> rb,
        arrow::ImportRecordBatch(c_array.get(), c_schema.get()));
    ArrowSchemaRelease(c_schema.get());
    ArrowArrayRelease(c_array.get());
    return std::make_shared<gluten::columnartorow::ArrowColumnarToRowConverter>(
        rb, arrowPool);
  }

  /// Separate the scan ids and stream ids, and get the scan infos.
  void getInfoAndIds(
      std::unordered_map<
          core::PlanNodeId,
          std::shared_ptr<facebook::velox::substrait::SplitInfo>> splitInfoMap,
      std::unordered_set<core::PlanNodeId> leafPlanNodeIds,
      std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>&
          scanInfos,
      std::vector<core::PlanNodeId>& scanIds,
      std::vector<core::PlanNodeId>& streamIds);

  std::shared_ptr<Metrics> GetMetrics(void* raw_iter, int64_t exportNanos)
      override {
    auto iter = static_cast<WholeStageResIter*>(raw_iter);
    return iter->GetMetrics(exportNanos);
  }

  std::shared_ptr<arrow::Schema> GetOutputSchema() override;

 private:
  void setInputPlanNode(const ::substrait::SortRel& sSort);

  void setInputPlanNode(const ::substrait::AggregateRel& sagg);

  void setInputPlanNode(const ::substrait::ProjectRel& sproject);

  void setInputPlanNode(const ::substrait::FilterRel& sfilter);

  void setInputPlanNode(const ::substrait::JoinRel& sJoin);

  void setInputPlanNode(const ::substrait::ReadRel& sread);

  void setInputPlanNode(const ::substrait::Rel& srel);

  void setInputPlanNode(const ::substrait::RelRoot& sroot);

  std::shared_ptr<const core::PlanNode> getVeloxPlanNode(
      const ::substrait::Plan& splan);

  std::string nextPlanNodeId();

  void cacheOutputSchema(const std::shared_ptr<const core::PlanNode>& planNode);

  /* Result Iterator */
  class WholeStageResIterFirstStage;

  class WholeStageResIterMiddleStage;

  int planNodeId_ = 0;

  std::unordered_map<std::string, std::string> confMap_;

  std::vector<std::shared_ptr<gluten::GlutenResultIterator>> arrowInputIters_;

  std::shared_ptr<facebook::velox::substrait::SubstraitParser> subParser_ =
      std::make_shared<facebook::velox::substrait::SubstraitParser>();

  std::shared_ptr<facebook::velox::substrait::SubstraitVeloxPlanConverter>
      subVeloxPlanConverter_ = std::make_shared<
          facebook::velox::substrait::SubstraitVeloxPlanConverter>(
          gluten::memory::GetDefaultWrappedVeloxMemoryPool().get());

  // Cache for tests/benchmark purpose.
  std::shared_ptr<const core::PlanNode> planNode_;
  std::shared_ptr<arrow::Schema> output_schema_;
};

} // namespace compute
} // namespace velox
