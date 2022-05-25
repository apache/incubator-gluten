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
#include "jni/exec_backend.h"
#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"
#include "utils/result_iterator.h"
#include "velox/common/caching/DataCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/common/CachedBufferedInput.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
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

class VeloxInitializer {
 public:
  VeloxInitializer() {
    Init();
  }
  void Init();
};

// This class is used to convert the Substrait plan into Velox plan.
class VeloxPlanConverter : public gluten::ExecBackendBase {
 public:
  std::shared_ptr<gluten::RecordBatchResultIterator> GetResultIterator() override;

  std::shared_ptr<gluten::RecordBatchResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputs) override;

  std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase> getColumnarConverter(
      std::shared_ptr<arrow::RecordBatch> rb, arrow::MemoryPool* memory_pool,
      bool wsChild) override {
    if (wsChild) {
      return std::make_shared<VeloxToRowConverter>(rb, memory_pool);
    } else {
      // If the child is not Velox output, use Arrow-to-Row conversion instead.
      return std::make_shared<gluten::columnartorow::ArrowColumnarToRowConverter>(
          rb, memory_pool);
    }
  }

 private:
  int planNodeId_ = 0;
  bool fakeArrowOutput_ = false;
  bool dsAsInput_ = true;
  u_int32_t partitionIndex_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> arrowInputIters_;
  std::shared_ptr<ArrowArrayStream> arrowStreamIter_;

  void setInputPlanNode(const ::substrait::AggregateRel& sagg);
  void setInputPlanNode(const ::substrait::ProjectRel& sproject);
  void setInputPlanNode(const ::substrait::FilterRel& sfilter);
  void setInputPlanNode(const ::substrait::JoinRel& sJoin);
  void setInputPlanNode(const ::substrait::ReadRel& sread);
  void setInputPlanNode(const ::substrait::Rel& srel);
  void setInputPlanNode(const ::substrait::RelRoot& sroot);

  std::shared_ptr<const core::PlanNode> getVeloxPlanNode(const ::substrait::Plan& splan);

  std::string nextPlanNodeId();
  std::shared_ptr<facebook::velox::substrait::SubstraitParser> subParser_ =
      std::make_shared<facebook::velox::substrait::SubstraitParser>();
  std::shared_ptr<facebook::velox::substrait::SubstraitVeloxPlanConverter>
      subVeloxPlanConverter_ =
          std::make_shared<facebook::velox::substrait::SubstraitVeloxPlanConverter>();

  /* Result Iterator */
  class WholeStageResIter;
  class WholeStageResIterFirstStage;
  class WholeStageResIterMiddleStage;
};

}  // namespace compute
}  // namespace velox
