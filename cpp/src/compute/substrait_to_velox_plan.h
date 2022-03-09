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

#include "substrait_to_velox_expr.h"
#include "substrait_utils.h"
#include "velox/vector/arrow/Abi.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace gazellejni {
namespace compute {

class VeloxInitializer {
 public:
  VeloxInitializer();
  void Init();
};

// This class is used to convert the Substrait plan into Velox plan.
class SubstraitVeloxPlanConverter {
 public:
  SubstraitVeloxPlanConverter();

  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::AggregateRel& sagg,
      std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::ProjectRel& sproject,
      std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::FilterRel& sfilter,
      std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(const substrait::ReadRel& sread,
                                                    u_int32_t* index,
                                                    std::vector<std::string>* paths,
                                                    std::vector<u_int64_t>* starts,
                                                    std::vector<u_int64_t>* lengths);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::InputRel& sinput,
      std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::Rel& srel, std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::RelRoot& sroot,
      std::vector<arrow::RecordBatchIterator> arrow_iters);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const substrait::Plan& splan, std::vector<arrow::RecordBatchIterator> arrow_iters);

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter(
      const std::shared_ptr<substrait::Plan>& plan_ptr,
      std::vector<arrow::RecordBatchIterator> arrow_iters);

 private:
  int plan_node_id_ = 0;
  std::shared_ptr<SubstraitParser> sub_parser_;
  std::shared_ptr<SubstraitVeloxExprConverter> expr_converter_;
  std::unordered_map<uint64_t, std::string> functions_map_;
  bool fake_arrow_output_ = false;
  bool ds_as_input_ = true;
  u_int32_t partition_index_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  std::string nextPlanNodeId();
  struct ArrowArrayStream velox_array_stream_;
  /* Result Iterator */
  class WholeStageResIter;
  class WholeStageResIterFirstStage;
  class WholeStageResIterMiddleStage;
};

}  // namespace compute
}  // namespace gazellejni
