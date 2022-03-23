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

#include "arrow/c/abi.h"
#include "substrait_to_velox_expr.h"
#include "substrait_utils.h"

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

  /// This method is used to create a Project node as the parent of Aggregation node,
  /// in order to unify the plan node id in column names.
  std::shared_ptr<const core::PlanNode> createUnifyNode(
      const std::shared_ptr<const core::PlanNode>& aggNode, uint64_t groupingSize,
      uint64_t aggSize);
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
      const substrait::Plan& plan, std::vector<arrow::RecordBatchIterator> arrow_iters);

  void getIterInputSchema(
      const substrait::Plan& splan,
      std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>& schema_map);

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
  std::shared_ptr<ArrowArrayStream> arrowStreamIter_;

  /// This class is used to check if some of the input columns of Aggregation
  /// should be combined into a single column. Currently, this case occurs in
  /// final Average. The phase of Aggregation will also be set.
  bool needsRowConstruct(const substrait::AggregateRel& sagg,
                         core::AggregationNode::Step& aggStep);

  /// This class is used to convert AggregateRel into Velox plan node.
  /// This class will add a Project node before Aggregation to combine columns.
  /// A Project node will be added after Aggregation to unify the column names.
  std::shared_ptr<const core::PlanNode> toVeloxAggWithRowConstruct(
      const substrait::AggregateRel& sagg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// This class is used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const substrait::AggregateRel& sagg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /* Result Iterator */
  class WholeStageResIter;
  class WholeStageResIterFirstStage;
  class WholeStageResIterMiddleStage;

  void getIterInputSchemaFromRel(
      const substrait::Rel& srel,
      std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>& schema_map);
};

}  // namespace compute
}  // namespace gazellejni
