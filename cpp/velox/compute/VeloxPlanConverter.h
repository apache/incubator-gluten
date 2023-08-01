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

#include "compute/ResultIterator.h"
#include "memory/VeloxMemoryPool.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"

namespace gluten {
// This class is used to convert the Substrait plan into Velox plan.
class VeloxPlanConverter {
 public:
  explicit VeloxPlanConverter(
      std::vector<std::shared_ptr<ResultIterator>>& inputIters,
      const std::unordered_map<std::string, std::string>& confMap);

  std::shared_ptr<const facebook::velox::core::PlanNode> toVeloxPlan(::substrait::Plan& substraitPlan);

  const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() {
    return subVeloxPlanConverter_->splitInfos();
  }

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

  std::string nextPlanNodeId();

  int planNodeId_ = 0;
  std::vector<std::shared_ptr<ResultIterator>> inputIters_;

  std::shared_ptr<SubstraitParser> subParser_ = std::make_shared<SubstraitParser>();

  std::shared_ptr<SubstraitVeloxPlanConverter> subVeloxPlanConverter_;
};

} // namespace gluten
