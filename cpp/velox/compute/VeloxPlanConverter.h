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

#include <velox/common/memory/MemoryPool.h>
#include "compute/ResultIterator.h"
#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"

namespace gluten {

// This class is used to convert the Substrait plan into Velox plan.
class VeloxPlanConverter {
 public:
  explicit VeloxPlanConverter(
      const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
      facebook::velox::memory::MemoryPool* veloxPool,
      const std::unordered_map<std::string, std::string>& confMap,
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      bool validationMode = false);

  std::shared_ptr<const facebook::velox::core::PlanNode> toVeloxPlan(
      const ::substrait::Plan& substraitPlan,
      std::vector<::substrait::ReadRel_LocalFiles> localFiles);

  const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() {
    return substraitVeloxPlanConverter_.splitInfos();
  }

 private:
  std::string nextPlanNodeId();

  int planNodeId_ = 0;

  bool validationMode_;

  SubstraitToVeloxPlanConverter substraitVeloxPlanConverter_;
};

} // namespace gluten
