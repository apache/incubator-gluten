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

#include "VeloxPlanConverter.h"
#include <filesystem>

#include "arrow/c/bridge.h"
#include "compute/ResultIterator.h"
#include "config/GlutenConfig.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/common/file/FileSystems.h"

using namespace facebook;

namespace gluten {

VeloxPlanConverter::VeloxPlanConverter(
    std::vector<std::shared_ptr<ResultIterator>>& inputIters,
    const std::unordered_map<std::string, std::string>& confMap) {
  inputIters_ = inputIters;
  subVeloxPlanConverter_ = std::make_shared<SubstraitVeloxPlanConverter>(defaultLeafVeloxMemoryPool().get(), confMap);
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::FetchRel& fetchRel) {
  if (fetchRel.has_input()) {
    setInputPlanNode(fetchRel.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ExpandRel& sexpand) {
  if (sexpand.has_input()) {
    setInputPlanNode(sexpand.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::SortRel& ssort) {
  if (ssort.has_input()) {
    setInputPlanNode(ssort.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::WindowRel& swindow) {
  if (swindow.has_input()) {
    setInputPlanNode(swindow.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    setInputPlanNode(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::JoinRel& sjoin) {
  if (sjoin.has_left()) {
    setInputPlanNode(sjoin.left());
  } else {
    throw std::runtime_error("Left child expected");
  }

  if (sjoin.has_right()) {
    setInputPlanNode(sjoin.right());
  } else {
    throw std::runtime_error("Right child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->streamIsInput(sread);
  if (iterIdx == -1) {
    return;
  }
  if (inputIters_.size() == 0) {
    throw std::runtime_error("Invalid input iterator.");
  }
  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> subTypeList;
  if (sread.has_base_schema()) {
    const auto& baseSchema = sread.base_schema();
    // Input names is not used. Instead, new input/output names will be created
    // because the ValueStreamNode in Velox does not support name change.
    colNum = baseSchema.names().size();
    subTypeList = subParser_->parseNamedStruct(baseSchema);
  }

  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    auto colName = subParser_->makeNodeName(planNodeId_, idx);
    outNames.emplace_back(colName);
  }

  std::vector<velox::TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto vectorStream = std::make_shared<RowVectorStream>(std::move(inputIters_[iterIdx]), outputType);
  auto valuesNode = std::make_shared<ValueStreamNode>(nextPlanNodeId(), outputType, std::move(vectorStream));
  subVeloxPlanConverter_->insertInputNode(iterIdx, valuesNode, planNodeId_);
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    setInputPlanNode(srel.aggregate());
  } else if (srel.has_project()) {
    setInputPlanNode(srel.project());
  } else if (srel.has_filter()) {
    setInputPlanNode(srel.filter());
  } else if (srel.has_read()) {
    setInputPlanNode(srel.read());
  } else if (srel.has_join()) {
    setInputPlanNode(srel.join());
  } else if (srel.has_sort()) {
    setInputPlanNode(srel.sort());
  } else if (srel.has_expand()) {
    setInputPlanNode(srel.expand());
  } else if (srel.has_fetch()) {
    setInputPlanNode(srel.fetch());
  } else if (srel.has_window()) {
    setInputPlanNode(srel.window());
  } else {
    throw std::runtime_error("Rel is not supported: " + srel.DebugString());
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::RelRoot& sroot) {
  // Output names can be got from RelRoot, but are not used currently.
  if (sroot.has_input()) {
    setInputPlanNode(sroot.input());
  } else {
    throw std::runtime_error("Input is expected in RelRoot.");
  }
}

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    ::substrait::Plan& substraitPlan) {
  // In fact, only one RelRoot is expected here.
  for (auto& srel : substraitPlan.relations()) {
    if (srel.has_root()) {
      setInputPlanNode(srel.root());
    }
    if (srel.has_rel()) {
      setInputPlanNode(srel.rel());
    }
  }
  auto veloxPlan = subVeloxPlanConverter_->toVeloxPlan(substraitPlan);
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Plan Node: " << std::endl << veloxPlan->toString(true, true) << std::endl;
#endif
  return veloxPlan;
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

} // namespace gluten
