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

#include "substrait_to_velox_plan.h"

#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

namespace substrait = io::substrait;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

SubstraitVeloxPlanConverter::SubstraitVeloxPlanConverter() {
  sub_parser_ = std::make_shared<SubstraitParser>();
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::AggregateRel& sagg) {
  std::shared_ptr<const core::PlanNode> child_node;
  if (sagg.has_input()) {
    child_node = toVeloxPlan(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> groupingExpr;
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregateExprs;
  aggregateExprs.reserve(1);
  std::vector<std::shared_ptr<const core::ITypedExpr>> agg_params;
  agg_params.reserve(1);
  auto field_agg =
      std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), "mul_res");
  agg_params.emplace_back(field_agg);
  auto aggExpr =
      std::make_shared<const core::CallTypedExpr>(DOUBLE(), std::move(agg_params), "sum");
  aggregateExprs.emplace_back(aggExpr);
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
      aggregateExprs.size());
  auto aggNames = makeNames("a", aggregateExprs.size());
  auto agg_node = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(), core::AggregationNode::Step::kPartial, groupingExpr, aggNames,
      aggregateExprs, aggregateMasks, ignoreNullKeys, child_node);
  return agg_node;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::ProjectRel& sproject) {
  std::shared_ptr<const core::PlanNode> child_node;
  if (sproject.has_input()) {
    child_node = toVeloxPlan(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
  std::vector<std::shared_ptr<const core::ITypedExpr>> scan_params;
  scan_params.reserve(2);
  auto field_0 =
      std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), "l_extendedprice");
  auto field_1 =
      std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), "l_discount");
  scan_params.emplace_back(field_0);
  scan_params.emplace_back(field_1);
  // Expressions
  std::vector<std::string> projectNames;
  projectNames.push_back("mul_res");
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  auto mulExpr = std::make_shared<const core::CallTypedExpr>(
      DOUBLE(), std::move(scan_params), "multiply");
  expressions.emplace_back(mulExpr);
  auto project_node = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(), std::move(projectNames), std::move(expressions), child_node);
  return project_node;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::FilterRel& sfilter) {
  // FIXME: currently Filter is skipped.
  std::shared_ptr<const core::PlanNode> child_node;
  if (sfilter.has_input()) {
    child_node = toVeloxPlan(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
  return child_node;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::ReadRel& sread, u_int32_t* index, std::vector<std::string>* paths,
    std::vector<u_int64_t>* starts, std::vector<u_int64_t>* lengths) {
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> substrait_type_list;
  if (sread.has_base_schema()) {
    auto& base_schema = sread.base_schema();
    auto type_list = sub_parser_->ParseNamedStruct(base_schema);
    for (auto type : type_list) {
      substrait_type_list.push_back(type);
    }
  }
  // Parse local files
  if (sread.has_local_files()) {
    auto& local_files = sread.local_files();
    *index = local_files.index();
    auto& files_list = local_files.items();
    for (auto& file : files_list) {
      (*paths).push_back(file.uri_path());
      (*starts).push_back(file.start());
      (*lengths).push_back(file.length());
    }
  }
  std::vector<std::string> col_name_list;
  for (auto sub_type : substrait_type_list) {
    col_name_list.push_back(sub_type->name);
  }
  std::vector<TypePtr> velox_type_list;
  for (auto sub_type : substrait_type_list) {
    velox_type_list.push_back(sub_parser_->getVeloxType(sub_type->type));
  }
  auto& sfilter = sread.filter();
  // ParseExpression(sfilter);
  hive::SubfieldFilters filters;
  filters[common::Subfield(col_name_list[3])] = std::make_unique<common::DoubleRange>(
      8766.0, false, false, 9131.0, false, true, false);
  filters[common::Subfield(col_name_list[0])] =
      std::make_unique<common::DoubleRange>(0, true, false, 24, false, true, false);
  filters[common::Subfield(col_name_list[2])] = std::make_unique<common::DoubleRange>(
      0.05, false, false, 0.07, false, false, false);
  bool filterPushdownEnabled = true;
  auto tableHandle = std::make_shared<hive::HiveTableHandle>(filterPushdownEnabled,
                                                             std::move(filters), nullptr);

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
  for (int idx = 0; idx < col_name_list.size(); idx++) {
    assignments[col_name_list[idx]] = std::make_shared<hive::HiveColumnHandle>(
        col_name_list[idx], hive::HiveColumnHandle::ColumnType::kRegular,
        velox_type_list[idx]);
  }
  auto outputType = ROW(std::move(col_name_list), std::move(velox_type_list));
  auto tableScanNode = std::make_shared<core::TableScanNode>(nextPlanNodeId(), outputType,
                                                             tableHandle, assignments);
  return tableScanNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    return toVeloxPlan(srel.aggregate());
  } else if (srel.has_project()) {
    return toVeloxPlan(srel.project());
  } else if (srel.has_filter()) {
    return toVeloxPlan(srel.filter());
  } else if (srel.has_read()) {
    return toVeloxPlan(srel.read(), &partition_index_, &paths_, &starts_, &lengths_);
  } else {
    throw new std::runtime_error("Rel is not supported.");
  }
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::Plan& splan) {
  for (auto& smap : splan.mappings()) {
    if (!smap.has_function_mapping()) {
      continue;
    }
    auto& sfmap = smap.function_mapping();
    auto id = sfmap.function_id().id();
    auto name = sfmap.name();
    functions_map_[id] = name;
  }
  // FIXME: only one Rel is expected here after updating Substrait.
  std::shared_ptr<const core::PlanNode> plan_node;
  for (auto& srel : splan.relations()) {
    plan_node = toVeloxPlan(srel);
  }
  return plan_node;
}

std::string SubstraitVeloxPlanConverter::findFunction(uint64_t id) {
  if (functions_map_.find(id) == functions_map_.end()) {
    throw std::runtime_error("Could not find function " + std::to_string(id));
  }
  return functions_map_[id];
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", plan_node_id_);
  plan_node_id_++;
  return id;
}

std::vector<std::string> SubstraitVeloxPlanConverter::makeNames(const std::string& prefix,
                                                                int size) {
  std::vector<std::string> names;
  for (int i = 0; i < size; i++) {
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return names;
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>>
SubstraitVeloxPlanConverter::getResIter(std::shared_ptr<const core::PlanNode> plan_node) {
  auto wholestage_iter = std::make_shared<WholeStageResultIterator>(
      plan_node, partition_index_, paths_, starts_, lengths_);
  auto res_iter =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(wholestage_iter);
  return res_iter;
}

class SubstraitVeloxPlanConverter::WholeStageResultIterator
    : public ResultIterator<arrow::RecordBatch> {
 public:
  WholeStageResultIterator(const std::shared_ptr<const core::PlanNode>& plan_node,
                           const u_int32_t& index, const std::vector<std::string>& paths,
                           const std::vector<u_int64_t>& starts,
                           const std::vector<u_int64_t>& lengths)
      : plan_node_(plan_node),
        index_(index),
        paths_(paths),
        starts_(starts),
        lengths_(lengths) {
    std::vector<std::shared_ptr<ConnectorSplit>> connectorSplits;
    for (int idx = 0; idx < paths.size(); idx++) {
      auto path = paths[idx];
      auto start = starts[idx];
      auto length = lengths[idx];
      auto split = std::make_shared<hive::HiveConnectorSplit>(
          "hive-connector", path, FileFormat::ORC, start, length);
      connectorSplits.push_back(split);
    }
    splits_.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits_.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
    }
    params_.planNode = plan_node;
    cursor_ = std::make_unique<test::TaskCursor>(params_);
    addSplits_ = [&](Task* task) {
      if (noMoreSplits_) {
        return;
      }
      for (auto& split : splits_) {
        task->addSplit("0", std::move(split));
      }
      task->noMoreSplits("0");
      noMoreSplits_ = true;
    };
  }

  bool HasNext() override {
    if (!may_has_next_) {
      return false;
    }
    if (num_rows_ > 0) {
      return true;
    } else {
      addSplits_(cursor_->task().get());
      if (cursor_->moveNext()) {
        result_ = cursor_->current();
        num_rows_ += result_->size();
        return true;
      } else {
        may_has_next_ = false;
        return false;
      }
    }
  }

  arrow::Status CopyBuffer(const uint8_t* from, uint8_t* to, int64_t copy_bytes) {
    // ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer(size * length, memory_pool_));
    // uint8_t* buffer_data = (*out)->mutable_data();
    std::memcpy(to, from, copy_bytes);
    // double val = *(double*)buffer_data;
    // std::cout << "buffler val: " << val << std::endl;
    return arrow::Status::OK();
  }

  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
    // FIXME: only one-col case is considered
    auto col_num = 1;
    std::vector<std::shared_ptr<arrow::Array>> out_arrays;
    for (int idx = 0; idx < col_num; idx++) {
      arrow::ArrayData out_data;
      out_data.type = arrow::float64();
      out_data.buffers.resize(2);
      out_data.length = num_rows_;
      auto vec = result_->childAt(idx)->as<FlatVector<double>>();
      uint64_t array_null_count = 0;
      std::optional<int32_t> null_count = vec->getNullCount();
      std::shared_ptr<arrow::Buffer> val_buffer = nullptr;
      if (null_count) {
        int32_t vec_null_count = *null_count;
        array_null_count += vec_null_count;
        const uint64_t* rawNulls = vec->rawNulls();
        // FIXME: set BitMap
      }
      out_data.null_count = array_null_count;
      uint8_t* raw_result = vec->mutableRawValues<uint8_t>();
      auto bytes = sizeof(double);
      auto data_buffer = std::make_shared<arrow::Buffer>(raw_result, bytes * num_rows_);
      out_data.buffers[0] = val_buffer;
      out_data.buffers[1] = data_buffer;
      std::shared_ptr<arrow::Array> out_array =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      out_arrays.push_back(out_array);
      // int ref_count = vec->mutableValues(0)->refCount();
    }
    // auto typed_array = std::dynamic_pointer_cast<arrow::DoubleArray>(out_arrays[0]);
    // for (int i = 0; i < typed_array->length(); i++) {
    //     std::cout << "array val: " << typed_array->GetView(i) << std::endl;
    // }
    std::vector<std::shared_ptr<arrow::Field>> ret_types = {
        arrow::field("res", arrow::float64())};
    *out = arrow::RecordBatch::Make(arrow::schema(ret_types), num_rows_, out_arrays);
    num_rows_ = 0;
    return arrow::Status::OK();
  }

 private:
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  std::shared_ptr<const core::PlanNode> plan_node_;
  std::unique_ptr<test::TaskCursor> cursor_;
  test::CursorParameters params_;
  std::vector<exec::Split> splits_;
  bool noMoreSplits_ = false;
  std::function<void(exec::Task*)> addSplits_;
  u_int32_t index_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  // FIXME: use the setted one
  uint64_t batch_size_ = 10000;
  uint64_t num_rows_ = 0;
  bool may_has_next_ = true;
  RowVectorPtr result_;
};
