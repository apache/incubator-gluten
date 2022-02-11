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

#include "type_utils.h"
#include "velox/buffer/Buffer.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

namespace gazellejni {
namespace compute {

VeloxInitializer::VeloxInitializer() {}

// The Init will be called per executor.
void VeloxInitializer::Init() {
  // Setup
  filesystems::registerLocalFileSystem();
  std::unique_ptr<folly::IOThreadPoolExecutor> executor =
      std::make_unique<folly::IOThreadPoolExecutor>(3);
  // auto hiveConnectorFactory = std::make_shared<hive::HiveConnectorFactory>();
  // registerConnectorFactory(hiveConnectorFactory);
  auto hiveConnector = getConnectorFactory("hive")->newConnector(
      "hive-connector", nullptr, nullptr, executor.get());
  registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  functions::prestosql::registerAllFunctions();
  aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
}

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
  auto input_types = child_node->outputType();
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> velox_grouping_exprs;
  auto& groupings = sagg.groupings();
  int input_plan_node_id = plan_node_id_ - 1;
  int out_idx = 0;
  for (auto& grouping : groupings) {
    auto grouping_exprs = grouping.grouping_expressions();
    for (auto& grouping_expr : grouping_exprs) {
      auto field_expr = expr_converter_->toVeloxExpr(grouping_expr, input_plan_node_id);
      // Velox's groupings are limited to be Field, and pre-projection for grouping cols
      // is not supported.
      auto typed_field_expr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(field_expr);
      velox_grouping_exprs.push_back(typed_field_expr);
      out_idx += 1;
    }
  }
  // Parse measures
  core::AggregationNode::Step agg_step;
  bool phase_inited = false;
  std::vector<std::shared_ptr<const core::CallTypedExpr>> agg_exprs;
  std::vector<std::shared_ptr<const core::ITypedExpr>> project_exprs;
  std::vector<std::string> project_out_names;
  for (auto& smea : sagg.measures()) {
    auto agg_function = smea.measure();
    if (!phase_inited) {
      switch (agg_function.phase()) {
        case substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
          agg_step = core::AggregationNode::Step::kPartial;
          break;
        case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
          agg_step = core::AggregationNode::Step::kIntermediate;
          break;
        case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
          agg_step = core::AggregationNode::Step::kFinal;
          break;
        default:
          throw new std::runtime_error("Aggregate phase is not supported.");
          break;
      }
      phase_inited = true;
    }
    auto func_id = agg_function.function_reference();
    auto sub_func_name = sub_parser_->findFunction(functions_map_, func_id);
    auto func_name = sub_parser_->substrait_velox_function_map[sub_func_name];
    std::vector<std::shared_ptr<const core::ITypedExpr>> agg_params;
    auto args = agg_function.args();
    for (auto arg : args) {
      switch (arg.rex_type_case()) {
        case substrait::Expression::RexTypeCase::kSelection: {
          auto sel = arg.selection();
          auto field_expr = expr_converter_->toVeloxExpr(sel, input_plan_node_id);
          agg_params.push_back(field_expr);
          break;
        }
        case substrait::Expression::RexTypeCase::kScalarFunction: {
          // Pre-projection is needed before Aggregate.
          auto sfunc = arg.scalar_function();
          auto velox_expr = expr_converter_->toVeloxExpr(sfunc, input_plan_node_id);
          project_exprs.push_back(velox_expr);
          auto col_out_name = sub_parser_->makeNodeName(plan_node_id_, out_idx);
          project_out_names.push_back(col_out_name);
          auto sub_type = sub_parser_->parseType(sfunc.output_type());
          auto velox_type = expr_converter_->getVeloxType(sub_type->type);
          auto agg_input_param = std::make_shared<const core::FieldAccessTypedExpr>(
              velox_type, col_out_name);
          agg_params.push_back(agg_input_param);
          break;
        }
        default:
          throw new std::runtime_error("Expression not supported");
          break;
      }
    }
    auto agg_out_type = agg_function.output_type();
    auto agg_velox_type =
        expr_converter_->getVeloxType(sub_parser_->parseType(agg_out_type)->type);
    auto agg_expr = std::make_shared<const core::CallTypedExpr>(
        agg_velox_type, std::move(agg_params), func_name);
    agg_exprs.push_back(agg_expr);
    out_idx += 1;
  }
  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(out_idx);
  if (project_out_names.size() > 0) {
    auto project_node = std::make_shared<core::ProjectNode>(
        nextPlanNodeId(), std::move(project_out_names), std::move(project_exprs),
        child_node);
    std::vector<std::string> agg_out_names;
    for (int idx = 0; idx < out_idx; idx++) {
      agg_out_names.push_back(sub_parser_->makeNodeName(plan_node_id_, idx));
    }
    auto agg_node = std::make_shared<core::AggregationNode>(
        nextPlanNodeId(), agg_step, velox_grouping_exprs, agg_out_names, agg_exprs,
        aggregateMasks, ignoreNullKeys, project_node);
    return agg_node;
  } else {
    std::vector<std::string> agg_out_names;
    for (int idx = 0; idx < out_idx; idx++) {
      agg_out_names.push_back(sub_parser_->makeNodeName(plan_node_id_, idx));
    }
    auto agg_node = std::make_shared<core::AggregationNode>(
        nextPlanNodeId(), agg_step, velox_grouping_exprs, agg_out_names, agg_exprs,
        aggregateMasks, ignoreNullKeys, child_node);
    return agg_node;
  }
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::ProjectRel& sproject) {
  std::shared_ptr<const core::PlanNode> child_node;
  if (sproject.has_input()) {
    child_node = toVeloxPlan(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
  // Expressions
  std::vector<std::string> project_names;
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  auto pre_plan_node_id = plan_node_id_ - 1;
  int col_idx = 0;
  for (auto& expr : sproject.expressions()) {
    auto velox_expr = expr_converter_->toVeloxExpr(expr, pre_plan_node_id);
    expressions.push_back(velox_expr);
    auto col_out_name = sub_parser_->makeNodeName(plan_node_id_, col_idx);
    project_names.push_back(col_out_name);
    col_idx += 1;
  }
  auto project_node = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(), std::move(project_names), std::move(expressions), child_node);
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
  /*
  if (sfilter.has_condition()) {
    ParseExpression(sfilter.condition());
  }
  for (auto& stype : sfilter.input_types()) {
    ParseType(stype);
  }
  */
  return child_node;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::ReadRel& sread, u_int32_t* index, std::vector<std::string>* paths,
    std::vector<u_int64_t>* starts, std::vector<u_int64_t>* lengths) {
  std::vector<std::string> col_name_list;
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> substrait_type_list;
  if (sread.has_base_schema()) {
    auto& base_schema = sread.base_schema();
    for (auto& name : base_schema.names()) {
      col_name_list.push_back(name);
    }
    auto type_list = sub_parser_->parseNamedStruct(base_schema);
    for (auto type : type_list) {
      substrait_type_list.push_back(type);
    }
  }
  // Parse local files
  if (sread.has_local_files()) {
    auto& local_files = sread.local_files();
    auto& files_list = local_files.items();
    for (auto& file : files_list) {
      // Expect all partions share the same index.
      (*index) = file.partition_index();
      (*paths).push_back(file.uri_file());
      (*starts).push_back(file.start());
      (*lengths).push_back(file.length());
    }
  }
  std::vector<TypePtr> velox_type_list;
  for (auto sub_type : substrait_type_list) {
    velox_type_list.push_back(expr_converter_->getVeloxType(sub_type->type));
  }
  // Note: Velox require Filter pushdown must being enabled.
  bool filter_pushdown_enabled = true;
  std::shared_ptr<hive::HiveTableHandle> table_handle;
  if (!sread.has_filter()) {
    std::cout << "no filter" << std::endl;
    table_handle = std::make_shared<hive::HiveTableHandle>(
        filter_pushdown_enabled, hive::SubfieldFilters{}, nullptr);
  } else {
    auto& sfilter = sread.filter();
    hive::SubfieldFilters filters =
        expr_converter_->toVeloxFilter(col_name_list, velox_type_list, sfilter);
    table_handle = std::make_shared<hive::HiveTableHandle>(filter_pushdown_enabled,
                                                           std::move(filters), nullptr);
  }
  std::vector<std::string> out_names;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
  for (int idx = 0; idx < col_name_list.size(); idx++) {
    auto out_name = sub_parser_->makeNodeName(plan_node_id_, idx);
    assignments[out_name] = std::make_shared<hive::HiveColumnHandle>(
        col_name_list[idx], hive::HiveColumnHandle::ColumnType::kRegular,
        velox_type_list[idx]);
    out_names.push_back(out_name);
  }
  auto output_type = ROW(std::move(out_names), std::move(velox_type_list));
  auto table_scan_node = std::make_shared<core::TableScanNode>(
      nextPlanNodeId(), output_type, table_handle, assignments);
  return table_scan_node;
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
    const substrait::RelRoot& sroot) {
  auto& snames = sroot.names();
  if (sroot.has_input()) {
    auto& srel = sroot.input();
    return toVeloxPlan(srel);
  } else {
    throw new std::runtime_error("Input is expected in RelRoot.");
  }
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const substrait::Plan& splan) {
  for (auto& sextension : splan.extensions()) {
    if (!sextension.has_extension_function()) {
      continue;
    }
    auto& sfmap = sextension.extension_function();
    auto id = sfmap.function_anchor();
    auto name = sfmap.name();
    functions_map_[id] = name;
  }
  expr_converter_ =
      std::make_shared<SubstraitVeloxExprConverter>(sub_parser_, functions_map_);
  std::shared_ptr<const core::PlanNode> plan_node;
  // In fact, only one RelRoot is expected here.
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      plan_node = toVeloxPlan(srel.root());
    }
    if (srel.has_rel()) {
      plan_node = toVeloxPlan(srel.rel());
    }
  }
  return plan_node;
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", plan_node_id_);
  plan_node_id_++;
  return id;
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>>
SubstraitVeloxPlanConverter::getResIter(
    const std::shared_ptr<const core::PlanNode>& plan_node) {
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

  /* This method converts Velox RowVector into Arrow RecordBatch based on Velox's
     Arrow conversion implementation, in which memcopy is not needed for fixed-width data
     types, but is conducted in String conversion. The output batch will be the input of
     Columnar Shuffle.
  */
  void toRealArrowBatch(const RowTypePtr& out_types,
                        std::shared_ptr<arrow::RecordBatch>* out) {
    uint32_t col_num = out_types->size();
    std::vector<std::shared_ptr<arrow::Array>> out_arrays;
    std::vector<std::shared_ptr<arrow::Field>> ret_types;
    for (uint32_t idx = 0; idx < col_num; idx++) {
      arrow::ArrayData out_data;
      out_data.length = num_rows_;
      auto vec = result_->childAt(idx);
      // FIXME: need to release this.
      ArrowArray arrowArray;
      exportToArrow(vec, arrowArray, velox_pool_.get());
      out_data.buffers.resize(arrowArray.n_buffers);
      out_data.null_count = arrowArray.null_count;
      // Validity buffer
      std::shared_ptr<arrow::Buffer> val_buffer = nullptr;
      if (arrowArray.null_count > 0) {
        arrowArray.buffers[0];
        // FIXME: set BitMap
      }
      out_data.buffers[0] = val_buffer;
      auto col_type = out_types->childAt(idx);
      auto col_arrow_type = toArrowType(col_type);
      ret_types.push_back(arrow::field("res", col_arrow_type));
      out_data.type = col_arrow_type;
      if (isPrimitive(col_type)) {
        auto data_buffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            bytesOfType(col_type) * num_rows_);
        out_data.buffers[1] = data_buffer;
      } else if (isString(col_type)) {
        auto value_buffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            arrowArray.string_data_size);
        auto offset_bytes = sizeof(int32_t);
        auto offset_buffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[2]),
            offset_bytes * (num_rows_ + 1));
        /* Velox:                     Arrow:
           buffer_1 -> value          buffer_1 -> offset
           buffer_2 -> offset         buffer_2 -> value
        */
        out_data.buffers[1] = offset_buffer;
        out_data.buffers[2] = value_buffer;
      }
      std::shared_ptr<arrow::Array> out_array =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      out_arrays.push_back(out_array);
      // int ref_count = vec->mutableValues(0)->refCount();
    }
    // auto typed_array = std::dynamic_pointer_cast<arrow::StringArray>(out_arrays[0]);
    // for (int i = 0; i < typed_array->length(); i++) {
    //   std::cout << "array val: " << typed_array->GetString(i) << std::endl;
    // }
    *out = arrow::RecordBatch::Make(arrow::schema(ret_types), num_rows_, out_arrays);
  }

  /* This method converts Velox RowVector into Faked Arrow RecordBatch. Velox's impl is
    used for fixed-width data types. For String conversion, a faked array is constructed.
    The output batch will be converted into Unsafe Row in Velox-to-Row converter.
  */
  void toFakedArrowBatch(const RowTypePtr& out_types,
                         std::shared_ptr<arrow::RecordBatch>* out) {
    uint32_t col_num = out_types->size();
    std::vector<std::shared_ptr<arrow::Array>> out_arrays;
    std::vector<std::shared_ptr<arrow::Field>> ret_types;
    for (uint32_t idx = 0; idx < col_num; idx++) {
      arrow::ArrayData out_data;
      out_data.length = num_rows_;
      auto vec = result_->childAt(idx);
      auto col_type = out_types->childAt(idx);
      auto col_arrow_type = toArrowType(col_type);
      ret_types.push_back(arrow::field("res", col_arrow_type));
      if (isPrimitive(col_type)) {
        out_data.type = col_arrow_type;
        // FIXME: need to release this.
        ArrowArray arrowArray;
        exportToArrow(vec, arrowArray, velox_pool_.get());
        out_data.buffers.resize(arrowArray.n_buffers);
        out_data.null_count = arrowArray.null_count;
        auto data_buffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            bytesOfType(col_type) * num_rows_);
        // Validity buffer
        std::shared_ptr<arrow::Buffer> val_buffer = nullptr;
        if (arrowArray.null_count > 0) {
          arrowArray.buffers[0];
          // FIXME: set BitMap
        }
        out_data.buffers[0] = val_buffer;
        out_data.buffers[1] = data_buffer;
      } else if (isString(col_type)) {
        // Will construct a faked String Array.
        out_data.buffers.resize(3);
        out_data.null_count = 0;
        out_data.type = arrow::utf8();
        auto str_values = vec->asFlatVector<StringView>()->rawValues();
        auto val_buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(str_values), 8 * num_rows_);
        out_data.buffers[0] = nullptr;
        out_data.buffers[1] = val_buffer;
        out_data.buffers[2] = val_buffer;
      }
      std::shared_ptr<arrow::Array> out_array =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      out_arrays.push_back(out_array);
      // int ref_count = vec->mutableValues(0)->refCount();
    }
    // auto typed_array = std::dynamic_pointer_cast<arrow::StringArray>(out_arrays[0]);
    // for (int i = 0; i < typed_array->length(); i++) {
    //   std::cout << "array val: " << typed_array->GetString(i) << std::endl;
    // }
    *out = arrow::RecordBatch::Make(arrow::schema(ret_types), num_rows_, out_arrays);
  }

  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
    // FIXME: only one-col case is considered
    auto out_types = plan_node_->outputType();
    toFakedArrowBatch(out_types, out);
    num_rows_ = 0;
    return arrow::Status::OK();
  }

 private:
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  std::unique_ptr<memory::MemoryPool> velox_pool_{memory::getDefaultScopedMemoryPool()};
  const std::shared_ptr<const core::PlanNode> plan_node_;
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

}  // namespace compute
}  // namespace gazellejni
