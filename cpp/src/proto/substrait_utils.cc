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

#include "substrait_utils.h"

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

SubstraitParser::SubstraitParser() {
  // if (!initialized) {
  //   initialized = true;
  // }
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> SubstraitParser::getResIter() {
  auto wholestage_iter = std::make_shared<WholeStageResultIterator>(
      plan_node_, partition_index_, paths_, starts_, lengths_);
  auto res_iter =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(wholestage_iter);
  return res_iter;
}

void SubstraitParser::ParseLiteral(const substrait::Expression::Literal& slit) {
  switch (slit.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      double val = slit.fp64();
      // std::cout << "double lit: " << val << std::endl;
      break;
    }
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      bool val = slit.boolean();
      break;
    }
    default:
      std::cout << "not supported" << std::endl;
      break;
  }
}

void SubstraitParser::ParseScalarFunction(
    const substrait::Expression::ScalarFunction& sfunc) {
  for (auto& sarg : sfunc.args()) {
    ParseExpression(sarg);
  }
  auto function_id = sfunc.id().id();
  auto function_name = findFunction(function_id);
  // std::cout << "function_name: " << function_name << std::endl;
  auto out_type = sfunc.output_type();
  ParseType(out_type);
}

void SubstraitParser::ParseReferenceSegment(const ::substrait::ReferenceSegment& sref) {
  switch (sref.reference_type_case()) {
    case substrait::ReferenceSegment::ReferenceTypeCase::kStructField: {
      auto sfield = sref.struct_field();
      auto field_id = sfield.field();
      // std::cout << "field_id: " << field_id << std::endl;
      break;
    }
    default:
      std::cout << "not supported" << std::endl;
      break;
  }
}

void SubstraitParser::ParseFieldReference(const substrait::FieldReference& sfield) {
  switch (sfield.reference_type_case()) {
    case substrait::FieldReference::ReferenceTypeCase::kDirectReference: {
      auto dref = sfield.direct_reference();
      ParseReferenceSegment(dref);
      break;
    }
    case substrait::FieldReference::ReferenceTypeCase::kMaskedReference: {
      // std::cout << "not supported" << std::endl;
      break;
    }
    default:
      std::cout << "not supported" << std::endl;
      break;
  }
}

void SubstraitParser::ParseExpression(const substrait::Expression& sexpr) {
  switch (sexpr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kLiteral: {
      auto slit = sexpr.literal();
      ParseLiteral(slit);
      break;
    }
    case substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sfunc = sexpr.scalar_function();
      ParseScalarFunction(sfunc);
      break;
    }
    case substrait::Expression::RexTypeCase::kSelection: {
      auto sel = sexpr.selection();
      ParseFieldReference(sel);
      break;
    }
    default:
      std::cout << "Expression not supported" << std::endl;
      break;
  }
}

std::shared_ptr<SubstraitParser::SubstraitType> SubstraitParser::ParseType(
    const substrait::Type& stype) {
  std::shared_ptr<SubstraitParser::SubstraitType> substrait_type;
  switch (stype.kind_case()) {
    case substrait::Type::KindCase::kBool: {
      auto sbool = stype.bool_();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "BOOL", sbool.variation().name(), sbool.nullability());
      break;
    }
    case substrait::Type::KindCase::kFp64: {
      auto sfp64 = stype.fp64();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "FP64", sfp64.variation().name(), sfp64.nullability());
      break;
    }
    case substrait::Type::KindCase::kStruct: {
      // TODO
      auto sstruct = stype.struct_();
      auto stypes = sstruct.types();
      for (auto& type : stypes) {
        ParseType(type);
      }
      break;
    }
    case substrait::Type::KindCase::kString: {
      auto sstring = stype.string();
      auto nullable = sstring.nullability();
      auto name = sstring.variation().name();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "STRING", sstring.variation().name(), sstring.nullability());
      break;
    }
    default:
      std::cout << "Type not supported" << std::endl;
      break;
  }
  return substrait_type;
}

std::vector<std::shared_ptr<SubstraitParser::SubstraitType>>
SubstraitParser::ParseNamedStruct(const substrait::Type::NamedStruct& named_struct) {
  auto& snames = named_struct.names();
  std::vector<std::string> name_list;
  for (auto& sname : snames) {
    name_list.push_back(sname);
  }
  // Parse Struct
  auto& sstruct = named_struct.struct_();
  auto& stypes = sstruct.types();
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> substrait_type_list;
  for (auto& type : stypes) {
    auto substrait_type = ParseType(type);
    substrait_type_list.push_back(substrait_type);
  }
  return substrait_type_list;
}

void SubstraitParser::ParseAggregateRel(const substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    ParseRel(sagg.input());
  }
  // Parse groupings
  auto& groupings = sagg.groupings();
  for (auto& grouping : groupings) {
    auto grouping_fields = grouping.input_fields();
    for (auto& grouping_field : grouping_fields) {
      // std::cout << "Agg grouping_field: " << grouping_field << std::endl;
    }
  }
  // Parse measures
  bool is_partial = false;
  for (auto& smea : sagg.measures()) {
    auto aggFunction = smea.measure();
    switch (aggFunction.phase()) {
      case substrait::Expression_AggregationPhase::
          Expression_AggregationPhase_INITIAL_TO_INTERMEDIATE:
        is_partial = true;
        break;
      default:
        break;
    }
    auto function_id = aggFunction.id().id();
    // std::cout << "Agg Function id: " << function_id << std::endl;
    auto args = aggFunction.args();
    for (auto arg : args) {
      ParseExpression(arg);
    }
  }
  auto agg_phase = sagg.phase();
  // Parse Input and Output types
  // std::cout << "Agg input and output:" << std::endl;
  for (auto& stype : sagg.input_types()) {
    ParseType(stype);
  }
  for (auto& stype : sagg.output_types()) {
    ParseType(stype);
  }
  if (is_partial) {
    bool ignoreNullKeys = false;
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> groupingExpr;
    std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregateExprs;
    aggregateExprs.reserve(1);
    std::vector<std::shared_ptr<const core::ITypedExpr>> agg_params;
    agg_params.reserve(1);
    auto field_agg =
        std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), "mul_res");
    agg_params.emplace_back(field_agg);
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(
        DOUBLE(), std::move(agg_params), "sum");
    aggregateExprs.emplace_back(aggExpr);
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
        aggregateExprs.size());
    auto aggNames = makeNames("a", aggregateExprs.size());
    plan_node_ = std::make_shared<core::AggregationNode>(
        nextPlanNodeId(), core::AggregationNode::Step::kPartial, groupingExpr, aggNames,
        aggregateExprs, aggregateMasks, ignoreNullKeys, plan_node_);
  }
}

void SubstraitParser::ParseProjectRel(const substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    ParseRel(sproject.input());
  }
  for (auto& stype : sproject.input_types()) {
    ParseType(stype);
  }
  for (auto& expr : sproject.expressions()) {
    ParseExpression(expr);
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
  plan_node_ = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(), std::move(projectNames), std::move(expressions), plan_node_);
}

void SubstraitParser::ParseFilterRel(const substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    ParseRel(sfilter.input());
  }
  if (sfilter.has_condition()) {
    ParseExpression(sfilter.condition());
  }
  for (auto& stype : sfilter.input_types()) {
    ParseType(stype);
  }
}

void SubstraitParser::ParseReadRel(const substrait::ReadRel& sread, u_int32_t* index,
                                   std::vector<std::string>* paths,
                                   std::vector<u_int64_t>* starts,
                                   std::vector<u_int64_t>* lengths) {
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> substrait_type_list;
  if (sread.has_base_schema()) {
    auto& base_schema = sread.base_schema();
    auto type_list = ParseNamedStruct(base_schema);
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
    velox_type_list.push_back(getVeloxType(sub_type->type));
  }
  auto& sfilter = sread.filter();
  // std::cout << "filter pushdown: " << std::endl;
  ParseExpression(sfilter);
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

  plan_node_ = std::make_shared<core::TableScanNode>(nextPlanNodeId(), outputType,
                                                     tableHandle, assignments);
}

void SubstraitParser::ParseRel(const substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    ParseAggregateRel(srel.aggregate());
  } else if (srel.has_project()) {
    ParseProjectRel(srel.project());
  } else if (srel.has_filter()) {
    ParseFilterRel(srel.filter());
  } else if (srel.has_read()) {
    ParseReadRel(srel.read(), &partition_index_, &paths_, &starts_, &lengths_);
  } else {
    std::cout << "not supported" << std::endl;
  }
}

void SubstraitParser::ParsePlan(const substrait::Plan& splan) {
  for (auto& smap : splan.mappings()) {
    if (!smap.has_function_mapping()) {
      continue;
    }
    auto& sfmap = smap.function_mapping();
    auto id = sfmap.function_id().id();
    auto name = sfmap.name();
    functions_map_[id] = name;
    // std::cout << "Function id: " << id << ", name: " << name << std::endl;
  }
  for (auto& srel : splan.relations()) {
    ParseRel(srel);
  }
}

std::string SubstraitParser::findFunction(uint64_t id) {
  if (functions_map_.find(id) == functions_map_.end()) {
    throw std::runtime_error("Could not find function " + std::to_string(id));
  }
  return functions_map_[id];
}

TypePtr SubstraitParser::getVeloxType(std::string type_name) {
  if (type_name == "BOOL") {
    return BOOLEAN();
  } else if (type_name == "FP64") {
    return DOUBLE();
  } else {
    throw std::runtime_error("not supported");
  }
}

std::string SubstraitParser::nextPlanNodeId() {
  auto id = fmt::format("{}", plan_node_id_);
  plan_node_id_++;
  return id;
}

std::vector<std::string> SubstraitParser::makeNames(const std::string& prefix, int size) {
  std::vector<std::string> names;
  for (int i = 0; i < size; i++) {
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return names;
}

class SubstraitParser::WholeStageResultIterator
    : public ResultIterator<arrow::RecordBatch> {
 public:
  WholeStageResultIterator(const std::shared_ptr<core::PlanNode>& plan_node,
                           u_int32_t index, std::vector<std::string> paths,
                           std::vector<u_int64_t> starts, std::vector<u_int64_t> lengths)
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
  std::shared_ptr<core::PlanNode> plan_node_;
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
