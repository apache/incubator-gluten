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
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

SubstraitParser::SubstraitParser() {
  // std::cout << "construct SubstraitParser" << std::endl;
  if (!initialized) {
    initialized = true;
    // Setup
    filesystems::registerLocalFileSystem();
    std::unique_ptr<folly::IOThreadPoolExecutor> executor =
        std::make_unique<folly::IOThreadPoolExecutor>(3);
    // auto hiveConnectorFactory = std::make_shared<hive::HiveConnectorFactory>();
    // registerConnectorFactory(hiveConnectorFactory);
    auto hiveConnector = getConnectorFactory("hive")->newConnector(
        facebook::velox::exec::test::kHiveConnectorId, nullptr, nullptr, executor.get());
    registerConnector(hiveConnector);
    dwrf::registerDwrfReaderFactory();
    // Register Velox functions
    functions::prestosql::registerAllFunctions();
    aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
  }
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> SubstraitParser::getResIter() {
  auto wholestage_iter = std::make_shared<WholeStageResultIterator>(
      plan_builder_, partition_index_, paths_, starts_, lengths_);
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
  auto function_name = FindFunction(function_id);
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

void SubstraitParser::ParseAggregateRel(const substrait::AggregateRel& sagg,
                                        std::shared_ptr<PlanBuilder>* plan_builder) {
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
    (*plan_builder) = std::make_shared<PlanBuilder>(
        (*plan_builder)
            ->aggregation({}, {"sum(mul_res)"}, {}, core::AggregationNode::Step::kPartial,
                          false));
  }
}

void SubstraitParser::ParseProjectRel(const substrait::ProjectRel& sproject,
                                      std::shared_ptr<PlanBuilder>* plan_builder) {
  if (sproject.has_input()) {
    ParseRel(sproject.input());
  }
  for (auto& stype : sproject.input_types()) {
    ParseType(stype);
  }
  for (auto& expr : sproject.expressions()) {
    ParseExpression(expr);
  }
  (*plan_builder) = std::make_shared<PlanBuilder>(
      (*plan_builder)
          ->project(std::vector<std::string>{"l_extendedprice * l_discount"},
                    std::vector<std::string>{"mul_res"}));
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

void SubstraitParser::ParseReadRel(const substrait::ReadRel& sread,
                                   std::shared_ptr<PlanBuilder>* plan_builder,
                                   u_int32_t* index, std::vector<std::string>* paths,
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
    velox_type_list.push_back(GetVeloxType(sub_type->type));
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
  auto tableHandle = HiveConnectorTestBase::makeTableHandle(std::move(filters), nullptr);

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
  assignments[col_name_list[0]] =
      HiveConnectorTestBase::regularColumn(col_name_list[0], velox_type_list[0]);
  assignments[col_name_list[1]] =
      HiveConnectorTestBase::regularColumn(col_name_list[1], velox_type_list[1]);
  assignments[col_name_list[2]] =
      HiveConnectorTestBase::regularColumn(col_name_list[2], velox_type_list[2]);
  assignments[col_name_list[3]] =
      HiveConnectorTestBase::regularColumn(col_name_list[3], velox_type_list[3]);

  auto outputType = ROW(std::move(col_name_list), std::move(velox_type_list));

  (*plan_builder) = std::make_shared<PlanBuilder>(
      PlanBuilder().tableScan(outputType, tableHandle, assignments));
}

void SubstraitParser::ParseRel(const substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    ParseAggregateRel(srel.aggregate(), &plan_builder_);
  } else if (srel.has_project()) {
    ParseProjectRel(srel.project(), &plan_builder_);
  } else if (srel.has_filter()) {
    ParseFilterRel(srel.filter());
  } else if (srel.has_read()) {
    ParseReadRel(srel.read(), &plan_builder_, &partition_index_, &paths_, &starts_,
                 &lengths_);
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

std::string SubstraitParser::FindFunction(uint64_t id) {
  if (functions_map_.find(id) == functions_map_.end()) {
    throw std::runtime_error("Could not find function " + std::to_string(id));
  }
  return functions_map_[id];
}

TypePtr SubstraitParser::GetVeloxType(std::string type_name) {
  if (type_name == "BOOL") {
    return BOOLEAN();
  } else if (type_name == "FP64") {
    return DOUBLE();
  } else {
    throw std::runtime_error("not supported");
  }
}

class SubstraitParser::WholeStageResultIterator
    : public ResultIterator<arrow::RecordBatch> {
 public:
  WholeStageResultIterator(const std::shared_ptr<PlanBuilder>& plan_builder,
                           u_int32_t index, std::vector<std::string> paths,
                           std::vector<u_int64_t> starts, std::vector<u_int64_t> lengths)
      : plan_builder_(plan_builder),
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
          facebook::velox::exec::test::kHiveConnectorId, path, FileFormat::ORC, start,
          length);
      connectorSplits.push_back(split);
    }
    splits_.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits_.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
    }
    auto op = plan_builder_->planNode();
    params_.planNode = op;
    cursor_ = std::make_unique<TaskCursor>(params_);
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
  std::shared_ptr<PlanBuilder> plan_builder_;
  std::unique_ptr<TaskCursor> cursor_;
  std::vector<exec::Split> splits_;
  bool noMoreSplits_ = false;
  CursorParameters params_;
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
