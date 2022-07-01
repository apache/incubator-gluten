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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include "arrow/array/builder_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/util/checked_cast.h"
#include "kernels_ext.h"
#include "protobuf_utils.h"
#include "utils/exception.h"
#include "memory/allocator.h"

namespace gluten {
namespace compute {

SubstraitParser::SubstraitParser() {
  std::cout << "construct SubstraitParser" << std::endl;
}

std::shared_ptr<ArrowArrayResultIterator> SubstraitParser::GetResultIterator() {
  auto res_iter = std::make_shared<FirstStageResultIterator>();
  return std::make_shared<ArrowArrayResultIterator>(std::move(res_iter));
}

std::shared_ptr<ArrowArrayResultIterator> SubstraitParser::GetResultIterator(
    std::vector<std::shared_ptr<ArrowArrayResultIterator>> inputs) {
  auto res_iter = std::make_shared<MiddleStageResultIterator>(std::move(inputs));
  return std::make_shared<ArrowArrayResultIterator>(std::move(res_iter));
}

void SubstraitParser::ParseLiteral(const substrait::Expression::Literal& slit) {
  switch (slit.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      double val = slit.fp64();
      std::cout << "double lit: " << val << std::endl;
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
  auto function_id = sfunc.function_reference();
  auto function_name = FindFunction(function_id);
  std::cout << "function_name: " << function_name << std::endl;
  auto out_type = sfunc.output_type();
  ParseType(out_type);
}

void SubstraitParser::ParseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment& sref) {
  switch (sref.reference_type_case()) {
    case substrait::Expression::ReferenceSegment::ReferenceTypeCase::kStructField: {
      auto sfield = sref.struct_field();
      auto field_id = sfield.field();
      std::cout << "field_id: " << field_id << std::endl;
      break;
    }
    default:
      std::cout << "not supported" << std::endl;
      break;
  }
}

void SubstraitParser::ParseFieldReference(
    const substrait::Expression::FieldReference& sfield) {
  switch (sfield.reference_type_case()) {
    case substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
      auto dref = sfield.direct_reference();
      ParseReferenceSegment(dref);
      break;
    }
    case substrait::Expression::FieldReference::ReferenceTypeCase::kMaskedReference: {
      std::cout << "not supported" << std::endl;
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

void SubstraitParser::ParseType(const substrait::Type& stype) {
  switch (stype.kind_case()) {
    case substrait::Type::KindCase::kBool: {
      auto sbool = stype.bool_();
      auto nullable = sbool.nullability();
      auto type_id = sbool.type_variation_reference();
      break;
    }
    case substrait::Type::KindCase::kI32: {
      auto nullable = stype.i32().nullability();
      break;
    }
    case substrait::Type::KindCase::kI64: {
      auto nullability = stype.i64().nullability();
      break;
    }
    case substrait::Type::KindCase::kFp64: {
      auto sfp64 = stype.fp64();
      auto nullable = sfp64.nullability();
      auto type_id = sfp64.type_variation_reference();
      break;
    }
    case substrait::Type::KindCase::kStruct: {
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
      auto type_id = sstring.type_variation_reference();
      break;
    }
    default:
      std::cout << "Type not supported: " << stype.kind_case() << std::endl;
      break;
  }
}

void SubstraitParser::ParseNamedStruct(const substrait::NamedStruct& named_struct) {
  auto& snames = named_struct.names();
  for (auto& sname : snames) {
    std::cout << "NamedStruct name: " << sname << std::endl;
  }
  // Parse Struct
  auto& sstruct = named_struct.struct_();
  auto& stypes = sstruct.types();
  for (auto& type : stypes) {
    ParseType(type);
  }
}

void SubstraitParser::ParseAggregateRel(const substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    ParseRel(sagg.input());
  }
  // Parse groupings
  auto& groupings = sagg.groupings();
  for (auto& grouping : groupings) {
    auto& grouping_exprs = grouping.grouping_expressions();
    for (auto& grouping_expr : grouping_exprs) {
      ParseExpression(grouping_expr);
    }
  }
  // Parse measures
  for (auto& smea : sagg.measures()) {
    auto agg_function = smea.measure();
    auto phase = agg_function.phase();
    auto function_id = agg_function.function_reference();
    auto args = agg_function.args();
    for (auto arg : args) {
      ParseExpression(arg);
    }
    auto out_type = agg_function.output_type();
  }
}

void SubstraitParser::ParseProjectRel(const substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    ParseRel(sproject.input());
  }
  for (auto& expr : sproject.expressions()) {
    ParseExpression(expr);
  }
}

void SubstraitParser::ParseFilterRel(const substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    ParseRel(sfilter.input());
  }
  if (sfilter.has_condition()) {
    ParseExpression(sfilter.condition());
  }
}

void SubstraitParser::ParseReadRel(const substrait::ReadRel& sread) {
  if (sread.has_base_schema()) {
    auto& base_schema = sread.base_schema();
    ParseNamedStruct(base_schema);
  }
  // Parse local files
  if (sread.has_local_files()) {
    auto& local_files = sread.local_files();
    auto& files_list = local_files.items();
    for (auto& file : files_list) {
      auto& uri_file = file.uri_file();
      auto index = file.partition_index();
      auto start = file.start();
      auto length = file.length();
    }
  }
  auto& sfilter = sread.filter();
  ParseExpression(sfilter);
}

void SubstraitParser::ParseRel(const substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    ParseAggregateRel(srel.aggregate());
  } else if (srel.has_project()) {
    ParseProjectRel(srel.project());
  } else if (srel.has_filter()) {
    ParseFilterRel(srel.filter());
  } else if (srel.has_read()) {
    ParseReadRel(srel.read());
  } else {
    std::cout << "not supported" << std::endl;
  }
}

void SubstraitParser::ParseRelRoot(const substrait::RelRoot& sroot) {
  if (sroot.has_input()) {
    auto& srel = sroot.input();
    ParseRel(srel);
  }
  auto& snames = sroot.names();
}

void SubstraitParser::ParsePlan(const substrait::Plan& splan) {
  for (auto& sextension : splan.extensions()) {
    if (!sextension.has_extension_function()) {
      continue;
    }
    auto& sfmap = sextension.extension_function();
    auto id = sfmap.function_anchor();
    auto name = sfmap.name();
    functions_map_[id] = name;
    std::cout << "Function id: " << id << ", name: " << name << std::endl;
  }
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      ParseRelRoot(srel.root());
    }
    if (srel.has_rel()) {
      ParseRel(srel.rel());
    }
  }
}

std::string SubstraitParser::FindFunction(uint64_t id) {
  if (functions_map_.find(id) == functions_map_.end()) {
    throw std::runtime_error("Could not find function " + std::to_string(id));
  }
  return functions_map_[id];
}

class SubstraitParser::FirstStageResultIterator {
 public:
  FirstStageResultIterator() {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(pool_, arrow::float64(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<arrow::DoubleBuilder*>(array_builder.release()));
  }

  arrow::Result<std::shared_ptr<ArrowArray>> Next() {
    if (!has_next_) {
      return nullptr;
    }
    double res = 10000;
    builder_->Append(res);
    std::shared_ptr<arrow::Array> array;
    auto status = builder_->Finish(&array);
    // res_arrays.push_back(array);
    // std::vector<std::shared_ptr<arrow::Field>> ret_types = {
    //     arrow::field("res", arrow::float64())};
    has_next_ = false;
    ArrowArray arrow_array;
    GLUTEN_THROW_NOT_OK(arrow::ExportArray(*array, &arrow_array));
    return std::make_shared<ArrowArray>(arrow_array);
  }

 private:
  arrow::MemoryPool* pool_ = gluten::memory::GetDefaultWrappedArrowMemoryPool();
  std::unique_ptr<arrow::DoubleBuilder> builder_;
  bool has_next_ = true;
  // std::vector<std::shared_ptr<arrow::Array>> res_arrays;
};

class SubstraitParser::MiddleStageResultIterator {
 public:
  MiddleStageResultIterator(
      std::vector<std::shared_ptr<ArrowArrayResultIterator>> inputs) {
    // TODO: the iter index should be acquired from Substrait Plan.
    int iter_idx = 0;
    lazy_iter_ = std::make_shared<LazyReadIterator>(
        std::move(inputs[iter_idx]->ToArrowArrayIterator()));
  }

  arrow::Result<std::shared_ptr<ArrowArray>> Next() {
    if (!lazy_iter_->HasNext()) {
      return nullptr;
    }
    std::shared_ptr<ArrowArray> array;
    lazy_iter_->Next(&array);
    return array;
  }

 private:
  std::shared_ptr<LazyReadIterator> lazy_iter_;
};

}  // namespace compute
}  // namespace gluten
