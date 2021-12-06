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

namespace substrait = io::substrait;

SubstraitParser::SubstraitParser() {
  std::cout << "construct SubstraitParser" << std::endl;
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> SubstraitParser::getResIter() {
  auto wholestage_iter = std::make_shared<WholeStageResultIterator>();
  auto res_iter =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(wholestage_iter);
  return res_iter;
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
  auto function_id = sfunc.id().id();
  auto function_name = FindFunction(function_id);
  std::cout << "function_name: " << function_name << std::endl;
  auto out_type = sfunc.output_type();
  ParseType(out_type);
}

void SubstraitParser::ParseReferenceSegment(const ::substrait::ReferenceSegment& sref) {
  switch (sref.reference_type_case()) {
    case substrait::ReferenceSegment::ReferenceTypeCase::kStructField: {
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

void SubstraitParser::ParseFieldReference(const substrait::FieldReference& sfield) {
  switch (sfield.reference_type_case()) {
    case substrait::FieldReference::ReferenceTypeCase::kDirectReference: {
      auto dref = sfield.direct_reference();
      ParseReferenceSegment(dref);
      break;
    }
    case substrait::FieldReference::ReferenceTypeCase::kMaskedReference: {
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
      auto name = sbool.variation().name();
      break;
    }
    case substrait::Type::KindCase::kFp64: {
      auto sfp64 = stype.fp64();
      auto nullable = sfp64.nullability();
      auto name = sfp64.variation().name();
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
      auto name = sstring.variation().name();
      break;
    }
    default:
      std::cout << "Type not supported" << std::endl;
      break;
  }
}

void SubstraitParser::ParseNamedStruct(const substrait::Type::NamedStruct& named_struct) {
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
    auto grouping_fields = grouping.input_fields();
    for (auto& grouping_field : grouping_fields) {
      std::cout << "Agg grouping_field: " << grouping_field << std::endl;
    }
  }
  // Parse measures
  for (auto& smea : sagg.measures()) {
    auto aggFunction = smea.measure();
    auto phase = aggFunction.phase();
    auto function_id = aggFunction.id().id();
    std::cout << "Agg Function id: " << function_id << std::endl;
    auto args = aggFunction.args();
    for (auto arg : args) {
      ParseExpression(arg);
    }
  }
  auto agg_phase = sagg.phase();
  // Parse Input and Output types
  std::cout << "Agg input and output:" << std::endl;
  for (auto& stype : sagg.input_types()) {
    ParseType(stype);
  }
  for (auto& stype : sagg.output_types()) {
    ParseType(stype);
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

void SubstraitParser::ParseReadRel(const substrait::ReadRel& sread) {
  if (sread.has_base_schema()) {
    auto& base_schema = sread.base_schema();
    ParseNamedStruct(base_schema);
  }
  // Parse local files
  if (sread.has_local_files()) {
    auto& local_files = sread.local_files();
    auto index = local_files.index();
    auto& files_list = local_files.items();
    for (auto& file : files_list) {
      auto& uri_path = file.uri_path();
      auto start = file.start();
      auto length = file.length();
      std::cout << "uri_path: " << uri_path << " start: " << start
                << " length: " << length << std::endl;
    }
  }
  auto& sfilter = sread.filter();
  std::cout << "filter pushdown: " << std::endl;
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

void SubstraitParser::ParsePlan(const substrait::Plan& splan) {
  for (auto& smap : splan.mappings()) {
    if (!smap.has_function_mapping()) {
      continue;
    }
    auto& sfmap = smap.function_mapping();
    auto id = sfmap.function_id().id();
    auto name = sfmap.name();
    functions_map_[id] = name;
    std::cout << "Function id: " << id << ", name: " << name << std::endl;
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

class SubstraitParser::WholeStageResultIterator
    : public ResultIterator<arrow::RecordBatch> {
  bool HasNext() override { return false; }

  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
    return arrow::Status::OK();
  }
};
