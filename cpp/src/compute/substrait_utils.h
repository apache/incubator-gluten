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

#include "jni/exec_backend.h"
#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"
#include "utils/result_iterator.h"

namespace gluten {
namespace compute {

class SubstraitParser : public ExecBackendBase {
 public:
  SubstraitParser();
  void ParseLiteral(const ::substrait::Expression::Literal& slit);
  void ParseScalarFunction(const ::substrait::Expression::ScalarFunction& sfunc);
  void ParseReferenceSegment(const ::substrait::Expression::ReferenceSegment& sref);
  void ParseFieldReference(const ::substrait::Expression::FieldReference& sfield);
  void ParseExpression(const ::substrait::Expression& sexpr);
  void ParseType(const ::substrait::Type& stype);
  void ParseNamedStruct(const ::substrait::NamedStruct& named_struct);
  void ParseAggregateRel(const ::substrait::AggregateRel& sagg);
  void ParseProjectRel(const ::substrait::ProjectRel& sproject);
  void ParseFilterRel(const ::substrait::FilterRel& sfilter);
  void ParseReadRel(const ::substrait::ReadRel& sread);
  void ParseRelRoot(const ::substrait::RelRoot& sroot);
  void ParseRel(const ::substrait::Rel& srel);
  void ParsePlan(const ::substrait::Plan& splan);
  std::shared_ptr<RecordBatchResultIterator> GetResultIterator() override;
  std::shared_ptr<RecordBatchResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<RecordBatchResultIterator>> inputs) override;

 private:
  std::string FindFunction(uint64_t id);
  std::unordered_map<uint64_t, std::string> functions_map_;
  class FirstStageResultIterator;
  class MiddleStageResultIterator;
};

}  // namespace compute
}  // namespace gluten
