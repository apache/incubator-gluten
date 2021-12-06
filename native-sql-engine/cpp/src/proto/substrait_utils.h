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

#include "common/result_iterator.h"
#include "expression.pb.h"
#include "extensions.pb.h"
#include "function.pb.h"
#include "parameterized_types.pb.h"
#include "plan.pb.h"
#include "relations.pb.h"
#include "selection.pb.h"
#include "type.pb.h"
#include "type_expressions.pb.h"

class SubstraitParser {
 public:
  SubstraitParser();
  void ParseLiteral(const io::substrait::Expression::Literal& slit);
  void ParseScalarFunction(const io::substrait::Expression::ScalarFunction& sfunc);
  void ParseReferenceSegment(const io::substrait::ReferenceSegment& sref);
  void ParseFieldReference(const io::substrait::FieldReference& sfield);
  void ParseExpression(const io::substrait::Expression& sexpr);
  void ParseType(const io::substrait::Type& stype);
  void ParseNamedStruct(const io::substrait::Type::NamedStruct& named_struct);
  void ParseAggregateRel(const io::substrait::AggregateRel& sagg);
  void ParseProjectRel(const io::substrait::ProjectRel& sproject);
  void ParseFilterRel(const io::substrait::FilterRel& sfilter);
  void ParseReadRel(const io::substrait::ReadRel& sread);
  void ParseRel(const io::substrait::Rel& srel);
  void ParsePlan(const io::substrait::Plan& splan);
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter();

 private:
  std::string FindFunction(uint64_t id);
  std::unordered_map<uint64_t, std::string> functions_map_;
  class WholeStageResultIterator;
};
