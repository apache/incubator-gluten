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

#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/core/PlanNode.h>

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
#include "velox/buffer/Buffer.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class SubstraitParser {
 public:
  SubstraitParser();
  struct SubstraitType {
    std::string type;
    std::string name;
    bool nullable;
    SubstraitType(const std::string& t, const std::string& n, const bool& nul) {
      type = t;
      name = n;
      nullable = nul;
    }
  };
  void ParseLiteral(const io::substrait::Expression::Literal& slit);
  void ParseScalarFunction(const io::substrait::Expression::ScalarFunction& sfunc);
  void ParseReferenceSegment(const io::substrait::ReferenceSegment& sref);
  void ParseFieldReference(const io::substrait::FieldReference& sfield);
  void ParseExpression(const io::substrait::Expression& sexpr);
  std::shared_ptr<SubstraitType> ParseType(const io::substrait::Type& stype);
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> ParseNamedStruct(
      const io::substrait::Type::NamedStruct& named_struct);
  void ParseAggregateRel(const io::substrait::AggregateRel& sagg);
  void ParseProjectRel(const io::substrait::ProjectRel& sproject);
  void ParseFilterRel(const io::substrait::FilterRel& sfilter);
  void ParseReadRel(const io::substrait::ReadRel& sread, u_int32_t* index,
                    std::vector<std::string>* paths, std::vector<u_int64_t>* starts,
                    std::vector<u_int64_t>* lengths);
  void ParseRel(const io::substrait::Rel& srel);
  void ParsePlan(const io::substrait::Plan& splan);
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter();

 private:
  std::shared_ptr<core::PlanNode> plan_node_;
  int plan_node_id_ = 0;
  std::unordered_map<uint64_t, std::string> functions_map_;
  u_int32_t partition_index_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  std::string findFunction(uint64_t id);
  TypePtr getVeloxType(std::string type_name);
  std::string nextPlanNodeId();
  std::vector<std::string> makeNames(const std::string& prefix, int size);
  class WholeStageResultIterator;
  inline static bool initialized = false;
};
