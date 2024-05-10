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

#include "SubstraitToVeloxPlan.h"
#include "velox/core/QueryCtx.h"

namespace gluten {

/// This class is used to validate whether the computing of
/// a Substrait plan is supported in Velox.
class SubstraitToVeloxPlanValidator {
 public:
  SubstraitToVeloxPlanValidator(memory::MemoryPool* pool, core::ExecCtx* execCtx)
      : pool_(pool), execCtx_(execCtx), planConverter_(pool_, confMap_, std::nullopt, true) {}

  /// Used to validate whether the computing of this Plan is supported.
  bool validate(const ::substrait::Plan& plan);

  const std::vector<std::string>& getValidateLog() const {
    return validateLog_;
  }

 private:
  /// Used to validate whether the computing of this Write is supported.
  bool validate(const ::substrait::WriteRel& writeRel);

  /// Used to validate whether the computing of this Limit is supported.
  bool validate(const ::substrait::FetchRel& fetchRel);

  /// Used to validate whether the computing of this TopN is supported.
  bool validate(const ::substrait::TopNRel& topNRel);

  /// Used to validate whether the computing of this Expand is supported.
  bool validate(const ::substrait::ExpandRel& expandRel);

  /// Used to validate whether the computing of this Generate is supported.
  bool validate(const ::substrait::GenerateRel& generateRel);

  /// Used to validate whether the computing of this Sort is supported.
  bool validate(const ::substrait::SortRel& sortRel);

  /// Used to validate whether the computing of this Window is supported.
  bool validate(const ::substrait::WindowRel& windowRel);

  /// Used to validate whether the computing of this WindowGroupLimit is supported.
  bool validate(const ::substrait::WindowGroupLimitRel& windowGroupLimitRel);

  /// Used to validate whether the computing of this Aggregation is supported.
  bool validate(const ::substrait::AggregateRel& aggRel);

  /// Used to validate whether the computing of this Project is supported.
  bool validate(const ::substrait::ProjectRel& projectRel);

  /// Used to validate whether the computing of this Filter is supported.
  bool validate(const ::substrait::FilterRel& filterRel);

  /// Used to validate Join.
  bool validate(const ::substrait::JoinRel& joinRel);

  /// Used to validate Cartesian product.
  bool validate(const ::substrait::CrossRel& crossRel);

  /// Used to validate whether the computing of this Read is supported.
  bool validate(const ::substrait::ReadRel& readRel);

  /// Used to validate whether the computing of this Rel is supported.
  bool validate(const ::substrait::Rel& rel);

  /// Used to validate whether the computing of this RelRoot is supported.
  bool validate(const ::substrait::RelRoot& relRoot);

  /// A memory pool used for function validation.
  memory::MemoryPool* pool_;

  /// An execution context used for function validation.
  core::ExecCtx* execCtx_;

  // Unused customized conf map.
  std::unordered_map<std::string, std::string> confMap_ = {};

  /// A converter used to convert Substrait plan into Velox's plan node.
  SubstraitToVeloxPlanConverter planConverter_;

  /// An expression converter used to convert Substrait representations into
  /// Velox expressions.
  SubstraitVeloxExprConverter* exprConverter_ = nullptr;

  std::vector<std::string> validateLog_;

  /// Used to get types from advanced extension and validate them.
  bool validateInputTypes(const ::substrait::extensions::AdvancedExtension& extension, std::vector<TypePtr>& types);

  bool validateAggRelFunctionType(const ::substrait::AggregateRel& substraitAgg);

  /// Validate the round scalar function.
  bool validateRound(const ::substrait::Expression::ScalarFunction& scalarFunction, const RowTypePtr& inputType);

  /// Validate extract function.
  bool validateExtractExpr(const std::vector<core::TypedExprPtr>& params);

  /// Validates regex functions.
  /// Ensures the second pattern argument is a literal string.
  /// Check if the pattern can pass with RE2 compilation.
  bool validateRegexExpr(const std::string& name, const ::substrait::Expression::ScalarFunction& scalarFunction);

  /// Validate Substrait scarlar function.
  bool validateScalarFunction(
      const ::substrait::Expression::ScalarFunction& scalarFunction,
      const RowTypePtr& inputType);

  /// Validate Substrait Cast expression.
  bool validateCast(const ::substrait::Expression::Cast& castExpr, const RowTypePtr& inputType);

  /// Validate Substrait expression.
  bool validateExpression(const ::substrait::Expression& expression, const RowTypePtr& inputType);

  /// Validate Substrait literal.
  bool validateLiteral(const ::substrait::Expression_Literal& literal, const RowTypePtr& inputType);

  /// Validate Substrait if-then expression.
  bool validateIfThen(const ::substrait::Expression_IfThen& ifThen, const RowTypePtr& inputType);

  /// Validate Substrait IN expression.
  bool validateSingularOrList(
      const ::substrait::Expression::SingularOrList& singularOrList,
      const RowTypePtr& inputType);

  /// Add necessary log for fallback
  void logValidateMsg(const std::string& log) {
    validateLog_.emplace_back(log);
  }
};

} // namespace gluten
