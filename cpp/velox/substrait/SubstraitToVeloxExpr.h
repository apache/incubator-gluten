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

#include "SubstraitParser.h"
#include "velox/core/Expressions.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten {

/// This class is used to convert Substrait representations to Velox
/// expressions.
class SubstraitVeloxExprConverter {
 public:
  /// subParser: A Substrait parser used to convert Substrait representations
  /// into recognizable representations. functionMap: A pre-constructed map
  /// storing the relations between the function id and the function name.
  explicit SubstraitVeloxExprConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<uint64_t, std::string>& functionMap)
      : pool_(pool), functionMap_(functionMap) {}

  /// Stores the variant and its type.
  struct TypedVariant {
    variant veloxVariant;
    TypePtr variantType;
  };

  /// Convert Substrait Field into Velox Field Expression.
  static std::shared_ptr<const core::FieldAccessTypedExpr> toVeloxExpr(
      const ::substrait::Expression::FieldReference& substraitField,
      const RowTypePtr& inputType);

  /// Convert Substrait ScalarFunction into Velox Expression.
  core::TypedExprPtr toVeloxExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

  /// Convert Substrait SingularOrList into Velox Expression.
  core::TypedExprPtr toVeloxExpr(
      const ::substrait::Expression::SingularOrList& singularOrList,
      const RowTypePtr& inputType);

  /// Convert Substrait CastExpression to Velox Expression.
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression::Cast& castExpr, const RowTypePtr& inputType);

  /// Create expression for extract.
  static core::TypedExprPtr toExtractExpr(const std::vector<core::TypedExprPtr>& params, const TypePtr& outputType);

  /// Used to convert Substrait Literal into Velox Expression.
  std::shared_ptr<const core::ConstantTypedExpr> toVeloxExpr(const ::substrait::Expression::Literal& substraitLit);

  /// Convert Substrait Expression into Velox Expression.
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression& substraitExpr, const RowTypePtr& inputType);

  /// Convert Substrait IfThen into switch or if expression.
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression::IfThen& substraitIfThen, const RowTypePtr& inputType);

  /// Wrap a constant vector from literals with an array vector inside to create
  /// the constant expression.
  std::shared_ptr<const core::ConstantTypedExpr> literalsToConstantExpr(
      const std::vector<::substrait::Expression::Literal>& literals);

  /// Create expression for lambda.
  std::shared_ptr<const core::ITypedExpr> toLambdaExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

 private:
  /// Convert list literal to ArrayVector.
  ArrayVectorPtr literalsToArrayVector(const ::substrait::Expression::Literal& literal);
  /// Convert map literal to MapVector.
  MapVectorPtr literalsToMapVector(const ::substrait::Expression::Literal& literal);
  VectorPtr literalsToVector(
      ::substrait::Expression_Literal::LiteralTypeCase childTypeCase,
      vector_size_t childSize,
      const ::substrait::Expression::Literal& literal,
      std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAtFunc);
  RowVectorPtr literalsToRowVector(const ::substrait::Expression::Literal& structLiteral);

  /// Memory pool.
  memory::MemoryPool* pool_;

  /// The map storing the relations between the function id and the function
  /// name.
  std::unordered_map<uint64_t, std::string> functionMap_;

  // The map storing the Substrait extract function input field and velox
  // function name.
  static std::unordered_map<std::string, std::string> extractDatetimeFunctionMap_;
};

} // namespace gluten
