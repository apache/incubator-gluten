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
#include "operators/functions/SparkExprToSubfieldFilterParser.h"

namespace gluten {

using namespace facebook::velox;

std::pair<common::Subfield, std::unique_ptr<common::Filter>> SparkExprToSubfieldFilterParser::toSubfieldFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  if (expr->isCallKind(); auto* call = expr->asUnchecked<core::CallTypedExpr>()) {
    if (call->name() == "or") {
      auto left = toSubfieldFilter(call->inputs()[0], evaluator);
      auto right = toSubfieldFilter(call->inputs()[1], evaluator);
      VELOX_CHECK(left.first == right.first);
      return {std::move(left.first), makeOrFilter(std::move(left.second), std::move(right.second))};
    }
    common::Subfield subfield;
    std::unique_ptr<common::Filter> filter;
    if (call->name() == "not") {
      if (auto* inner = call->inputs()[0]->asUnchecked<core::CallTypedExpr>()) {
        filter = leafCallToSubfieldFilter(*inner, subfield, evaluator, true);
      }
    } else {
      filter = leafCallToSubfieldFilter(*call, subfield, evaluator, false);
    }
    if (filter) {
      return std::make_pair(std::move(subfield), std::move(filter));
    }
  }
  VELOX_UNSUPPORTED("Unsupported expression for range filter: {}", expr->toString());
}

std::unique_ptr<common::Filter> SparkExprToSubfieldFilterParser::leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return nullptr;
  }

  const auto* leftSide = call.inputs()[0].get();

  if (call.name() == "equalto") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeNotEqualFilter(call.inputs()[1], evaluator) : makeEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lessthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanFilter(call.inputs()[1], evaluator)
                     : makeLessThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lessthan") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeLessThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "greaterthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeLessThanFilter(call.inputs()[1], evaluator)
                     : makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "greaterthan") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeLessThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeGreaterThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "in") {
    if (toSubfield(leftSide, subfield)) {
      return makeInFilter(call.inputs()[1], evaluator, negated);
    }
  } else if (call.name() == "isnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return exec::isNotNull();
      }
      return exec::isNull();
    }
  } else if (call.name() == "isnotnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return exec::isNull();
      }
      return exec::isNotNull();
    }
  }
  return nullptr;
}

} // namespace gluten
