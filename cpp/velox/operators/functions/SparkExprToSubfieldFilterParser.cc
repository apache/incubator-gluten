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

bool SparkExprToSubfieldFilterParser::toSparkSubfield(const core::ITypedExpr* field, common::Subfield& subfield) {
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  for (auto* current = field;;) {
    if (auto* fieldAccess = dynamic_cast<const core::FieldAccessTypedExpr*>(current)) {
      path.push_back(std::make_unique<common::Subfield::NestedField>(fieldAccess->name()));
    } else if (dynamic_cast<const core::DereferenceTypedExpr*>(current)) {
      return false;
    } else if (dynamic_cast<const core::InputTypedExpr*>(current) == nullptr) {
      return false;
    } else {
      break;
    }

    if (!current->inputs().empty()) {
      return false;
    } else {
      break;
    }
  }
  std::reverse(path.begin(), path.end());
  subfield = common::Subfield(std::move(path));
  return true;
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
    if (toSparkSubfield(leftSide, subfield)) {
      return negated ? makeNotEqualFilter(call.inputs()[1], evaluator) : makeEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lessthanorequal") {
    if (toSparkSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanFilter(call.inputs()[1], evaluator)
                     : makeLessThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lessthan") {
    if (toSparkSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeLessThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "greaterthanorequal") {
    if (toSparkSubfield(leftSide, subfield)) {
      return negated ? makeLessThanFilter(call.inputs()[1], evaluator)
                     : makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "greaterthan") {
    if (toSparkSubfield(leftSide, subfield)) {
      return negated ? makeLessThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeGreaterThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "in") {
    if (toSparkSubfield(leftSide, subfield)) {
      return makeInFilter(call.inputs()[1], evaluator, negated);
    }
  } else if (call.name() == "isnull") {
    if (toSparkSubfield(leftSide, subfield)) {
      if (negated) {
        return exec::isNotNull();
      }
      return exec::isNull();
    }
  } else if (call.name() == "isnotnull") {
    if (toSparkSubfield(leftSide, subfield)) {
      if (negated) {
        return exec::isNull();
      }
      return exec::isNotNull();
    }
  }
  return nullptr;
}
} // namespace gluten
