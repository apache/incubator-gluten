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

#include "velox/common/base/BloomFilter.h"
#include "velox/expression/Expr.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

using namespace facebook::velox;

namespace {

// Evaluates an expression as a constant. Returns nullptr if the expression is
// not constant or evaluation fails. Errors are intentionally swallowed because
// a non-evaluable expression simply means the filter cannot be pushed down.
VectorPtr toConstant(const core::TypedExprPtr& expr, core::ExpressionEvaluator* evaluator) {
  auto exprSet = evaluator->compile(expr);
  if (!exprSet->exprs()[0]->isConstantExpr()) {
    return nullptr;
  }
  RowVector input(evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const VeloxUserError&) {
    return nullptr;
  }
  return result;
}

/// Subfield filter backed by Velox's BloomFilter from bloom_filter_agg / might_contain.
class SparkMightContain final : public common::Filter {
 public:
  SparkMightContain(const char* serializedData, bool nullAllowed)
      : Filter(true, nullAllowed, common::FilterKind::kBigintValuesUsingBloomFilter) {
    bloomFilter_.merge(serializedData);
  }

  bool testInt64(int64_t value) const final {
    return bloomFilter_.mayContain(folly::hasher<int64_t>()(value));
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/) const final {
    return true;
  }

  std::unique_ptr<Filter> clone(std::optional<bool> nullAllowed) const override {
    std::vector<char> data(bloomFilter_.serializedSize());
    bloomFilter_.serialize(data.data());
    return std::make_unique<SparkMightContain>(data.data(), nullAllowed.value_or(nullAllowed_));
  }

  bool testingEquals(const Filter& other) const override {
    return dynamic_cast<const SparkMightContain*>(&other) != nullptr;
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("Serialization is not supported for SparkMightContain");
  }

 private:
  BloomFilter<> bloomFilter_;
};

std::optional<std::pair<facebook::velox::common::Subfield, std::unique_ptr<facebook::velox::common::Filter>>> combine(
    facebook::velox::common::Subfield& subfield,
    std::unique_ptr<facebook::velox::common::Filter>& filter) {
  if (filter != nullptr) {
    return std::make_pair(std::move(subfield), std::move(filter));
  }

  return std::nullopt;
}

} // namespace

std::optional<std::pair<facebook::velox::common::Subfield, std::unique_ptr<facebook::velox::common::Filter>>>
SparkExprToSubfieldFilterParser::leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return std::nullopt;
  }

  const auto* leftSide = call.inputs()[0].get();

  common::Subfield subfield;
  if (call.name() == "equalto") {
    if (toSubfield(leftSide, subfield)) {
      auto filter =
          negated ? makeNotEqualFilter(call.inputs()[1], evaluator) : makeEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "lessthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeGreaterThanFilter(call.inputs()[1], evaluator)
                            : makeLessThanOrEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "lessthan") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator)
                            : makeLessThanFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "greaterthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeLessThanFilter(call.inputs()[1], evaluator)
                            : makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "greaterthan") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeLessThanOrEqualFilter(call.inputs()[1], evaluator)
                            : makeGreaterThanFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "in") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = makeInFilter(call.inputs()[1], evaluator, negated);
      return combine(subfield, filter);
    }
  } else if (call.name() == "isnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), facebook::velox::exec::isNotNull());
      }
      return std::make_pair(std::move(subfield), facebook::velox::exec::isNull());
    }
  } else if (call.name() == "isnotnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), facebook::velox::exec::isNull());
      }
      return std::make_pair(std::move(subfield), facebook::velox::exec::isNotNull());
    }
  } else if (call.name() == "might_contain" && !negated) {
    // might_contain(bloomFilter, value) — the column to filter is input[1].
    if (call.inputs().size() >= 2) {
      const auto* valueSide = call.inputs()[1].get();
      if (toSubfield(valueSide, subfield)) {
        auto bloomFilterValue = toConstant(call.inputs()[0], evaluator);
        if (bloomFilterValue && !bloomFilterValue->isNullAt(0)) {
          auto sv = bloomFilterValue->as<SimpleVector<StringView>>()->valueAt(0);
          std::unique_ptr<common::Filter> filter =
              std::make_unique<SparkMightContain>(sv.data(), false /*nullAllowed*/);
          return combine(subfield, filter);
        }
      }
    }
  }
  return std::nullopt;
}

} // namespace gluten
