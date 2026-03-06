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

#include <gtest/gtest.h>

#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::common;

namespace gluten {
namespace {

class SparkExprToSubfieldFilterParserTest : public ::testing::Test,
                                            public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  /// Builds a serialized Velox BloomFilter containing the given int64 values.
  std::vector<char> makeSerializedBloomFilter(
      const std::vector<int64_t>& values) {
    BloomFilter<> bf;
    bf.reset(std::max<int32_t>(100, values.size() * 4));
    for (auto v : values) {
      bf.insert(folly::hasher<int64_t>()(v));
    }
    std::vector<char> data(bf.serializedSize());
    bf.serialize(data.data());
    return data;
  }

  /// Creates a ConstantTypedExpr wrapping serialized bloom filter bytes.
  core::TypedExprPtr makeVarbinaryConstant(const std::vector<char>& data) {
    auto vector = makeFlatVector<StringView>(
        std::vector<StringView>{StringView(data.data(), data.size())},
        VARBINARY());
    return std::make_shared<const core::ConstantTypedExpr>(vector);
  }

  /// Creates a null VARBINARY constant expression.
  core::TypedExprPtr makeNullVarbinaryConstant() {
    auto vector = BaseVector::createNullConstant(VARBINARY(), 1, pool());
    return std::make_shared<const core::ConstantTypedExpr>(vector);
  }

  /// Constructs a might_contain(bloomFilter, value) CallTypedExpr.
  core::CallTypedExprPtr makeMightContainCall(
      const core::TypedExprPtr& bloomFilterExpr,
      const core::TypedExprPtr& valueExpr) {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), "might_contain", bloomFilterExpr, valueExpr);
  }

  /// Calls leafCallToSubfieldFilter on the parser. Returns (subfield, nullptr)
  /// when the parser cannot translate the expression.
  std::pair<Subfield, std::unique_ptr<Filter>> parse(
      const core::CallTypedExprPtr& call,
      bool negated = false) {
    if (auto result =
            parser_.leafCallToSubfieldFilter(*call, &evaluator_, negated)) {
      return std::move(result.value());
    }
    return std::make_pair(Subfield(), nullptr);
  }

 private:
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  exec::SimpleExpressionEvaluator evaluator_{queryCtx_.get(), pool()};
  SparkExprToSubfieldFilterParser parser_;
};

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainBasic) {
  std::vector<int64_t> inserted = {42, 100, 200, 300};
  auto serialized = makeSerializedBloomFilter(inserted);

  auto bloomExpr = makeVarbinaryConstant(serialized);
  auto columnExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  auto call = makeMightContainCall(bloomExpr, columnExpr);

  auto [subfield, filter] = parse(call);

  ASSERT_TRUE(filter);

  // Verify the subfield points to column "a".
  ASSERT_EQ(subfield.path().size(), 1);
  EXPECT_EQ(*subfield.path()[0], Subfield::NestedField("a"));

  // All inserted values must pass the bloom filter.
  for (auto v : inserted) {
    EXPECT_TRUE(filter->testInt64(v)) << "Value " << v << " should pass";
  }

  // Most non-inserted values should be rejected.
  int falsePositives = 0;
  for (int64_t v = 1000; v < 2000; v++) {
    if (filter->testInt64(v)) {
      ++falsePositives;
    }
  }
  EXPECT_LT(falsePositives, 100) << "Too many false positives";
}

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainNullBloomFilter) {
  auto nullExpr = makeNullVarbinaryConstant();
  auto columnExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  auto call = makeMightContainCall(nullExpr, columnExpr);

  auto [subfield, filter] = parse(call);
  EXPECT_FALSE(filter);
}

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainNegated) {
  auto serialized = makeSerializedBloomFilter({42});
  auto bloomExpr = makeVarbinaryConstant(serialized);
  auto columnExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  auto call = makeMightContainCall(bloomExpr, columnExpr);

  auto [subfield, filter] = parse(call, /*negated=*/true);
  EXPECT_FALSE(filter);
}

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainNonColumnValue) {
  auto serialized = makeSerializedBloomFilter({42});
  auto bloomExpr = makeVarbinaryConstant(serialized);
  // Use a constant (not a column reference) as the value argument.
  auto constValue = makeVarbinaryConstant(serialized); // type doesn't matter
  auto call = makeMightContainCall(bloomExpr, constValue);

  auto [subfield, filter] = parse(call);
  EXPECT_FALSE(filter);
}

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainInt64Range) {
  auto serialized = makeSerializedBloomFilter({42});
  auto bloomExpr = makeVarbinaryConstant(serialized);
  auto columnExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  auto call = makeMightContainCall(bloomExpr, columnExpr);

  auto [subfield, filter] = parse(call);
  ASSERT_TRUE(filter);

  // Bloom filters cannot efficiently prune integer ranges.
  EXPECT_TRUE(filter->testInt64Range(0, 1000, false));
  EXPECT_TRUE(filter->testInt64Range(0, 1000, true));
}

TEST_F(SparkExprToSubfieldFilterParserTest, mightContainClone) {
  std::vector<int64_t> inserted = {42, 100};
  auto serialized = makeSerializedBloomFilter(inserted);
  auto bloomExpr = makeVarbinaryConstant(serialized);
  auto columnExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  auto call = makeMightContainCall(bloomExpr, columnExpr);

  auto [subfield, filter] = parse(call);
  ASSERT_TRUE(filter);

  auto cloned = filter->clone(std::nullopt);
  ASSERT_TRUE(cloned);
  EXPECT_TRUE(filter->testingEquals(*cloned));

  for (auto v : inserted) {
    EXPECT_TRUE(cloned->testInt64(v));
  }
}

} // namespace
} // namespace gluten
