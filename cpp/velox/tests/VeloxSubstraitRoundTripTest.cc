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

#include <folly/Random.h>
#include <folly/init/Init.h>

#include "operators/functions/RegistrationAllFunctions.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/VeloxToSubstraitPlan.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "substrait/VariantToVectorConverter.h"
#include "substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

namespace gluten {
class VeloxSubstraitRoundTripTest : public OperatorTestBase {
 protected:
  /// Makes a vector of INTEGER type with 'size' RowVectorPtr.
  /// @param size The number of RowVectorPtr.
  /// @param childSize The number of columns for each row.
  /// @param batchSize The batch Size of the data.
  std::vector<RowVectorPtr> makeVectors(int64_t size, int64_t childSize, int64_t batchSize) {
    std::vector<RowVectorPtr> vectors;
    std::mt19937 gen(std::mt19937::default_seed);
    for (int i = 0; i < size; i++) {
      std::vector<VectorPtr> children;
      for (int j = 0; j < childSize; j++) {
        children.emplace_back(makeFlatVector<int32_t>(
            batchSize,
            [&](auto /*row*/) { return folly::Random::rand32(INT32_MAX / 4, INT32_MAX / 2, gen); },
            nullEvery(2)));
      }

      vectors.push_back(makeRowVector({children}));
    }
    return vectors;
  }

  void assertPlanConversion(const std::shared_ptr<const core::PlanNode>& plan, const std::string& duckDbSql) {
    assertQuery(plan, duckDbSql);

    // Convert Velox Plan to Substrait Plan.
    google::protobuf::Arena arena;
    auto substraitPlan = veloxConvertor_->toSubstrait(arena, plan);
    std::unordered_map<std::string, std::string> sessionConf = {};
    std::shared_ptr<SubstraitToVeloxPlanConverter> substraitConverter_ =
        std::make_shared<SubstraitToVeloxPlanConverter>(pool_.get(), sessionConf, std::nullopt, true);

    // Convert Substrait Plan to the same Velox Plan.
    auto samePlan = substraitConverter_->toVeloxPlan(substraitPlan);

    // Assert velox again.
    assertQuery(samePlan, duckDbSql);
  }

  void assertFailingPlanConversion(const std::shared_ptr<const core::PlanNode>& plan, const std::string& errorMessage) {
    try {
      CursorParameters params;
      params.planNode = plan;
      readCursor(params, [](auto /*task*/) {});

      // Convert Velox Plan to Substrait Plan.
      google::protobuf::Arena arena;
      auto substraitPlan = veloxConvertor_->toSubstrait(arena, plan);
      std::unordered_map<std::string, std::string> sessionConf = {};
      std::shared_ptr<SubstraitToVeloxPlanConverter> substraitConverter_ =
          std::make_shared<SubstraitToVeloxPlanConverter>(pool_.get(), sessionConf, std::nullopt, true);
      // Convert Substrait Plan to the same Velox Plan.
      auto samePlan = substraitConverter_->toVeloxPlan(substraitPlan);

      // Assert velox again.
      params.planNode = samePlan;
      readCursor(params, [](auto /*task*/) {});
      FAIL() << "Expected an exception";
    } catch (const VeloxException& e) {
      ASSERT_TRUE(e.message().find(errorMessage) != std::string::npos)
          << "Expected error message to contain '" << errorMessage << "', but received '" << e.message() << "'.";
    }
  }

  std::shared_ptr<VeloxToSubstraitPlanConvertor> veloxConvertor_ = std::make_shared<VeloxToSubstraitPlanConvertor>();
};

TEST_F(VeloxSubstraitRoundTripTest, project) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).project({"c0 + c1", "c1 / c2"}).planNode();
  assertPlanConversion(plan, "SELECT c0 + c1, c1 // c2 FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, cast) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);
  // Cast int32 to int64.
  auto plan = PlanBuilder().values(vectors).project({"cast(c0 as bigint)"}).planNode();
  assertPlanConversion(plan, "SELECT cast(c0 as bigint) FROM tmp");

  // Cast literal "abc" to int64 and allow cast failure, expecting no exception.
  plan = PlanBuilder().values(vectors).project({"try_cast('abc' as bigint)"}).planNode();
  assertPlanConversion(plan, "SELECT try_cast('abc' as bigint) FROM tmp");

  // Cast literal "abc" to int64, expecting an exception to be thrown.
  plan = PlanBuilder().values(vectors).project({"cast('abc' as bigint)"}).planNode();
  assertFailingPlanConversion(plan, "Cannot cast VARCHAR 'abc' to BIGINT.");
}

TEST_F(VeloxSubstraitRoundTripTest, filter) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().values(vectors).filter("c2 < 1000").planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp WHERE c2 < 1000");
}

TEST_F(VeloxSubstraitRoundTripTest, null) {
  auto vectors = makeRowVector(ROW({}, {}), 1);
  auto plan = PlanBuilder().values({vectors}).project({"NULL"}).planNode();
  assertPlanConversion(plan, "SELECT NULL ");
}

TEST_F(VeloxSubstraitRoundTripTest, values) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775, 4077358421272316858}),
       makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
       makeFlatVector<double>({0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
       makeFlatVector<bool>({true, false, false}),
       makeFlatVector<int32_t>(3, nullptr, nullEvery(1))

      });
  createDuckDbTable({vectors});

  auto plan = PlanBuilder().values({vectors}).planNode();

  assertPlanConversion(plan, "SELECT * FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, count) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .singleAggregation({"c0", "c1"}, {"count(c4) as num_price"})
                  .project({"num_price"})
                  .planNode();

  assertPlanConversion(plan, "SELECT count(c4) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
}

TEST_F(VeloxSubstraitRoundTripTest, countAll) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .singleAggregation({"c0", "c1"}, {"count(1) as num_price"})
                  .project({"num_price"})
                  .planNode();

  assertPlanConversion(plan, "SELECT count(*) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
}

TEST_F(VeloxSubstraitRoundTripTest, sum) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().values(vectors).partialAggregation({}, {"sum(1)", "count(c4)"}).planNode();

  assertPlanConversion(plan, "SELECT sum(1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, sumAndCount) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder().values(vectors).partialAggregation({}, {"sum(c1)", "count(c4)"}).finalAggregation().planNode();

  assertPlanConversion(plan, "SELECT sum(c1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, sumGlobal) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  // Global final aggregation.
  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({"c0"}, {"sum(c0)", "sum(c1)"})
                  .intermediateAggregation()
                  .finalAggregation()
                  .planNode();
  assertPlanConversion(plan, "SELECT c0, sum(c0), sum(c1) FROM tmp GROUP BY c0");
}

TEST_F(VeloxSubstraitRoundTripTest, sumMask) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .project({"c0", "c1", "c2 % 2 < 10 AS m0", "c3 % 3 = 0 AS m1"})
                  .partialAggregation({}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
                  .finalAggregation()
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT sum(c0) FILTER (WHERE c2 % 2 < 10), "
      "sum(c0) FILTER (WHERE c3 % 3 = 0), sum(c1) FILTER (WHERE c3 % 3 = 0) "
      "FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, rowConstructor) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<double_t>({0.905791934145, 0.968867771124}),
       makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775}),
       makeFlatVector<int32_t>({581869302, -133711905})});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder().values({vectors}).project({"row_constructor(c1, c2)"}).planNode();
  assertPlanConversion(plan, "SELECT row(c1, c2) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, projectAs) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<double_t>({0.905791934145, 0.968867771124}),
       makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775}),
       makeFlatVector<int32_t>({581869302, -133711905})});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder()
                  .values({vectors})
                  .filter("c0 < 0.5")
                  .project({"c1 * c2 as revenue"})
                  .partialAggregation({}, {"sum(revenue)"})
                  .planNode();
  assertPlanConversion(plan, "SELECT sum(c1 * c2) as revenue FROM tmp WHERE c0 < 0.5");
}

TEST_F(VeloxSubstraitRoundTripTest, avg) {
  GTEST_SKIP(); // Only partial step and single step of aggregation is currently supported.
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().values(vectors).partialAggregation({}, {"avg(c4)"}).finalAggregation().planNode();

  assertPlanConversion(plan, "SELECT avg(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, caseWhen) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan =
      PlanBuilder().values(vectors).project({"case when c0=1 then c1 when c0=2 then c2 else c3  end as x"}).planNode();

  assertPlanConversion(plan, "SELECT case when c0=1 then c1 when c0=2 then c2 else c3 end as x FROM tmp");

  // Switch expression without else.
  plan = PlanBuilder().values(vectors).project({"case when c0=1 then c1 when c0=2 then c2 end as x"}).planNode();
  assertPlanConversion(plan, "SELECT case when c0=1 then c1 when c0=2 then c2  end as x FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, ifThen) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).project({"if (c0=1, c0 + 1, c1 + 2) as x"}).planNode();
  assertPlanConversion(plan, "SELECT if (c0=1, c0 + 1, c1 + 2) as x FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, orderBySingleKey) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).orderBy({"c0 DESC NULLS LAST"}, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST");
}

TEST_F(VeloxSubstraitRoundTripTest, orderBy) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).orderBy({"c0 ASC NULLS FIRST", "c1 ASC NULLS LAST"}, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST");
}

TEST_F(VeloxSubstraitRoundTripTest, limit) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).limit(0, 10, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp LIMIT 10");

  // With offset.
  plan = PlanBuilder().values(vectors).limit(5, 10, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp OFFSET 5 LIMIT 10");
}

TEST_F(VeloxSubstraitRoundTripTest, topN) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).topN({"c0 NULLS FIRST"}, 10, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST LIMIT 10");
}

TEST_F(VeloxSubstraitRoundTripTest, topNFilter) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).filter("c0 > 15").topN({"c0 DESC NULLS FIRST"}, 10, false).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 DESC NULLS FIRST LIMIT 10");
}

TEST_F(VeloxSubstraitRoundTripTest, topNTwoKeys) {
  auto vectors = makeVectors(10, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 15")
                  .topN({"c0 NULLS FIRST", "c1 DESC NULLS LAST"}, 10, false)
                  .planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 NULLS FIRST, c1 DESC NULLS LAST LIMIT 10");
}

namespace {
core::TypedExprPtr makeConstantExpr(const TypePtr& type, const variant& value) {
  return std::make_shared<const core::ConstantTypedExpr>(type, value);
}

core::TypedExprPtr makeConstantExpr(const VectorPtr& vector) {
  return std::make_shared<const core::ConstantTypedExpr>(BaseVector::wrapInConstant(1, 0, vector));
}
} // namespace

TEST_F(VeloxSubstraitRoundTripTest, notNullLiteral) {
  auto vectors = makeRowVector(ROW({}, {}), 1);
  auto plan = PlanBuilder(pool_.get())
                  .values({vectors})
                  .addNode([&](std::string id, core::PlanNodePtr input) {
                    std::vector<std::string> projectNames = {"a", "b", "c", "d", "e", "f", "g", "h"};
                    std::vector<core::TypedExprPtr> projectExpressions = {
                        makeConstantExpr(BOOLEAN(), (bool)1),
                        makeConstantExpr(TINYINT(), (int8_t)23),
                        makeConstantExpr(SMALLINT(), (int16_t)45),
                        makeConstantExpr(INTEGER(), (int32_t)678),
                        makeConstantExpr(BIGINT(), (int64_t)910),
                        makeConstantExpr(REAL(), (float)1.23),
                        makeConstantExpr(DOUBLE(), (double)4.56),
                        makeConstantExpr(VARCHAR(), "789")};
                    return std::make_shared<core::ProjectNode>(
                        id, std::move(projectNames), std::move(projectExpressions), input);
                  })
                  .planNode();
  assertPlanConversion(plan, "SELECT true, 23, 45, 678, 910, 1.23, 4.56, '789'");
}

TEST_F(VeloxSubstraitRoundTripTest, arrayLiteral) {
  auto vectors = makeRowVector(ROW({}), 1);
  auto plan = PlanBuilder(pool_.get())
                  .values({vectors})
                  .addNode([&](std::string id, core::PlanNodePtr input) {
                    std::vector<core::TypedExprPtr> expressions = {
                        makeConstantExpr(makeNullableArrayVector<bool>({{true, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<int8_t>({{0, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<int16_t>({{1, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<int32_t>({{2, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<int64_t>({{3, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<float>({{4.4, std::nullopt}})),
                        makeConstantExpr(makeNullableArrayVector<double>({{5.5, std::nullopt}})),
                        makeConstantExpr(makeArrayVector<StringView>({{StringView("6")}})),
                        makeConstantExpr(makeArrayVector<Timestamp>({{Timestamp(123'456, 123'000)}})),
                        makeConstantExpr(makeArrayVector<int32_t>({{8035}}, DATE())),
                        makeConstantExpr(makeArrayVector<int64_t>({{54 * 1000}}, INTERVAL_DAY_TIME())),
                        makeConstantExpr(makeArrayVector<int64_t>({{}})),
                        // Nested array: [[1, 2, 3], [4, 5]]
                        makeConstantExpr(makeArrayVector({0}, makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}}))),
                    };
                    std::vector<std::string> names(expressions.size());
                    for (auto i = 0; i < names.size(); ++i) {
                      names[i] = fmt::format("e{}", i);
                    }
                    return std::make_shared<core::ProjectNode>(id, std::move(names), std::move(expressions), input);
                  })
                  .planNode();
  assertPlanConversion(
      plan,
      "SELECT array[true, null], array[0, null], array[1, null], "
      "array[2, null], array[3, null], array[4.4, null], array[5.5, null], "
      "array['6'],"
      "array['1970-01-02T10:17:36.000123000'::TIMESTAMP],"
      "array['1992-01-01'::DATE],"
      "array[INTERVAL 54 MILLISECONDS], "
      "array[], array[array[1,2,3], array[4,5]]");
}

TEST_F(VeloxSubstraitRoundTripTest, dateType) {
  auto a = makeFlatVector<int32_t>({0, 1});
  auto b = makeFlatVector<double_t>({0.3, 0.4});
  auto c = makeFlatVector<int32_t>({8036, 8035}, DATE());

  auto vectors = makeRowVector({"a", "b", "c"}, {a, b, c});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder().values({vectors}).filter({"c > DATE '1992-01-01'"}).planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp WHERE c > DATE '1992-01-01'");
}

TEST_F(VeloxSubstraitRoundTripTest, subField) {
  RowVectorPtr data = makeRowVector(
      {"a", "b", "c"},
      {
          makeFlatVector<int64_t>({249, 235, 858}),
          makeFlatVector<int32_t>({581, -708, -133}),
          makeFlatVector<double>({0.905, 0.968, 0.632}),
      });
  createDuckDbTable({data});
  auto plan = PlanBuilder()
                  .values({data})
                  .project({"cast(row_constructor(a, b) as row(a bigint, b bigint)) as ab", "a", "b", "c"})
                  .project({"cast(row_constructor(ab, c) as row(ab row(a bigint, b bigint), c bigint)) as abc"})
                  .project({"(abc).ab.a", "(abc).ab.b", "abc.c"})
                  .planNode();

  assertPlanConversion(plan, "SELECT a, b, c FROM tmp");

  plan =
      PlanBuilder().values({data}).project({"(cast(row_constructor(a, b) as row(a bigint, b bigint))).a"}).planNode();
  assertFailingPlanConversion(plan, "Non-field expression is not supported");
}

TEST_F(VeloxSubstraitRoundTripTest, sumCompanion) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().values(vectors).singleAggregation({}, {"sum_partial(1)", "count_partial(c4)"}).planNode();

  assertPlanConversion(plan, "SELECT sum(1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, sumAndCountCompanion) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .singleAggregation({}, {"sum_partial(c1)", "count_partial(c4)"})
                  .singleAggregation({}, {"sum_merge_extract(a0)", "count_merge_extract(a1)"})
                  .planNode();

  assertPlanConversion(plan, "SELECT sum(c1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, sumGlobalCompanion) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  // Global final aggregation.
  auto plan = PlanBuilder()
                  .values(vectors)
                  .singleAggregation({"c0"}, {"sum_partial(c0)", "sum_partial(c1)"})
                  .singleAggregation({"c0"}, {"sum_merge(a0)", "sum_merge(a1)"})
                  .singleAggregation({"c0"}, {"sum_merge_extract(a0)", "sum_merge_extract(a1)"})
                  .planNode();
  assertPlanConversion(plan, "SELECT c0, sum(c0), sum(c1) FROM tmp GROUP BY c0");
}

TEST_F(VeloxSubstraitRoundTripTest, sumMaskCompanion) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .project({"c0", "c1", "c2 % 2 < 10 AS m0", "c3 % 3 = 0 AS m1"})
                  .singleAggregation({}, {"sum_partial(c0)", "sum_partial(c0)", "sum_partial(c1)"}, {"m0", "m1", "m1"})
                  .singleAggregation({}, {"sum_merge_extract(a0)", "sum_merge_extract(a1)", "sum_merge_extract(a2)"})
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT sum(c0) FILTER (WHERE c2 % 2 < 10), "
      "sum(c0) FILTER (WHERE c3 % 3 = 0), sum(c1) FILTER (WHERE c3 % 3 = 0) "
      "FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripTest, projectAsCompanion) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<double_t>({0.905791934145, 0.968867771124}),
       makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775}),
       makeFlatVector<int32_t>({581869302, -133711905})});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder()
                  .values({vectors})
                  .filter("c0 < 0.5")
                  .project({"c1 * c2 as revenue"})
                  .singleAggregation({}, {"sum_partial(revenue)"})
                  .planNode();
  assertPlanConversion(plan, "SELECT sum(c1 * c2) as revenue FROM tmp WHERE c0 < 0.5");
}

TEST_F(VeloxSubstraitRoundTripTest, avgCompanion) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .singleAggregation({}, {"avg_partial(c4)"})
                  .singleAggregation({}, {"avg_merge_extract(a0)"})
                  .planNode();

  assertPlanConversion(plan, "SELECT avg(c4) FROM tmp");
}

} // namespace gluten

int main(int argc, char** argv) {
  gluten::registerAllFunctions();
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
