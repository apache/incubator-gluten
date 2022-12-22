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

package io.glutenproject.utils.velox

import io.glutenproject.utils.BackendTestSettings
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._

object VeloxTestSettings extends BackendTestSettings {
  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude(
      "zero moments", // [velox does not return NaN]
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      "rollup overlapping columns", // wait velox to fix
      "cube overlapping columns", // wait velox to fix
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate"
    )

  enableSuite[GlutenCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )

  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOff]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )

  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOn]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )

  enableSuite[GlutenCastSuiteWithAnsiModeOn]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )

  enableSuite[GlutenTryCastSuite]
    .exclude(
      // array/map/struct not supported yet.
      "cast from invalid string array to numeric array should throw NumberFormatException",
      "cast from array II",
      "cast from map II",
      "cast from struct II"
    )
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
       // NaN case
      "replace nan with float",
      "replace nan with double"
    )

  enableSuite[GlutenDataFrameRangeSuite]
    .exclude(
      // don't know why, corner case
      "SPARK-21041 SparkSession.range()'s behavior is inconsistent with SparkContext.range()" +
        " (whole-stage-codegen on)"
    )

  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff].exclude(
    // overwritten
    "DPP should not be rewritten as an existential join",
    "no partition pruning when the build side is a stream",
    "Make sure dynamic pruning works on uncorrelated queries",
    "SPARK-32509: Unused Dynamic Pruning filter shouldn't affect " +
      "canonicalization and exchange reuse",
    "Subquery reuse across the whole plan",
    "static scan metrics",

    // to be fixed
    "SPARK-32659: Fix the data issue when pruning DPP on non-atomic type",
    "partition pruning in broadcast hash joins with aliases",
    "broadcast multiple keys in an UnsafeHashedRelation",
    "different broadcast subqueries with identical children",
    "avoid reordering broadcast join keys to match input hash partitioning",
    "dynamic partition pruning ambiguity issue across nested joins",
    "Plan broadcast pruning only when the broadcast can be reused",
    GLUTEN_TEST + "Subquery reuse across the whole plan"
  )

  enableSuite[GlutenLiteralExpressionSuite]
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenHashExpressionsSuite]
  enableSuite[GlutenCollectionExpressionsSuite]
  enableSuite[GlutenDateExpressionsSuite]
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenStringFunctionsSuite]
  enableSuite[GlutenRegexpExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
  enableSuite[GlutenMathExpressionsSuite]
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
  enableSuite[GlutenRandomSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude(
      "% (Remainder)" // Velox will throw exception when right is zero
    )
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenDateFunctionsSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
  enableSuite[GlutenDataFrameTungstenSuite]
  enableSuite[GlutenDataFrameSetOperationsSuite]
  enableSuite[GlutenDataFrameStatSuite]
  enableSuite[GlutenComplexTypesSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenApproximatePercentileQuerySuite]
  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893", // Rewrite this test because it checks Spark's physical operators.
      "SPARK-32290" // Need to be removed after picking Velox #3571.
    )
  enableSuite[GlutenDataFrameWindowFramesSuite]
  enableSuite[GlutenColumnExpressionSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenGeneratorFunctionSuite]
  enableSuite[GlutenDataFrameTimeWindowingSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
  enableSuite[GlutenBroadcastExchangeSuite]
  enableSuite[GlutenDataFramePivotSuite]
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenSameResultSuite]
  // spill not supported yet.
  enableSuite[GlutenSQLWindowFunctionSuite].exclude("test with low buffer spill threshold")

}
