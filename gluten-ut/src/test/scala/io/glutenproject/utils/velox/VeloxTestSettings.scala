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
import org.apache.spark.sql.execution.joins.{GlutenExistenceJoinSuite, GlutenOuterJoinSuite, GlutenInnerJoinSuite}

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
  enableSuite[GlutenDataFrameSuite]
    // Rewrite these tests because it checks Spark's physical operators.
    .excludeByPrefix(
      "SPARK-22520",
      "reuse exchange"
    )
    .exclude(
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      /**
       * Mismatched, the rdd partition is equal to 1
       * because the configuration "spark.sql.shuffle.partitions" is always 1.
       */
      "repartitionByRange",
      "distributeBy and localSort",
      // To be fixed
      "describe"
    )

  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
       // NaN case
      "replace nan with float",
      "replace nan with double"
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
    "dynamic partition pruning ambiguity issue across nested joins"
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
      "% (Remainder)" // Velox will throw exception when right is zero, need fallback
    )
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    // Spill not supported yet.
    .exclude("Window spill with more than the inMemoryThreshold and spillThreshold")
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
  enableSuite[GlutenDataFrameRangeSuite]
  enableSuite[GlutenTakeOrderedAndProjectSuite]
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
  enableSuite[GlutenSortSuite]
      // Sort spill is not supported.
      .exclude("sorting does not crash for large inputs")
  enableSuite[GlutenExistenceJoinSuite]
  enableSuite[GlutenDataFrameJoinSuite]
  enableSuite[GlutenOuterJoinSuite]
  enableSuite[GlutenInnerJoinSuite]
    // The following tests will be re-run in GlutenInnerJoinSuite by
    // changing the struct schema from "struct(id, id) as key" to
    // "struct(id as id1, id as id2) as key". Because the first
    // Struct child will be covered with the second same name id.
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin" +
      " (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin" +
      " (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin" +
      " (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin" +
      " (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin" +
      " (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin" +
      " (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin" +
      " (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin" +
      " (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeAdaptor does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeAdaptor does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
}
