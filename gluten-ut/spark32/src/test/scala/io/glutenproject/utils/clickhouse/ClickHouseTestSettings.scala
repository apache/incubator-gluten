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

package io.glutenproject.utils.clickhouse

import io.glutenproject.utils.BackendTestSettings
import org.apache.spark.sql._
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FallbackStrategiesSuite, GlutenExchangeSuite, GlutenReuseExchangeAndSubquerySuite}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.extension.{GlutenCustomerExtensionSuite, GlutenSessionExtensionSuite}

class ClickHouseTestSettings extends BackendTestSettings {
  /*
  ExpressionSuite
   */
  enableSuite[GlutenDecimalExpressionSuite]

  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[GlutenCustomerExtensionSuite]

  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude(
      "average", // [overwritten by Gluten - xxx]
      "multiple column distinct count", // [not urgent, function with multiple params]
      "agg without groups and functions", // [not urgent]
      "collect functions structs", // [not urgent]
      "SPARK-31500: collect_set() of BinaryType returns duplicate elements", // [not urgent]
      "SPARK-17641: collect functions should not collect null values", // [not urgent]
      "collect functions should be able to cast to array type with no null values", // [not urgent]
      "SQL decimal test (used for catching certain decimal " +
        "handling bugs in aggregates)", // [wishlist] support decimal
      "SPARK-17616: distinct aggregate combined with a non-partial aggregate", // [not urgent]
      "SPARK-17237 remove backticks in a pivot result schema", // [not urgent]
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
      " before using it", // [not urgent]
      "max_by", // [not urgent]
      "min_by", // [not urgent]
      "aggregation with filter",
      // replaceSortAggWithHashAgg is not turned on for CH backend.
      GLUTEN_TEST + "use gluten hash agg to replace vanilla spark sort agg"
    )
    .excludeByPrefix(
      "SPARK-22951", // [not urgent] dropDuplicates
      "SPARK-26021", // [not urgent] behavior on NaN and -0.0 are different
      "SPARK-32136", // [not urgent] struct type
      "SPARK-32344", // [not urgent] FIRST/LAST
      "SPARK-34713", // [not urgent] struct type
      "SPARK-34716", // [not urgent] interval
      "SPARK-34837", // [not urgent] interval
      "SPARK-35412", // [not urgent] interval
      "SPARK-36926", // [wishlist] support decimal
      "SPARK-38185", // [not urgent] empty agg
      "SPARK-32038" // [not urgent]
    )

  enableSuite[GlutenDataFrameFunctionsSuite]
    .include(
      "conditional function: least",
      "conditional function: greatest",
      "array size function",
      "map size function",
      "map_entries"
    )

  enableSuite[GlutenDateFunctionsSuite]
    .include(
      "quarter",
      "second",
      "dayofmonth",
      "function add_months",
      "date format",
      "function date_add",
      "function date_sub",
      "datediff"
    )

  enableSuite[GlutenDateExpressionsSuite]
    .include(
      // "Quarter", // ch backend not support cast 'yyyy-MM-dd HH:mm:ss' as date32
      "date_add",
      "date_sub",
      "datediff"
      // "add_months" // ch date32 year's range in [1900~2299].Not support 0001-01-01
      // "DateFormat" // ch formatDateTimeInJodaSyntax doesn't support non-constant format argument
    )


  enableSuite[GlutenMathFunctionsSuite]
    .exclude(
      "hex",
      "log1p"
    )

  enableSuite[GlutenComplexTypesSuite]
  enableSuite[GlutenComplexTypeSuite]
    .exclude(
      "CreateMap",
      "MapFromArrays",
      "SPARK-33460: GetMapValue NoSuchElementException"
    )
    .excludeByPrefix(
      "SPARK-33386" // different result: actual: empty excepted: null
    )
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude(
      "- (UnaryMinus)",
      "/ (Divide) basic",
      "/ (Divide) for Long and Decimal type",
      "% (Remainder)", // CH will throw exception when right is zero
      "SPARK-17617: % (Remainder) double % double on super big double",
      "pmod",
      "SPARK-28322: IntegralDivide supports decimal type",
      "SPARK-33008: division by zero on divide-like operations returns incorrect result",
      "SPARK-34920: error class"
    )

  // enableSuite[GlutenRegexpExpressionsSuite]
  //   .include(
  //     "SPLIT",
  //     "RLIKE Regular Expression"
  //     "RegexExtract"
  //   )

  enableSuite[GlutenJsonExpressionsSuite]
    .include(
      "Length of JSON array"
    )

  enableSuite[GlutenStringExpressionsSuite]
    .include(
      "concat_ws",
      // "LPAD/RPAD", not ready because CH required the third arg to be constant string
      // "translate" Not ready because CH requires from and to argument have the same length
      "REVERSE"
    )

  enableSuite[GlutenStringFunctionsSuite]
    .include(
      "string concat_ws"
      // "string translate" Not ready because CH requires from and to argument have the same length
    )

  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893", // Rewrite this test because it checks Spark's physical operators.
      "SPARK-32290",
      "SPARK-15832",
      "SPARK-15370",
      "SPARK-16804",
      "SPARK-17337",
      "SPARK-28441",
      "SPARK-28379",
      "SPARK-17348" // TODO: loneylee ch: INVALID_JOIN_ON_EXPRESSION
    )
    .exclude(
      "Correlated subqueries in LATERAL VIEW",
      "NOT EXISTS predicate subquery",
      "NOT IN predicate subquery",
      "disjunctive correlated scalar subquery",
      "IN predicate subquery" // TODO: loneylee ch: INVALID_JOIN_ON_EXPRESSION
    )

  enableSuite[GlutenDataFramePivotSuite]
    .include(
      "pivot with datatype not supported by PivotFirst",
      "pivot with datatype not supported by PivotFirst 2",
      "pivot with null and aggregate type not supported by PivotFirst returns correct result",
      "optimized pivot DecimalType"
    )

  enableSuite[GlutenPredicateSuite]
    .includeByPrefix(
      "SPARK-29100"
    )

  enableSuite[GlutenColumnExpressionSuite]
    .include(
      "IN/INSET with bytes, shorts, ints, dates"
    )

  enableSuite[GlutenJoinSuite]
    .include(
      "big inner join, 4 matches per row",
      "cartesian product join",
      "equi-join is hash-join",
      "inner join, no matches",
      "inner join, where, multiple matches",
      "inner join ON, one match per row",
      "inner join where, one match per row",
      "left semi join",
      "multiple-key equi-join is hash-join",
      "full outer join",
      GlutenTestConstants.GLUTEN_TEST + "test case sensitive for BHJ"
    )

  enableSuite[GlutenHashExpressionsSuite]
    .include(
      "md5",
      "sha1",
      "sha2",
      "crc32"
    )

  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
    .exclude(
      "Gluten - SPARK-32659: Fix the data issue when pruning DPP on non-atomic type"
    )
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
    .exclude(
      "Gluten - SPARK-32659: Fix the data issue when pruning DPP on non-atomic type"
    )
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
    .exclude(
      "Gluten - SPARK-32659: Fix the data issue when pruning DPP on non-atomic type"
    )
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]
    .exclude(
      "Gluten - SPARK-32659: Fix the data issue when pruning DPP on non-atomic type"
    )
  enableSuite[FallbackStrategiesSuite]

  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeExec does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeExec does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenReuseExchangeAndSubquerySuite]

  enableSuite[GlutenDataFrameWindowFramesSuite]
    .exclude(
      "rows between should accept int/long values as boundary",
      "reverse preceding/following range between with aggregation",
      "SPARK-24033: Analysis Failure of OffsetWindowFunction"
    )
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    .exclude(
      "reuse window partitionBy",
      "reuse window orderBy",
      "collect_list in ascending ordered window",
      "collect_list in descending ordered window",
      "collect_set in window",
      "lead/lag with ignoreNulls",
      "Window spill with less than the inMemoryThreshold",
      "Window spill with more than the inMemoryThreshold and spillThreshold",
      "Window spill with more than the inMemoryThreshold but less than the spillThreshold",
      "NaN and -0.0 in window partition keys"
    )
}

