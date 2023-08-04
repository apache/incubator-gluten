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
import org.apache.spark.sql.extension.{GlutenCustomerExpressionTransformerSuite, GlutenCustomerExtensionSuite, GlutenSessionExtensionSuite}

class ClickHouseTestSettings extends BackendTestSettings {

  /** Enable All expression UT. */
  enableSuite[GlutenCastSuiteWithAnsiModeOn]
    .exclude(
      "SPARK-35711: cast timestamp without time zone to timestamp with local time zone",
      "SPARK-35719: cast timestamp with local time zone to timestamp without timezone"
    )
  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOn]
    .exclude(
      "SPARK-35711: cast timestamp without time zone to timestamp with local time zone",
      "SPARK-35719: cast timestamp with local time zone to timestamp without timezone"
    )
  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOff]
    .exclude(
      "cast string to date",
      "SPARK-35711: cast timestamp without time zone to timestamp with local time zone",
      "SPARK-35719: cast timestamp with local time zone to timestamp without timezone"
    )
  enableSuite[GlutenTryCastSuite]
    .exclude(
      "cast string to date",
      "SPARK-35711: cast timestamp without time zone to timestamp with local time zone",
      "SPARK-35719: cast timestamp with local time zone to timestamp without timezone"
    )
  enableSuite[GlutenArithmeticExpressionSuite].exclude(
    "/ (Divide) basic",
    "% (Remainder)", // CH will throw exception when right is zero
    "SPARK-17617: % (Remainder) double % double on super big double",
    "pmod"
  )
  enableSuite[GlutenBitwiseExpressionsSuite]
    .exclude("BitGet")
  enableSuite[GlutenCastSuite]
    .exclude(
      "cast string to date",
      "cast string to timestamp",
      "cast from boolean",
      "data type casting",
      "cast struct with a timestamp field",
      "SPARK-27671: cast from nested null type in struct",
      "Process Infinity, -Infinity, NaN in case insensitive manner",
      "SPARK-22825 Cast array to string",
      "SPARK-33291: Cast array with null elements to string",
      "SPARK-22973 Cast map to string",
      "SPARK-22981 Cast struct to string",
      "SPARK-33291: Cast struct with null elements to string",
      "SPARK-35711: cast timestamp without time zone to timestamp with local time zone",
      "SPARK-35719: cast timestamp with local time zone to timestamp without timezone",
      "null cast #2",
      "cast string to date #2",
      "casting to fixed-precision decimals",
      "SPARK-28470: Cast should honor nullOnOverflow property",
      "cast string to boolean II",
      "cast from array II",
      "cast from map II",
      "cast from struct II",
      "cast from date",
      "cast from timestamp",
      "cast a timestamp before the epoch 1970-01-01 00:00:00Z",
      "SPARK-34727: cast from float II"
    )
  enableSuite[GlutenCollectionExpressionsSuite]
    .exclude(
      "Array and Map Size - legacy",
      "Sequence of numbers",
      "Reverse",
      "elementAt",
      "Concat",
      "Shuffle",
      "Array Distinct", // exclude this when fix GLUTEN-2340
      "SPARK-33386: element_at ArrayIndexOutOfBoundsException",
      "SPARK-33460: element_at NoSuchElementException"
    )
    // enable when fix GLUTEN-2390
    .excludeByPrefix("SPARK-36740")
  enableSuite[GlutenComplexTypeSuite]
    .exclude(
      "CreateNamedStruct",
      "SPARK-33386: GetArrayItem ArrayIndexOutOfBoundsException",
      "SPARK-33460: GetMapValue NoSuchElementException",
      "CreateMap",
      "MapFromArrays"
    )
  enableSuite[GlutenConditionalExpressionSuite]
    .exclude(
      "case when",
      "if/case when - null flags of non-primitive types"
    )
  enableSuite[GlutenDateExpressionsSuite]
    .exclude(
      "DayOfYear",
      "Year",
      "Quarter",
      "Month",
      "Day / DayOfMonth",
      "DayOfWeek",
      "WeekDay",
      "WeekOfYear",
      "DateFormat",
      "Hour",
      "add_months",
      "TruncDate",
      "unsupported fmt fields for trunc/date_trunc results null",
      "from_unixtime",
      "unix_timestamp",
      GLUTEN_TEST + "unix_timestamp",
      "to_unix_timestamp",
      GLUTEN_TEST + "to_unix_timestamp",
      "to_timestamp exception mode",
      "SPARK-31896: Handle am-pm timestamp parsing when hour is missing",
      "TIMESTAMP_MICROS",
      "SPARK-33498: GetTimestamp,UnixTimestamp,ToUnixTimestamp with parseError",
      "months_between" // Clickhouse donot support timezone string like "-8:00, +2:00, etc" for now
    )
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenHashExpressionsSuite]
    .exclude(
      "SPARK-30633: xxHash with different type seeds",
      "SPARK-35207: Compute hash consistent between -0.0 and 0.0"
    )
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenJsonExpressionsSuite]
    .exclude(
      "$.store.bicycle",
      "$['store'].bicycle",
      "$.store['bicycle']",
      "$['store']['bicycle']",
      "$.store.book",
      "$.store.book[0]",
      "$.store.book[*].reader",
      "$.store.book[*]",
      "$",
      "$.store.book[*].category",
      "$.store.book[*].isbn",
      "$.store.basket[*]",
      "$.store.basket[0][1]",
      "$.store.basket[*][0]",
      "$.store.basket[0][*]",
      "$.store.basket[*][*]",
      "$.store.basket[0][*].b",
      "$.zip code",
      "$.fb:testid",
      "$..no_recursive",
      "from_json - invalid data",
      "from_json - input=object, schema=array, output=array of single row",
      "from_json - input=empty array, schema=array, output=empty array",
      "from_json - input=empty object, schema=array, output=array of single row with null",
      "from_json - input=array of single object, schema=struct, output=single row",
      "from_json - input=array, schema=struct, output=single row",
      "from_json - input=empty array, schema=struct, output=single row with null",
      "from_json - input=empty object, schema=struct, output=single row with null",
      "SPARK-20549: from_json bad UTF-8",
      "from_json with timestamp",
      "to_json - struct",
      "to_json - array",
      "to_json - array with single empty row",
      "to_json with timestamp",
      "SPARK-21513: to_json support map[string, struct] to json",
      "SPARK-21513: to_json support map[struct, struct] to json",
      "from_json missing fields",
      "parse date with locale",
      "parse decimals using locale",
      "escape",
      "preserve newlines"
    )
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude(
      "null",
      "Gluten - default",
      "default",
      "decimal",
      "array",
      "seq", // 'Invalid Field get from type Decimal64 to type Int64
      "map",
      "struct",
      "construct literals from java.time.Instant",
      "construct literals from arrays of java.time.Instant"
    )
  enableSuite[GlutenMathExpressionsSuite]
    .exclude(
      "",
      "tanh",
      "ceil",
      "floor",
      "factorial",
      "log", // nan null
      "log10",
      "log1p",
      "log2",
      "unhex",
      "atan2",
      "round/bround"
    )
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude(
      "MonotonicallyIncreasingID",
      "SparkPartitionID"
    )
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
    .exclude(
      "IN/INSET: struct",
      "BinaryComparison: lessThan",
      "BinaryComparison: LessThanOrEqual",
      "BinaryComparison: GreaterThan",
      "BinaryComparison: GreaterThanOrEqual",
      "EqualTo on complex type",
      "SPARK-32764: compare special double/float values",
      "SPARK-32110: compare special double/float values in array",
      "SPARK-32110: compare special double/float values in struct"
    )
  enableSuite[GlutenRandomSuite]
    .exclude(
      "random",
      "SPARK-9127 codegen with long seed"
    )
  enableSuite[GlutenRegexpExpressionsSuite]
    .exclude(
      "LIKE Pattern",
      "LIKE Pattern ESCAPE '/'",
      "LIKE Pattern ESCAPE '#'",
      "LIKE Pattern ESCAPE '\"'",
      "RLIKE Regular Expression",
      "RegexReplace",
      "RegexExtract",
      "RegexExtractAll",
      "SPLIT"
    )
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
    .exclude(
      "concat",
      "concat_ws",
      "SPARK-22549: ConcatWs should not generate codes beyond 64KB",
      "StringComparison",
      "Substring",
      "ascii for string",
      "string for ascii",
      "replace",
      "translate",
      "INSTR",
      "LOCATE",
      "LPAD/RPAD",
      "REPEAT",
      "length for string / binary",
      "SPARK-40213: ascii for Latin-1 Supplement characters",
      "SPARK-40213: ascii for Latin-1 Supplement characters",
      "SPARK-33468: ParseUrl in ANSI mode should fail if input string is not a valid url",
      "ParseUrl"
    )

  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[GlutenCustomerExtensionSuite]
  enableSuite[GlutenCustomerExpressionTransformerSuite]

  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude(
      "average", // [overwritten by Gluten - xxx]
      "multiple column distinct count", // [not urgent, function with multiple params]
      "agg without groups and functions", // [not urgent]
      // collect_set is non-deterministic,
      // the order of the collection elements returned by CH is different from Spark.
      "collect functions",
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
      GLUTEN_TEST + "use gluten hash agg to replace vanilla spark sort agg",
      "zero average",
      "zero stddev"
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
      "array size function - legacy",
      "array size function",
      "map size function - legacy",
      "map size function",
      "map_entries",
      "misc md5 function",
      "misc sha1 function",
      "misc sha2 function",
      "misc crc32 function",
      "concat function - arrays",
      "array_max function",
      "array_min function",
      "array position function",
      "array contains function",
      "array_distinct functions",
      "array_union functions"
    )
    .includeByPrefix(
      "reverse function - array"
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
      "datediff",
      "function date_trunc",
      "function months_between"
    )

  enableSuite[GlutenMathFunctionsSuite]
    .exclude(
      "hex",
      "log1p"
    )

  enableSuite[GlutenComplexTypesSuite]

  // enableSuite[GlutenRegexpExpressionsSuite]
  //   .include(
  //     "SPLIT",
  //     "RLIKE Regular Expression"
  //     "RegexExtract"
  //   )

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
      "lead/lag with ignoreNulls",
      "Window spill with less than the inMemoryThreshold",
      "Window spill with more than the inMemoryThreshold and spillThreshold",
      "Window spill with more than the inMemoryThreshold but less than the spillThreshold",
      "NaN and -0.0 in window partition keys",
      "last/first with ignoreNulls",
      "SPARK-21258: complex object in combination with spilling",
      "Gluten - corr, covar_pop, stddev_pop functions in specific window",
      "corr, covar_pop, stddev_pop functions in specific window",
      "covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window",
      "SPARK-13860: " + "corr, covar_pop, stddev_pop functions in specific window " +
        "LEGACY_STATISTICAL_AGGREGATE off",
      "SPARK-13860: " + "covar_samp, var_samp (variance), stddev_samp (stddev) " +
        "functions in specific window LEGACY_STATISTICAL_AGGREGATE off"
    )

  enableSuite[GlutenDataFrameRangeSuite]
    .includeByPrefix("SPARK-21041")

  enableSuite[GlutenStringFunctionsSuite]
    .include("initcap function")
}
