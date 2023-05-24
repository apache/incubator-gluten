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
import org.apache.spark.sql.catalyst.expressions.{GlutenAnsiCastSuiteWithAnsiModeOff, GlutenAnsiCastSuiteWithAnsiModeOn, GlutenArithmeticExpressionSuite, GlutenBitwiseExpressionsSuite, GlutenCastSuite, GlutenCastSuiteWithAnsiModeOn, GlutenCollectionExpressionsSuite, GlutenComplexTypeSuite, GlutenConditionalExpressionSuite, GlutenDateExpressionsSuite, GlutenDecimalExpressionSuite, GlutenHashExpressionsSuite, GlutenIntervalExpressionsSuite, GlutenLiteralExpressionSuite, GlutenMathExpressionsSuite, GlutenMiscExpressionsSuite, GlutenNondeterministicSuite, GlutenNullExpressionsSuite, GlutenPredicateSuite, GlutenRandomSuite, GlutenRegexpExpressionsSuite, GlutenSortOrderExpressionsSuite, GlutenStringExpressionsSuite, GlutenTryCastSuite}
import org.apache.spark.sql.connector.{GlutenDataSourceV2DataFrameSessionCatalogSuite, GlutenDataSourceV2DataFrameSuite, GlutenDataSourceV2FunctionSuite, GlutenDataSourceV2SQLSessionCatalogSuite, GlutenDataSourceV2SQLSuite, GlutenDataSourceV2Suite, GlutenFileDataSourceV2FallBackSuite, GlutenLocalScanSuite, GlutenSupportsCatalogOptionsSuite, GlutenTableCapabilityCheckSuite, GlutenWriteDistributionAndOrderingSuite}
import org.apache.spark.sql.{GlutenBloomFilterAggregateQuerySuite, GlutenJsonFunctionsSuite, GlutenStringFunctionsSuite}

class VeloxTestSettings extends BackendTestSettings {
  enableSuite[GlutenStringFunctionsSuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
    // fallback might_contain, the input argument binary is not same with vanilla spark
    .exclude("Test NULL inputs for might_contain")
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuite]
  enableSuite[GlutenDataSourceV2Suite]
    // Gluten does not support the convert from spark columnar data
    // to velox columnar data.
    .exclude("columnar batch scan implementation")
    // Rewrite the following test in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]

  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOff]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")

  enableSuite[GlutenAnsiCastSuiteWithAnsiModeOn]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")

  enableSuite[GlutenCastSuiteWithAnsiModeOn]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
  enableSuite[GlutenTryCastSuite]
    .exclude(
      // array/map/struct not supported yet.
      "cast from invalid string array to numeric array should throw NumberFormatException",
      "cast from array II",
      "cast from map II",
      "cast from struct II"
    )
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("Fast fail for cast string type to decimal type in ansi mode")
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude(
      "% (Remainder)" // Velox will throw exception when right is zero, need fallback
    )
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("cast from float")
    .exclude("cast from double")
    .exclude("from decimal")
    .exclude("cast string to date #2")
    .exclude("casting to fixed-precision decimals")
    .exclude("cast from date")
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type")
    .exclude("Fast fail for cast string type to decimal type")
    .exclude("missing cases - from boolean")
  enableSuite[GlutenCollectionExpressionsSuite]
    .exclude("Map Concat")
    .exclude("Shuffle")
  enableSuite[GlutenComplexTypeSuite]
    .exclude("CreateMap")
    .exclude("MapFromArrays")
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDateExpressionsSuite]
    // Has exception in fallback execution when we use resultDF.collect in evaluation.
    .exclude("DATE_FROM_UNIX_DATE", "TIMESTAMP_MICROS")
    .exclude("DayOfYear")
    .exclude("Year")
    .exclude("Quarter")
    .exclude("Month")
    .exclude("Day / DayOfMonth")
    .exclude("DayOfWeek")
    .exclude("extract the seconds part with fraction from timestamps")
  enableSuite[GlutenDecimalExpressionSuite]
    .exclude("MakeDecimal")
  enableSuite[GlutenHashExpressionsSuite]
    .exclude("SPARK-30633: xxHash with different type seeds")
  enableSuite[GlutenIntervalExpressionsSuite]
    .exclude("seconds")
    .exclude("ANSI: extract days, hours, minutes and seconds")
  enableSuite[GlutenJsonFunctionsSuite]
    // Velox does not support single quotes in get_json_object function.
    .exclude("function get_json_object - support single quotes")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    .exclude("decimal")
    // FIXME(yma11): ObjectType is not covered in RowEncoder/Serializer in vanilla spark
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenMathExpressionsSuite]
    .exclude("cos")
    .exclude("cosh")
    .exclude("toDegrees")
    .exclude("toRadians")
    .exclude("cbrt")
    .exclude("exp")
    .exclude("log10")
    .exclude("log2")
    .exclude("pow")
    .exclude("atan2")
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
    .exclude("BinaryComparison: lessThan")
    .exclude("BinaryComparison: LessThanOrEqual")
    .exclude("BinaryComparison: GreaterThan")
    .exclude("BinaryComparison: GreaterThanOrEqual")
    .exclude("SPARK-32764: compare special double/float values")
  enableSuite[GlutenRandomSuite]
    .exclude("random")
    .exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
    .exclude("Substring")
    .exclude("string for ascii")
    .exclude("replace")
}
