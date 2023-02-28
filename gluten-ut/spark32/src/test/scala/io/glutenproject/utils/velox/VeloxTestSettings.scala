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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.{GlutenDataSourceV2DataFrameSessionCatalogSuite, GlutenDataSourceV2DataFrameSuite, GlutenDataSourceV2FunctionSuite, GlutenDataSourceV2SQLSessionCatalogSuite, GlutenDataSourceV2SQLSuite, GlutenDataSourceV2Suite, GlutenFileDataSourceV2FallBackSuite, GlutenLocalScanSuite, GlutenSupportsCatalogOptionsSuite, GlutenTableCapabilityCheckSuite, GlutenWriteDistributionAndOrderingSuite}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.GlutenAdaptiveQueryExecSuite
import org.apache.spark.sql.execution.datasources.{GlutenBucketingUtilsSuite, GlutenCSVReadSchemaSuite, GlutenDataSourceStrategySuite, GlutenDataSourceSuite, GlutenFileFormatWriterSuite, GlutenFileIndexSuite, GlutenFileSourceStrategySuite, GlutenHadoopFileLinesReaderSuite, GlutenHeaderCSVReadSchemaSuite, GlutenJsonReadSchemaSuite, GlutenMergedOrcReadSchemaSuite, GlutenMergedParquetReadSchemaSuite, GlutenOrcCodecSuite, GlutenOrcReadSchemaSuite, GlutenParquetCodecSuite, GlutenParquetReadSchemaSuite, GlutenPathFilterStrategySuite, GlutenPathFilterSuite, GlutenPruneFileSourcePartitionsSuite, GlutenVectorizedOrcReadSchemaSuite, GlutenVectorizedParquetReadSchemaSuite}
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.csv.{GlutenCSVLegacyTimeParserSuite, GlutenCSVv1Suite, GlutenCSVv2Suite}
import org.apache.spark.sql.execution.datasources.json.{GlutenJsonLegacyTimeParserSuite, GlutenJsonV1Suite, GlutenJsonV2Suite}
import org.apache.spark.sql.execution.datasources.orc.{GlutenOrcColumnarBatchReaderSuite, GlutenOrcFilterSuite, GlutenOrcPartitionDiscoverySuite, GlutenOrcSourceSuite, GlutenOrcV1FilterSuite, GlutenOrcV1PartitionDiscoverySuite, GlutenOrcV1QuerySuite, GlutenOrcV1SchemaPruningSuite, GlutenOrcV2QuerySuite, GlutenOrcV2SchemaPruningSuite}
import org.apache.spark.sql.execution.datasources.parquet.{GlutenParquetColumnIndexSuite, GlutenParquetCompressionCodecPrecedenceSuite, GlutenParquetEncodingSuite, GlutenParquetFileFormatV1Suite, GlutenParquetFileFormatV2Suite, GlutenParquetIOSuite, GlutenParquetInteroperabilitySuite, GlutenParquetProtobufCompatibilitySuite, GlutenParquetRebaseDatetimeV1Suite, GlutenParquetRebaseDatetimeV2Suite, GlutenParquetSchemaInferenceSuite, GlutenParquetSchemaSuite, GlutenParquetThriftCompatibilitySuite, GlutenParquetV1FilterSuite, GlutenParquetV1PartitionDiscoverySuite, GlutenParquetV1QuerySuite, GlutenParquetV1SchemaPruningSuite, GlutenParquetV2FilterSuite, GlutenParquetV2PartitionDiscoverySuite, GlutenParquetV2QuerySuite, GlutenParquetV2SchemaPruningSuite}
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.GlutenFileTableSuite
import org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite
import org.apache.spark.sql.execution.joins.{GlutenBroadcastJoinSuite, GlutenExistenceJoinSuite, GlutenInnerJoinSuite, GlutenOuterJoinSuite}
import org.apache.spark.sql.sources.{GlutenBucketedReadWithoutHiveSupportSuite, GlutenBucketedWriteWithoutHiveSupportSuite, GlutenCreateTableAsSelectSuite, GlutenDDLSourceLoadSuite, GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite, GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE, GlutenExternalCommandRunnerSuite, GlutenFilteredScanSuite, GlutenFiltersSuite, GlutenInsertSuite, GlutenPartitionedWriteSuite, GlutenPathOptionSuite, GlutenPrunedScanSuite, GlutenResolvedDataSourceSuite, GlutenSaveLoadSuite, GlutenTableScanSuite}

class VeloxTestSettings extends BackendTestSettings {
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
      /**
       * Rewrite these tests because the rdd partition is equal to
       * the configuration "spark.sql.shuffle.partitions".
       */
      "repartitionByRange",
      "distributeBy and localSort",
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      // Rewrite this test because the describe functions creates unmatched plan.
      "describe"
    )

  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
       // NaN case
      "replace nan with float",
      "replace nan with double"
    )

  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]

  enableSuite[GlutenAdaptiveQueryExecSuite]
    .includeByPrefix(
    "gluten", "SPARK-29906", "SPARK-30291", "SPARK-30403", "SPARK-30719", "SPARK-31384",
      "SPARK-30953", "SPARK-31658", "SPARK-32717", "SPARK-32649", "SPARK-34533",
      "SPARK-34781", "SPARK-35585", "SPARK-32932", "SPARK-33494", "SPARK-33933", "SPARK-31220",
    "SPARK-35874", "SPARK-39551")
    .include("Union/Except/Intersect queries",
      "Subquery de-correlation in Union queries",
      "force apply AQE",
      "tree string output",
    "control a plan explain mode in listener vis SQLConf",
    "AQE should set active session during execution",
    "No deadlock in UI update",
    "SPARK-35455: Unify empty relation optimization between normal and AQE optimizer - multi join")

  enableSuite[GlutenLiteralExpressionSuite]
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenHashExpressionsSuite]
  enableSuite[GlutenCollectionExpressionsSuite]
  enableSuite[GlutenDateExpressionsSuite]
      // Has exception in fallback execution when we use resultDF.collect in evaluation.
      .exclude("DATE_FROM_UNIX_DATE", "TIMESTAMP_MICROS")
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
    .exclude("NaN and -0.0 in window partition keys") // NaN case
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
      "SPARK-26893" // Rewrite this test because it checks Spark's physical operators.
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
  enableSuite[GlutenJoinSuite]
  enableSuite[GlutenOuterJoinSuite]
  enableSuite[GlutenInnerJoinSuite]
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeAdaptor does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeAdaptor does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenBroadcastJoinSuite]
  enableSuite[GlutenSQLQuerySuite]
    // Unstable. Needs to be fixed.
    .exclude("SPARK-36093: RemoveRedundantAliases should not change expression's name")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported.
    .exclude("SPARK-28156: self-join should not miss cached view")
    .exclude("SPARK-33338: GROUP BY using literal map should not fail")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported. Also disabled
    // Spark vectorized reading because Spark's columnar output is not compatible with Velox's.
    .exclude("SPARK-33593: Vector reader got incorrect data with binary partition value")
    // Rewrite to disable plan check for SMJ because SHJ is preferred in Gluten.
    .exclude("SPARK-11111 null-safe join should not use cartesian product")
    // Rewrite to change the information of a caught exception.
    .exclude(
      "SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    // Different exception.
    .exclude("run sql directly on files")
    // Columnar shuffle cannot generate the expected number of partitions if the row of a input
    // batch is less than the expected number of partitions.
    .exclude("SPARK-24940: coalesce and repartition hint")
    // Not useful and time consuming.
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetOptimizationSuite]
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSuite]
    // Rewrite the following two tests in GlutenDatasetSuite.
    .exclude("dropDuplicates: columns with same column name")
    .exclude("groupBy.as")
  enableSuite[GlutenJsonFunctionsSuite]
    // Velox does not support single quotes in get_json_object function.
    .exclude("function get_json_object - support single quotes")
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenXPathFunctionsSuite]
  enableSuite[GlutenFileBasedDataSourceSuite]
    // test data path is jar path, rewrite
    .exclude("Option recursiveFileLookup: disable partition inferring")
    // gluten executor exception cannot get in driver, rewrite
    .exclude("Spark native readers should respect spark.sql.caseSensitive - parquet")
    // shuffle_partitions config is different, rewrite
    .excludeByPrefix("SPARK-22790")
    // plan is different cause metric is different, rewrite
    .excludeByPrefix("SPARK-25237")
    // ignoreMissingFiles mode, wait to fix
    .exclude("Enabling/disabling ignoreMissingFiles using parquet")
  enableSuite[GlutenEnsureRequirementsSuite]
    // Rewrite to change the shuffle partitions for optimizing repartition
    .excludeByPrefix("SPARK-35675")
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    // Rewrite with columnar operators
    .excludeByPrefix("SPARK-24705")
    .excludeByPrefix("SPARK-34790")
    .excludeByPrefix("determining the number of reducers")
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
  enableSuite[GlutenDSV2CharVarcharTestSuite]
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenBinaryFileFormatSuite]
    // Exception.
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenCSVv1Suite]
  enableSuite[GlutenCSVv2Suite]
  enableSuite[GlutenCSVLegacyTimeParserSuite]
  enableSuite[GlutenJsonV1Suite]
  enableSuite[GlutenJsonV2Suite]
  enableSuite[GlutenJsonLegacyTimeParserSuite]
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcFilterSuite]
  enableSuite[GlutenOrcPartitionDiscoverySuite]
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
  enableSuite[GlutenOrcV1QuerySuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("Simple selection form ORC table")
    .exclude("simple select queries")
    .exclude("overwriting")
    .exclude("self-join")
    .exclude("columns only referenced by pushed down filters should remain")
    .exclude("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
  enableSuite[GlutenOrcV2QuerySuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("Simple selection form ORC table")
    .exclude("simple select queries")
    .exclude("overwriting")
    .exclude("self-join")
    .exclude("columns only referenced by pushed down filters should remain")
    .exclude("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
  enableSuite[GlutenOrcSourceSuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .exclude("SPARK-31238, SPARK-31423: rebasing dates in write")
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    // Ignored to disable vectorized reading check.
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
  enableSuite[GlutenOrcV1FilterSuite]
  enableSuite[GlutenOrcV1SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - with partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
  enableSuite[GlutenOrcV2SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - with partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
  enableSuite[GlutenParquetColumnIndexSuite]
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetEncodingSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
  enableSuite[GlutenParquetFileFormatV2Suite]
  enableSuite[GlutenParquetV1FilterSuite]
    // Rewrite.
    .exclude("Filter applied on merged Parquet schema with new column should work")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("filter pushdown - date")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("filter pushdown - StringStartsWith")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
  enableSuite[GlutenParquetV2FilterSuite]
    // Rewrite.
    .exclude("Filter applied on merged Parquet schema with new column should work")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("filter pushdown - date")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("filter pushdown - StringStartsWith")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
  enableSuite[GlutenParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetIOSuite]
    // Disable Spark's vectorized reading tests.
    .exclude("Standard mode - fixed-length decimals")
    .exclude("Legacy mode - fixed-length decimals")
    .exclude("SPARK-34167: read LongDecimals with precision < 10, VectorizedReader true")
    .exclude("read dictionary encoded decimals written as FIXED_LEN_BYTE_ARRAY")
    .exclude("read dictionary encoded decimals written as INT64")
    .exclude("read dictionary encoded decimals written as INT32")
    .exclude("SPARK-34817: Read UINT_64 as Decimal from parquet")
    // Spark plans scan schema as (i16/i32/i64) so the fallback does not take effect.
    // But Velox reads data based on the schema acquired from file metadata,
    // while i8 is not supported, so error occurs.
    .exclude("SPARK-34817: Read UINT_8/UINT_16/UINT_32 from parquet")
    // Exception.
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    // Rewrite to align exception msg.
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
    .exclude("SPARK-7847: Dynamic partition directory path escaping and unescaping")
    .exclude(
      "SPARK-22109: Resolve type conflicts between strings and timestamps in partition column")
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
    .exclude("SPARK-7847: Dynamic partition directory path escaping and unescaping")
    .exclude(
      "SPARK-22109: Resolve type conflicts between strings and timestamps in partition column")
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
  enableSuite[GlutenParquetV1QuerySuite]
    // spark.sql.parquet.enableVectorizedReader=true not supported
    .exclude("SPARK-16632: read Parquet int32 as ByteType and ShortType")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("returning batch for wide table")
  enableSuite[GlutenParquetV2QuerySuite]
    // spark.sql.parquet.enableVectorizedReader=true not supported
    .exclude("SPARK-16632: read Parquet int32 as ByteType and ShortType")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("returning batch for wide table")
  // requires resource files from Vanilla spark jar
  // enableSuite[GlutenParquetRebaseDatetimeV1Suite]
  // enableSuite[GlutenParquetRebaseDatetimeV2Suite]
  enableSuite[GlutenParquetV1SchemaPruningSuite]
    // spark.sql.parquet.enableVectorizedReader=true not supported
    .excludeByPrefix("Spark vectorized reader - ")
  enableSuite[GlutenParquetV2SchemaPruningSuite]
    // spark.sql.parquet.enableVectorizedReader=true not supported
    .excludeByPrefix("Spark vectorized reader - ")
   enableSuite[GlutenParquetRebaseDatetimeV1Suite]
     // jar path and ignore PARQUET_REBASE_MODE_IN_READ, rewrite some
     .excludeByPrefix("SPARK-31159")
     .excludeByPrefix("SPARK-35427")
   enableSuite[GlutenParquetRebaseDatetimeV2Suite]
     // jar path and ignore PARQUET_REBASE_MODE_IN_READ
     .excludeByPrefix("SPARK-31159")
     .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetSchemaInferenceSuite]
  enableSuite[GlutenParquetSchemaSuite]
    // error message mismatch is accepted
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    // Rewrite for file locating.
    .exclude("Read Parquet file generated by parquet-thrift")
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenFileFormatWriterSuite]
    .excludeByPrefix("empty file should be skipped while write to file")
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenParquetCodecSuite]
    // Unsupported compression codec.
    .exclude("write and read - file source parquet - codec: lz4")
  enableSuite[GlutenOrcCodecSuite]
  enableSuite[GlutenFileSourceStrategySuite]
    // Plan comparison.
    .exclude("partitioned table - after scan filters")
  enableSuite[GlutenHadoopFileLinesReaderSuite]
  enableSuite[GlutenPathFilterStrategySuite]
  enableSuite[GlutenPathFilterSuite]
  enableSuite[GlutenPruneFileSourcePartitionsSuite]
  enableSuite[GlutenCSVReadSchemaSuite]
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
  enableSuite[GlutenJsonReadSchemaSuite]
  enableSuite[GlutenOrcReadSchemaSuite]
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
    // Rewrite to disable Spark's vectorized reading.
    .exclude("change column position")
    .exclude("read byte, int, short, long together")
    .exclude("read float and double together")
  enableSuite[GlutenMergedOrcReadSchemaSuite]
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuite]
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenBucketedReadWithoutHiveSupportSuite]
    // Exclude the following suite for plan changed from SMJ to SHJ.
    .exclude("avoid shuffle when join 2 bucketed tables")
    .exclude("avoid shuffle and sort when sort columns are a super set of join keys")
    .exclude("only shuffle one side when join bucketed table and non-bucketed table")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket number")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket keys")
    .exclude("shuffle when join keys are not equal to bucket keys")
    .exclude("shuffle when join 2 bucketed tables with bucketing disabled")
    .exclude("check sort and shuffle when bucket and sort columns are join keys")
    .exclude("only sort one side when sort columns are different")
    .exclude("only sort one side when sort columns are same but their ordering is different")
    .exclude("SPARK-17698 Join predicates should not contain filter clauses")
    .exclude("SPARK-19122 Re-order join predicates if they match with the child's" +
      " output partitioning")
    .exclude("SPARK-19122 No re-ordering should happen if set of join columns != set of child's " +
      "partitioning columns")
    .exclude("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions")
    .exclude("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number")
    .exclude("bucket coalescing eliminates shuffle")
    .exclude("bucket coalescing is not satisfied")
    .excludeByPrefix("bucket coalescing is applied when join expressions match")
  enableSuite[GlutenBucketedWriteWithoutHiveSupportSuite]
  enableSuite[GlutenCreateTableAsSelectSuite]
    // TODO Gluten can not catch the spark exception in Driver side.
    .exclude("CREATE TABLE USING AS SELECT based on the file without write permission")
    .exclude("create a table, drop it and create another one with the same name")
  enableSuite[GlutenDDLSourceLoadSuite]
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite]
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[GlutenExternalCommandRunnerSuite]
  enableSuite[GlutenFilteredScanSuite]
  enableSuite[GlutenFiltersSuite]
  enableSuite[GlutenInsertSuite]
  enableSuite[GlutenPartitionedWriteSuite]
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenTableScanSuite]
  enableSuite[GlutenDataSourceV2Suite]
    // Gluten does not support the convert from spark columnar data
    // to velox columnar data.
    .exclude("columnar batch scan implementation")
    // Rewrite the following test in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
  enableSuite[GlutenCachedTableSuite]
  enableSuite[GlutenConfigBehaviorSuite]
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameWriterV2Suite]
  enableSuite[GlutenDatasetCacheSuite]
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite]
      // Rewrite with NaN test cases excluded.
    .exclude("cases when literal is max")
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenMetadataCacheSuite]
  enableSuite[GlutenSimpleShowCreateTableSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
}
