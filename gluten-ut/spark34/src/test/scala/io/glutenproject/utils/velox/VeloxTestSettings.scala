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

import io.glutenproject.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{GlutenArithmeticExpressionSuite, GlutenBitwiseExpressionsSuite, GlutenCastSuite, GlutenCollectionExpressionsSuite, GlutenComplexTypeSuite, GlutenConditionalExpressionSuite, GlutenDateExpressionsSuite, GlutenDecimalExpressionSuite, GlutenExpressionMappingSuite, GlutenHashExpressionsSuite, GlutenIntervalExpressionsSuite, GlutenLiteralExpressionSuite, GlutenMathExpressionsSuite, GlutenMiscExpressionsSuite, GlutenNondeterministicSuite, GlutenNullExpressionsSuite, GlutenPredicateSuite, GlutenRandomSuite, GlutenRegexpExpressionsSuite, GlutenSortOrderExpressionsSuite, GlutenStringExpressionsSuite}
import org.apache.spark.sql.connector.{GlutenDataSourceV2DataFrameSessionCatalogSuite, GlutenDataSourceV2DataFrameSuite, GlutenDataSourceV2FunctionSuite, GlutenDataSourceV2SQLSessionCatalogSuite, GlutenDataSourceV2SQLSuiteV1Filter, GlutenDataSourceV2SQLSuiteV2Filter, GlutenDataSourceV2Suite, GlutenDeleteFromTableSuite, GlutenFileDataSourceV2FallBackSuite, GlutenKeyGroupedPartitioningSuite, GlutenLocalScanSuite, GlutenMetadataColumnSuite, GlutenSupportsCatalogOptionsSuite, GlutenTableCapabilityCheckSuite, GlutenWriteDistributionAndOrderingSuite}
import org.apache.spark.sql.errors.{GlutenQueryCompilationErrorsDSv2Suite, GlutenQueryCompilationErrorsSuite, GlutenQueryExecutionErrorsSuite, GlutenQueryParsingErrorsSuite}
import org.apache.spark.sql.execution.{FallbackStrategiesSuite, GlutenBroadcastExchangeSuite, GlutenCoalesceShufflePartitionsSuite, GlutenExchangeSuite, GlutenReplaceHashWithSortAggSuite, GlutenReuseExchangeAndSubquerySuite, GlutenSameResultSuite, GlutenSortSuite, GlutenSQLWindowFunctionSuite, GlutenTakeOrderedAndProjectSuite}
import org.apache.spark.sql.execution.adaptive.velox.VeloxAdaptiveQueryExecSuite
import org.apache.spark.sql.execution.datasources.{GlutenBucketingUtilsSuite, GlutenCSVReadSchemaSuite, GlutenDataSourceStrategySuite, GlutenDataSourceSuite, GlutenFileFormatWriterSuite, GlutenFileIndexSuite, GlutenFileMetadataStructSuite, GlutenFileSourceStrategySuite, GlutenHadoopFileLinesReaderSuite, GlutenHeaderCSVReadSchemaSuite, GlutenJsonReadSchemaSuite, GlutenMergedOrcReadSchemaSuite, GlutenMergedParquetReadSchemaSuite, GlutenOrcCodecSuite, GlutenOrcReadSchemaSuite, GlutenOrcV1AggregatePushDownSuite, GlutenOrcV2AggregatePushDownSuite, GlutenParquetCodecSuite, GlutenParquetReadSchemaSuite, GlutenParquetV1AggregatePushDownSuite, GlutenParquetV2AggregatePushDownSuite, GlutenPathFilterStrategySuite, GlutenPathFilterSuite, GlutenPruneFileSourcePartitionsSuite, GlutenV1WriteCommandSuite, GlutenVectorizedOrcReadSchemaSuite, GlutenVectorizedParquetReadSchemaSuite}
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.csv.{GlutenCSVLegacyTimeParserSuite, GlutenCSVv1Suite, GlutenCSVv2Suite}
import org.apache.spark.sql.execution.datasources.exchange.GlutenValidateRequirementsSuite
import org.apache.spark.sql.execution.datasources.json.{GlutenJsonLegacyTimeParserSuite, GlutenJsonV1Suite, GlutenJsonV2Suite}
import org.apache.spark.sql.execution.datasources.orc.{GlutenOrcColumnarBatchReaderSuite, GlutenOrcFilterSuite, GlutenOrcPartitionDiscoverySuite, GlutenOrcSourceSuite, GlutenOrcV1FilterSuite, GlutenOrcV1PartitionDiscoverySuite, GlutenOrcV1QuerySuite, GlutenOrcV1SchemaPruningSuite, GlutenOrcV2QuerySuite, GlutenOrcV2SchemaPruningSuite}
import org.apache.spark.sql.execution.datasources.parquet.{GlutenParquetColumnIndexSuite, GlutenParquetCompressionCodecPrecedenceSuite, GlutenParquetDeltaByteArrayEncodingSuite, GlutenParquetDeltaEncodingInteger, GlutenParquetDeltaEncodingLong, GlutenParquetDeltaLengthByteArrayEncodingSuite, GlutenParquetEncodingSuite, GlutenParquetFieldIdIOSuite, GlutenParquetFileFormatV1Suite, GlutenParquetFileFormatV2Suite, GlutenParquetInteroperabilitySuite, GlutenParquetIOSuite, GlutenParquetProtobufCompatibilitySuite, GlutenParquetRebaseDatetimeV1Suite, GlutenParquetRebaseDatetimeV2Suite, GlutenParquetSchemaInferenceSuite, GlutenParquetSchemaSuite, GlutenParquetThriftCompatibilitySuite, GlutenParquetV1FilterSuite, GlutenParquetV1PartitionDiscoverySuite, GlutenParquetV1QuerySuite, GlutenParquetV1SchemaPruningSuite, GlutenParquetV2FilterSuite, GlutenParquetV2PartitionDiscoverySuite, GlutenParquetV2QuerySuite, GlutenParquetV2SchemaPruningSuite, GlutenParquetVectorizedSuite}
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDataSourceV2StrategySuite, GlutenFileTableSuite, GlutenV2PredicateSuite}
import org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite
import org.apache.spark.sql.execution.joins.{GlutenBroadcastJoinSuite, GlutenExistenceJoinSuite, GlutenInnerJoinSuite, GlutenOuterJoinSuite}
import org.apache.spark.sql.extension.{GlutenCollapseProjectExecTransformerSuite, GlutenSessionExtensionSuite, TestFileSourceScanExecTransformer}
import org.apache.spark.sql.gluten.GlutenFallbackSuite
import org.apache.spark.sql.hive.execution.GlutenHiveSQLQuerySuite
import org.apache.spark.sql.sources.{GlutenBucketedReadWithoutHiveSupportSuite, GlutenBucketedWriteWithoutHiveSupportSuite, GlutenCreateTableAsSelectSuite, GlutenDDLSourceLoadSuite, GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite, GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE, GlutenExternalCommandRunnerSuite, GlutenFilteredScanSuite, GlutenFiltersSuite, GlutenInsertSuite, GlutenPartitionedWriteSuite, GlutenPathOptionSuite, GlutenPrunedScanSuite, GlutenResolvedDataSourceSuite, GlutenSaveLoadSuite, GlutenTableScanSuite}

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class VeloxTestSettings extends BackendTestSettings {
  enableSuite[GlutenStringFunctionsSuite]
    .exclude("SPARK-31993: concat_ws in agg function with plenty of string/array types columns")
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuiteV1Filter]
  enableSuite[GlutenDataSourceV2SQLSuiteV2Filter]
  enableSuite[GlutenDataSourceV2Suite]
    // Rewrite the following tests in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
    .exclude("ordering and partitioning reporting")
  enableSuite[GlutenDeleteFromTableSuite]
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    // NEW SUITE: disable as they check vanilla spark plan
    .exclude("partitioned join: number of buckets mismatch should trigger shuffle")
    .exclude("partitioned join: only one side reports partitioning")
    .exclude("partitioned join: join with two partition keys and different # of partition keys")
    // disable due to check for SMJ node
    .excludeByPrefix("SPARK-41413: partitioned join:")
    .excludeByPrefix("SPARK-42038: partially clustered:")
    .exclude("SPARK-44641: duplicated records when SPJ is not triggered")
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenMetadataColumnSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenQueryCompilationErrorsDSv2Suite]
  enableSuite[GlutenQueryCompilationErrorsSuite]
  enableSuite[GlutenQueryExecutionErrorsSuite]
    // NEW SUITE: disable as it expects exception which doesn't happen when offloaded to gluten
    .exclude(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION: compatibility with Spark 2.4/3.2 in reading/writing dates")
    // gluten throws different exception
    .excludeByPrefix("SCALAR_SUBQUERY_TOO_MANY_ROWS:")
  enableSuite[GlutenQueryParsingErrorsSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("SPARK-45786: Decimal multiply, divide, remainder, quot")
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
  enableSuite[GlutenCollectionExpressionsSuite]
    // Rewrite in Gluten to replace Seq with Array
    .exclude("Shuffle")
    // TODO: ArrayDistinct should handle duplicated Double.NaN
    .excludeByPrefix("SPARK-36741")
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDateExpressionsSuite]
    // Has exception in fallback execution when we use resultDF.collect in evaluation.
    .exclude("TIMESTAMP_MICROS")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("to_unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("Hour")
    // Unsupported format: yyyy-MM-dd HH:mm:ss.SSS
    .exclude("SPARK-33498: GetTimestamp,UnixTimestamp,ToUnixTimestamp with parseError")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("DateFormat")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("to_timestamp exception mode")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("from_unixtime")
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenHashExpressionsSuite]
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenJsonFunctionsSuite]
    // Velox does not support single quotes in get_json_object function.
    .exclude("function get_json_object - support single quotes")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    // FIXME(yma11): ObjectType is not covered in RowEncoder/Serializer in vanilla spark
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenMathExpressionsSuite]
    // Spark round UT for round(3.1415,3) is not correct.
    .exclude("round/bround/floor/ceil")
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
  enableSuite[GlutenRandomSuite]
    .exclude("random")
    .exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
  enableSuite[VeloxAdaptiveQueryExecSuite]
    .includeAllGlutenTests()
    .includeByPrefix(
      "SPARK-29906",
      "SPARK-30291",
      "SPARK-30403",
      "SPARK-30719",
      "SPARK-31384",
      "SPARK-30953",
      "SPARK-31658",
      "SPARK-32717",
      "SPARK-32649",
      "SPARK-34533",
      "SPARK-34781",
      "SPARK-35585",
      "SPARK-32932",
      "SPARK-33494",
      "SPARK-33933",
      "SPARK-31220",
      "SPARK-35874",
      "SPARK-39551"
    )
    .include(
      "Union/Except/Intersect queries",
      "Subquery de-correlation in Union queries",
      "force apply AQE",
      "tree string output",
      "control a plan explain mode in listener vis SQLConf",
      "AQE should set active session during execution",
      "No deadlock in UI update",
      "SPARK-35455: Unify empty relation optimization between normal and AQE optimizer - multi join"
    )
  enableSuite[GlutenBinaryFileFormatSuite]
    // Exception.
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenCSVv1Suite]
  enableSuite[GlutenCSVv2Suite]
  enableSuite[GlutenCSVLegacyTimeParserSuite]
  enableSuite[GlutenJsonV1Suite]
    // FIXME: Array direct selection fails
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenJsonV2Suite]
    // exception test
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenJsonLegacyTimeParserSuite]
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenValidateRequirementsSuite]
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcFilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcPartitionDiscoverySuite]
    .exclude("read partitioned table - normal case")
    .exclude("read partitioned table - with nulls")
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
    .exclude("read partitioned table - normal case")
    .exclude("read partitioned table - with nulls")
    .exclude("read partitioned table - partition key included in orc file")
    .exclude("read partitioned table - with nulls and partition keys are included in Orc file")
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
    .exclude("Read/write binary data")
    .exclude("Read/write all types with non-primitive type")
    .exclude("Creating case class RDD table")
    .exclude("save and load case class RDD with `None`s as orc")
    .exclude("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when" +
      " compression is unset")
    .exclude("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .exclude("appending")
    .exclude("nested data - struct with array field")
    .exclude("nested data - array of struct")
    .exclude("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .exclude("SPARK-10623 Enable ORC PPD")
    .exclude("SPARK-14962 Produce correct results on array type with isnotnull")
    .exclude("SPARK-15198 Support for pushing down filters for boolean types")
    .exclude("Support for pushing down filters for decimal types")
    .exclude("Support for pushing down filters for timestamp types")
    .exclude("column nullability and comment - write and then read")
    .exclude("Empty schema does not read data from ORC file")
    .exclude("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("LZO compression options for writing to an ORC file")
    .exclude("Schema discovery on empty ORC files")
    .exclude("SPARK-21791 ORC should support column names with dot")
    .exclude("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("Read/write all timestamp types")
    .exclude("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .exclude("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
  enableSuite[GlutenOrcV2QuerySuite]
    .exclude("Read/write binary data")
    .exclude("Read/write all types with non-primitive type")
    // Rewrite to disable Spark's columnar reader.
    .exclude("Simple selection form ORC table")
    .exclude("Creating case class RDD table")
    .exclude("save and load case class RDD with `None`s as orc")
    .exclude("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when compression is unset")
    .exclude("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .exclude("appending")
    .exclude("nested data - struct with array field")
    .exclude("nested data - array of struct")
    .exclude("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .exclude("SPARK-10623 Enable ORC PPD")
    .exclude("SPARK-14962 Produce correct results on array type with isnotnull")
    .exclude("SPARK-15198 Support for pushing down filters for boolean types")
    .exclude("Support for pushing down filters for decimal types")
    .exclude("Support for pushing down filters for timestamp types")
    .exclude("column nullability and comment - write and then read")
    .exclude("Empty schema does not read data from ORC file")
    .exclude("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("LZO compression options for writing to an ORC file")
    .exclude("Schema discovery on empty ORC files")
    .exclude("SPARK-21791 ORC should support column names with dot")
    .exclude("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("Read/write all timestamp types")
    .exclude("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .exclude("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
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
    .exclude("create temporary orc table")
    .exclude("create temporary orc table as")
    .exclude("appending insert")
    .exclude("overwrite insert")
    .exclude("SPARK-34897: Support reconcile schemas based on index after nested column pruning")
    .excludeGlutenTest("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .excludeGlutenTest("SPARK-31238, SPARK-31423: rebasing dates in write")
    .excludeGlutenTest("SPARK-34862: Support ORC vectorized reader for nested column")
    // exclude as struct not supported
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers")
    .exclude("SPARK-37812: Reuse result row when deserializing a struct")
    // rewrite
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenOrcV1FilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
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
    // Vectorized reading.
    .exclude("Spark vectorized reader - without partition data column - " +
      "select only expressions without references")
    .exclude("Spark vectorized reader - with partition data column - " +
      "select only expressions without references")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field")
    .exclude(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - without partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - with partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - without partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - with partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function")
    .exclude(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Sort")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Expand")
    .exclude(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
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
    .exclude("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field")
    .exclude(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - without partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - with partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - without partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - with partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function")
    .exclude(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Sort")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Expand")
    .exclude(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
  enableSuite[GlutenParquetColumnIndexSuite]
    // Rewrite by just removing test timestamp.
    .exclude("test reading unaligned pages - test all types")
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[GlutenParquetDeltaEncodingInteger]
  enableSuite[GlutenParquetDeltaEncodingLong]
  enableSuite[GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[GlutenParquetEncodingSuite]
    // exclude as cases use Vectorization Column reader
    .exclude("parquet v2 pages - delta encoding")
    .exclude("parquet v2 pages - rle encoding for boolean value columns")
  enableSuite[GlutenParquetFieldIdIOSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
  enableSuite[GlutenParquetFileFormatV2Suite]
  enableSuite[GlutenParquetV1FilterSuite]
    // Rewrite.
    .exclude("Filter applied on merged Parquet schema with new column should work")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
  enableSuite[GlutenParquetV2FilterSuite]
    // Rewrite.
    .exclude("Filter applied on merged Parquet schema with new column should work")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
  enableSuite[GlutenParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetIOSuite]
    // Velox doesn't write file metadata into parquet file.
    .exclude("Write Spark version into Parquet metadata")
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
    // Exception msg.
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    // Timestamp is read as INT96.
    .exclude("read dictionary and plain encoded timestamp_millis written as INT64")
    // TODO: Unsupported Array schema in Parquet.
    .exclude("vectorized reader: optional array with required elements")
    .exclude("vectorized reader: required array with required elements")
    .exclude("vectorized reader: required array with optional elements")
    .exclude("vectorized reader: required array with legacy format")
    .exclude("SPARK-36726: test incorrect Parquet row group file offset")
    // add support in native reader
    .exclude("SPARK-41096: FIXED_LEN_BYTE_ARRAY support")
    .exclude("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings")
    // Timestamp is read as INT96
    .exclude("Read TimestampNTZ and TimestampLTZ for various logical TIMESTAMP types")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
    // Timezone is not supported yet.
    .exclude("Resolve type conflicts - decimals, dates and timestamps in partition column")
    // rewrite
    .exclude("Various partition value types")
    .exclude(("Various inferred partition value types"))
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
  enableSuite[GlutenParquetV1QuerySuite]
    // Only for testing a type mismatch issue caused by hive (before hive 2.2).
    // Only reproducible when spark.sql.parquet.enableVectorizedReader=true.
    .exclude("SPARK-16632: read Parquet int32 as ByteType and ShortType")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("returning batch for wide table")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // Timestamp is read as INT96.
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-10365 timestamp written and read as INT64 - TIMESTAMP_MICROS")
    .exclude("SPARK-36182: read TimestampNTZ as TimestampLTZ")
    // new added in spark-3.3 and need fix later, random failure may caused by memory free
    .exclude("SPARK-39833: pushed filters with project without filter columns")
    .exclude("SPARK-39833: pushed filters with count()")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV2QuerySuite]
    // Only for testing a type mismatch issue caused by hive (before hive 2.2).
    // Only reproducible when spark.sql.parquet.enableVectorizedReader=true.
    .exclude("SPARK-16632: read Parquet int32 as ByteType and ShortType")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("returning batch for wide table")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // Timestamp is read as INT96.
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-10365 timestamp written and read as INT64 - TIMESTAMP_MICROS")
    .exclude("SPARK-36182: read TimestampNTZ as TimestampLTZ")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
  enableSuite[GlutenParquetV2SchemaPruningSuite]
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ, rewrite some
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetRebaseDatetimeV2Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetSchemaInferenceSuite]
  enableSuite[GlutenParquetSchemaSuite]
    // error message mismatch is accepted
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
    // [PATH_NOT_FOUND] Path does not exist:
    // file:/opt/spark331/sql/core/src/test/resources/test-data/timestamp-nanos.parquet
    // May require for newer spark.test.home
    .excludeByPrefix("SPARK-40819")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    // Rewrite for file locating.
    .exclude("Read Parquet file generated by parquet-thrift")
  enableSuite[GlutenParquetVectorizedSuite]
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenDataSourceV2StrategySuite]
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenV2PredicateSuite]
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenFileFormatWriterSuite]
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenFileMetadataStructSuite]
  enableSuite[GlutenParquetV1AggregatePushDownSuite]
  enableSuite[GlutenParquetV2AggregatePushDownSuite]
  enableSuite[GlutenOrcV1AggregatePushDownSuite]
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenOrcV2AggregatePushDownSuite]
    .exclude("nested column: Max(top level column) not push down")
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenParquetCodecSuite]
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
    .exclude("append column into middle")
    .exclude("hide column in the middle")
    .exclude("change column position")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("read as string")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
    .exclude("change column type from float to decimal")
    .exclude("change column type from double to decimal")
    .exclude("read float, double, decimal together")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
    // Rewrite to disable Spark's vectorized reading.
    .exclude("change column position")
    .exclude("read byte, int, short, long together")
    .exclude("read float and double together")
    .exclude("append column into middle")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("change column type from float to double")
    .excludeGlutenTest("read byte, int, short, long together")
    .excludeGlutenTest("read float and double together")
  enableSuite[GlutenMergedOrcReadSchemaSuite]
    .exclude("append column into middle")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenV1WriteCommandSuite]
    // Rewrite to match SortExecTransformer.
    .excludeByPrefix("SPARK-41914:")
  enableSuite[GlutenEnsureRequirementsSuite]

  enableSuite[GlutenBroadcastJoinSuite]
    .exclude("Shouldn't change broadcast join buildSide if user clearly specified")
    .exclude("Shouldn't bias towards build right if user didn't specify")
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data")
    .exclude("broadcast hint isn't propagated after a join")
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning")

  enableSuite[GlutenExistenceJoinSuite]
  enableSuite[GlutenInnerJoinSuite]
  enableSuite[GlutenOuterJoinSuite]
  enableSuite[FallbackStrategiesSuite]
  enableSuite[GlutenBroadcastExchangeSuite]
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    .excludeByPrefix("determining the number of reducers")
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeExec does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeExec does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenReplaceHashWithSortAggSuite]
    .exclude("replace partial hash aggregate with sort aggregate")
    .exclude("replace partial and final hash aggregate together with sort aggregate")
    .exclude("do not replace hash aggregate if child does not have sort order")
    .exclude("do not replace hash aggregate if there is no group-by column")
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenSameResultSuite]
  enableSuite[GlutenSortSuite]
  // spill not supported yet.
  enableSuite[GlutenSQLWindowFunctionSuite].exclude("test with low buffer spill threshold")
  enableSuite[GlutenTakeOrderedAndProjectSuite]
  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[TestFileSourceScanExecTransformer]
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
    // the native write staing dir is differnt with vanilla Spark for coustom partition paths
    .exclude("SPARK-35106: Throw exception when rename custom partition paths returns false")
    .exclude("Stop task set if FileAlreadyExistsException was thrown")
    // Rewrite: Additional support for file scan with default values has been added in Spark-3.4.
    // It appends the default value in record if it is not present while scanning.
    // Velox supports default values for new records but it does not backfill the
    // existing records and provides null for the existing ones.
    .exclude("INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them")
    .exclude("SPARK-39557 INSERT INTO statements with tables with array defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with struct defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with map defaults")
  enableSuite[GlutenPartitionedWriteSuite]
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenTableScanSuite]
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
  enableSuite[GlutenApproximatePercentileQuerySuite]
    // requires resource files from Vanilla spark jar
    .exclude("SPARK-32908: maximum target error in percentile_approx")
  enableSuite[GlutenCachedTableSuite]
    .exclude("InMemoryRelation statistics")
    // Extra ColumnarToRow is needed to transform vanilla columnar data to gluten columnar data.
    .exclude("SPARK-37369: Avoid redundant ColumnarToRow transition on InMemoryTableScan")
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
  enableSuite[GlutenDSV2CharVarcharTestSuite]
  enableSuite[GlutenColumnExpressionSuite]
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenConfigBehaviorSuite]
    // Will be fixed by cleaning up ColumnarShuffleExchangeExec.
    .exclude("SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition")
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude(
      "zero moments", // [velox does not return NaN]
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      // Replaced with another test.
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
        " before using it"
    )
  enableSuite[GlutenDataFrameAsOfJoinSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
    // blocked by Velox-5768
    .exclude("aggregate function - array for primitive type containing null")
    .exclude("aggregate function - array for non-primitive type")
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenDataFrameJoinSuite]
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
      // NaN case
      "replace nan with float",
      "replace nan with double"
    )
  enableSuite[GlutenDataFramePivotSuite]
    // substring issue
    .exclude("pivot with column definition in groupby")
    // array comparison not supported for values that contain nulls
    .exclude(
      "pivot with null and aggregate type not supported by PivotFirst returns correct result")
  enableSuite[GlutenDataFrameRangeSuite]
    .exclude("SPARK-20430 Initialize Range parameters in a driver side")
    .excludeByPrefix("Cancelling stage in a query with Range")
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar")
    // Result depends on the implementation for nondeterministic expression rand.
    // Not really an issue.
    .exclude("SPARK-10740: handle nondeterministic expressions correctly for set operations")
  enableSuite[GlutenDataFrameStatSuite]
  enableSuite[GlutenDataFrameSuite]
    // Rewrite these tests because it checks Spark's physical operators.
    .excludeByPrefix("SPARK-22520", "reuse exchange")
    .exclude(
      /**
       * Rewrite these tests because the rdd partition is equal to the configuration
       * "spark.sql.shuffle.partitions".
       */
      "repartitionByRange",
      "distributeBy and localSort",
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      // Rewrite this test because the describe functions creates unmatched plan.
      "describe",
      // decimal failed ut.
      "SPARK-22271: mean overflows and returns null for some decimal variables",
      // Result depends on the implementation for nondeterministic expression rand.
      // Not really an issue.
      "SPARK-9083: sort with non-deterministic expressions"
    )
    // test for sort node not present but gluten uses shuffle hash join
    .exclude("SPARK-41048: Improve output partitioning and ordering with AQE cache")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
  enableSuite[GlutenDataFrameTungstenSuite]
  enableSuite[GlutenDataFrameWindowFramesSuite]
    // Local window fixes are not added.
    .exclude("range between should accept int/long values as boundary")
    .exclude("unbounded preceding/following range between with aggregation")
    .exclude("sliding range between with aggregation")
    .exclude("store and retrieve column stats in different time zones")
    .exclude("rows between should accept int/long values as boundary")
  enableSuite[GlutenDataFrameWriterV2Suite]
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetCacheSuite]
  enableSuite[GlutenDatasetOptimizationSuite]
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDatasetSuite]
    // Rewrite the following two tests in GlutenDatasetSuite.
    .exclude("dropDuplicates: columns with same column name")
    .exclude("groupBy.as")
  enableSuite[GlutenDateFunctionsSuite]
    // The below two are replaced by two modified versions.
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    // Unsupported datetime format: specifier X is not supported by velox.
    .exclude("to_timestamp with microseconds precision")
    // Replaced by another test.
    .exclude("to_timestamp")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("SPARK-30668: use legacy timestamp parser in to_timestamp")
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan]
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenFileBasedDataSourceSuite]
    // test data path is jar path, rewrite
    .exclude("Option recursiveFileLookup: disable partition inferring")
    // gluten executor exception cannot get in driver, rewrite
    .exclude("Spark native readers should respect spark.sql.caseSensitive - parquet")
    // shuffle_partitions config is different, rewrite
    .excludeByPrefix("SPARK-22790")
    // plan is different cause metric is different, rewrite
    .excludeByPrefix("SPARK-25237")
    // error msg from velox is different & reader options is not supported, rewrite
    .exclude("Enabling/disabling ignoreMissingFiles using parquet")
    .exclude("Enabling/disabling ignoreMissingFiles using orc")
    .exclude("Spark native readers should respect spark.sql.caseSensitive - orc")
    .exclude("Return correct results when data columns overlap with partition columns")
    .exclude("Return correct results when data columns overlap with partition " +
      "columns (nested data)")
    .exclude("SPARK-31116: Select nested schema with case insensitive mode")
    // exclude as original metric not correct when task offloaded to velox
    .exclude("SPARK-37585: test input metrics for DSV2 with output limits")
    // TODO(yuan): fix the input bytes on ORC code path
    .exclude("SPARK-30362: test input metrics for DSV2")
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenGeneratorFunctionSuite]
  enableSuite[GlutenInjectRuntimeFilterSuite]
    // FIXME: yan
    .exclude("Merge runtime bloom filters")
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenJoinSuite]
    // exclude as it check spark plan
    .exclude("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join")
    // exclude as it check for SMJ node
    .exclude(
      "SPARK-43113: Full outer join with duplicate stream-side references in condition (SMJ)")
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenMetadataCacheSuite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException")
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  // following UT is removed in spark3.3.1
  // enableSuite[GlutenSimpleShowCreateTableSuite]
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenSQLQuerySuite]
    // Decimal precision exceeds.
    .exclude("should be able to resolve a persistent view")
    // Unstable. Needs to be fixed.
    .exclude("SPARK-36093: RemoveRedundantAliases should not change expression's name")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported.
    .exclude("SPARK-28156: self-join should not miss cached view")
    .exclude("SPARK-33338: GROUP BY using literal map should not fail")
    // Rewrite to disable plan check for SMJ because SHJ is preferred in Gluten.
    .exclude("SPARK-11111 null-safe join should not use cartesian product")
    // Rewrite to change the information of a caught exception.
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    // Different exception.
    .exclude("run sql directly on files")
    // Not useful and time consuming.
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
    // exception test, rewritten in gluten
    .exclude("the escape character is not allowed to end with")
    // ORC related
    .exclude("SPARK-37965: Spark support read/write orc file with invalid char in field name")
    .exclude("SPARK-38173: Quoted column cannot be recognized correctly when quotedRegexColumnNames is true")
  enableSuite[GlutenSQLQueryTestSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
    // The output byte size of Velox is different
    .exclude("SPARK-33687: analyze all tables in a specific database")
  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893" // Rewrite this test because it checks Spark's physical operators.
    )
    // exclude as it checks spark plan
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery")
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite]
    // Rewrite with NaN test cases excluded.
    .exclude("cases when literal is max")
  enableSuite[GlutenXPathFunctionsSuite]
  enableSuite[GlutenFallbackSuite]
  enableSuite[GlutenHiveSQLQuerySuite]
  enableSuite[GlutenCollapseProjectExecTransformerSuite]
  enableSuite[GlutenExpressionMappingSuite]
  enableSuite[GlutenSparkSessionExtensionSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = VeloxSQLQueryTestSettings
}
// scalastyle:on line.size.limit
