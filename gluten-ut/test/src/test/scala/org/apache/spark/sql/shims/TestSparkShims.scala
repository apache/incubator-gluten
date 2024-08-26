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
package org.apache.spark.sql.shims

import org.apache.gluten.expression.Sig
import org.apache.gluten.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, HadoopFsRelation, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.schema.MessageType

import java.util
import java.util.Properties

class TestSparkShimProvider extends org.apache.gluten.sql.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new TestSparkShims()
  }

  def matches(version: String): Boolean = false
}

class TestSparkShims extends SparkShims {

  override def getShimDescriptor: ShimDescriptor = throw new UnsupportedOperationException()

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = throw new UnsupportedOperationException()

  override def scalarExpressionMappings: Seq[Sig] = throw new UnsupportedOperationException()

  override def aggregateExpressionMappings: Seq[Sig] = throw new UnsupportedOperationException()

  override def convertPartitionTransforms(
      partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) =
    throw new UnsupportedOperationException()

  override def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD =
    throw new UnsupportedOperationException()

  override def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): TextScan = throw new UnsupportedOperationException()

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] =
    throw new UnsupportedOperationException()

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table =
    throw new UnsupportedOperationException()

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      locations: Array[String]): PartitionedFile = throw new UnsupportedOperationException()

  override def bloomFilterExpressionMappings(): Seq[Sig] = throw new UnsupportedOperationException()

  override def newBloomFilterAggregate[T](
      child: Expression,
      estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int): TypedImperativeAggregate[T] =
    throw new UnsupportedOperationException()

  override def newMightContain(
      bloomFilterExpression: Expression,
      valueExpression: Expression): BinaryExpression = throw new UnsupportedOperationException()

  override def replaceBloomFilterAggregate[T](
      expr: Expression,
      bloomFilterAggReplacer: (
          Expression,
          Expression,
          Expression,
          Int,
          Int) => TypedImperativeAggregate[T]): Expression =
    throw new UnsupportedOperationException()

  override def replaceMightContain[T](
      expr: Expression,
      mightContainReplacer: (Expression, Expression) => BinaryExpression): Expression =
    throw new UnsupportedOperationException()

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    throw new UnsupportedOperationException()

  override def createTestTaskContext(properties: Properties): TaskContext =
    throw new UnsupportedOperationException()

  override def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = throw new UnsupportedOperationException()

  override def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = throw new UnsupportedOperationException()

  override def getShuffleReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): (Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], Boolean) =
    throw new UnsupportedOperationException()

  override def getPartitionId(taskInfo: TaskInfo): Int = throw new UnsupportedOperationException()

  override def supportDuplicateReadingTracking: Boolean = throw new UnsupportedOperationException()

  override def getFileStatus(partition: PartitionDirectory): Seq[FileStatus] =
    throw new UnsupportedOperationException()

  override def isFileSplittable(
      relation: HadoopFsRelation,
      filePath: Path,
      sparkSchema: StructType): Boolean = throw new UnsupportedOperationException()

  override def isRowIndexMetadataColumn(name: String): Boolean =
    throw new UnsupportedOperationException()

  override def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int =
    throw new UnsupportedOperationException()

  override def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] =
    throw new UnsupportedOperationException()

  override def structFromAttributes(attrs: Seq[Attribute]): StructType =
    throw new UnsupportedOperationException()

  override def attributesFromStruct(structType: StructType): Seq[Attribute] =
    throw new UnsupportedOperationException()

  override def getFileSizeAndModificationTime(file: PartitionedFile): (Option[Long], Option[Long]) =
    throw new UnsupportedOperationException()

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): util.Map[String, String] =
    throw new UnsupportedOperationException()

  override def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] =
    throw new UnsupportedOperationException()

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] =
    throw new UnsupportedOperationException()

  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    throw new UnsupportedOperationException()

  override def dateTimestampFormatInReadIsDefaultValue(
      csvOptions: CSVOptions,
      timeZone: String): Boolean = throw new UnsupportedOperationException()

  override def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean]): ParquetFilters = throw new UnsupportedOperationException()
}
