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
package org.apache.gluten.sql.shims

import org.apache.gluten.expression.Sig

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.{FileSourceScanExec, GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.{FileStatus, Path}

import java.util.{ArrayList => JArrayList, Map => JMap}

import scala.reflect.ClassTag

sealed abstract class ShimDescriptor

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch"

  def toMajorMinorVersion: String = s"$major.$minor"
}

trait SparkShims {
  def getShimDescriptor: ShimDescriptor

  // for this purpose, change HashClusteredDistribution to ClusteredDistribution
  // https://github.com/apache/spark/pull/32875
  def getDistribution(leftKeys: Seq[Expression], rightKeys: Seq[Expression]): Seq[Distribution]

  def scalarExpressionMappings: Seq[Sig]

  def aggregateExpressionMappings: Seq[Sig]

  def convertPartitionTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec])

  def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD

  def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression] = Seq.empty,
      dataFilters: Seq[Expression] = Seq.empty): TextScan

  def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]]

  // Spark3.4 new add table parameter in BatchScanExec.
  def getBatchScanExecTable(batchScan: BatchScanExec): Table

  // The PartitionedFile API changed in spark 3.4
  def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile

  def hasBloomFilterAggregate(
      agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec): Boolean

  def extractSubPlanFromMightContain(expr: Expression): Option[SparkPlan]

  def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = (plan.limit, 0)

  def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = (plan.limit, 0)

  def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    throw new UnsupportedOperationException()
  }

  def enableNativeWriteFilesByDefault(): Boolean = false

  def createTestTaskContext(): TaskContext

  def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    // Since Spark 3.4, the `sc.broadcast` has been optimized to use `sc.broadcastInternal`.
    // More details see SPARK-39983.
    // TODO, remove this shim once we drop Spark3.3 and previous
    sc.broadcast(value)
  }

  // To be compatible with Spark-3.5 and later
  // See https://github.com/apache/spark/pull/41440
  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit
  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit

  def getShuffleReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int)
      : Tuple2[Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])], Boolean]

  // Compatible with Spark-3.5 and later
  def getShuffleAdvisoryPartitionSize(shuffle: ShuffleExchangeLike): Option[Long] = None

  // Partition id in TaskInfo is only available after spark 3.3.
  def getPartitionId(taskInfo: TaskInfo): Int

  // Because above, this feature is only supported after spark 3.3
  def supportDuplicateReadingTracking: Boolean

  def getFileStatus(partition: PartitionDirectory): Seq[FileStatus]

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile]

  def structFromAttributes(attrs: Seq[Attribute]): StructType

  def attributesFromStruct(structType: StructType): Seq[Attribute]

  def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String] = Seq.empty): JMap[String, String]

  // For compatibility with Spark-3.5.
  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan]

  def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]]

  def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]]

  def orderPartitions(
      scan: Scan,
      keyGroupedPartitioning: Option[Seq[Expression]],
      filteredPartitions: Seq[Seq[InputPartition]],
      outputPartitioning: Partitioning): Seq[InputPartition] = filteredPartitions.flatten

  def extractExpressionTimestampAddUnit(timestampAdd: Expression): Option[Seq[String]] =
    Option.empty

  def supportsRowBased(plan: SparkPlan): Boolean = !plan.supportsColumnar

}
