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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExecShim, FileScan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Objects

/**
 * Columnar Based BatchScanExec. Although keyGroupedPartitioning is not used, it cannot be deleted,
 * it can make BatchScanExecTransformer contain a constructor with the same parameters as
 * Spark-3.3's BatchScanExec. Otherwise, the corresponding constructor will not be found when
 * calling TreeNode.makeCopy and will fail to copy this node during transformation.
 */
class BatchScanExecTransformer(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    keyGroupedPartitioning: Option[Seq[Expression]] = None)
  extends BatchScanExecShim(output, scan, runtimeFilters)
  with BasicScanExecTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = scan match {
    case fileScan: FileScan =>
      fileScan.dataFilters
    case _ =>
      throw new UnsupportedOperationException(s"${scan.getClass.toString} is not supported")
  }

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = filteredFlattenPartitions

  override def getPartitionSchemas: StructType = scan match {
    case fileScan: FileScan => fileScan.readPartitionSchema
    case _ => new StructType()
  }

  override def getDataSchemas: StructType = scan match {
    case fileScan: FileScan => fileScan.readDataSchema
    case _ => new StructType()
  }

  override def getInputFilePaths: Seq[String] = {
    if (BackendsApiManager.isVeloxBackend) {
      Seq.empty[String]
    } else {
      scan match {
        case fileScan: FileScan => fileScan.fileIndex.inputFiles.toSeq
        case _ => Seq.empty
      }
    }
  }

  override def doValidateInternal(): ValidationResult = {
    if (pushedAggregate.nonEmpty) {
      return ValidationResult.notOk(s"Unsupported aggregation push down for $scan.")
    }
    super.doValidateInternal()
  }

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def equals(other: Any): Boolean = other match {
    case that: BatchScanExecTransformer =>
      that.canEqual(this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[BatchScanExecTransformer]

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    Seq()
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = {
    this
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetricsUpdater(metrics)

  @transient protected lazy val filteredFlattenPartitions: Seq[InputPartition] =
    filteredPartitions.flatten

  @transient override lazy val fileFormat: ReadFileFormat = scan.getClass.getSimpleName match {
    case "OrcScan" => ReadFileFormat.OrcReadFormat
    case "ParquetScan" => ReadFileFormat.ParquetReadFormat
    case "DwrfScan" => ReadFileFormat.DwrfReadFormat
    case "ClickHouseScan" => ReadFileFormat.MergeTreeReadFormat
    case _ => ReadFileFormat.UnknownFormat
  }

  override def doCanonicalize(): BatchScanExecTransformer = {
    val canonicalized = super.doCanonicalize()
    new BatchScanExecTransformer(
      canonicalized.output,
      canonicalized.scan,
      canonicalized.runtimeFilters
    )
  }
}
