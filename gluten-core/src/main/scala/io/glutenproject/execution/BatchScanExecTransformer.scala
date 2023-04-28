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

import java.util.Objects

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.metrics.MetricsUpdater

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExecShim, FileScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class BatchScanExecTransformer(output: Seq[AttributeReference], @transient scan: Scan,
                               runtimeFilters: Seq[Expression],
                               pushdownFilters: Seq[Expression] = Seq())
  extends BatchScanExecShim(output, scan, runtimeFilters) with BasicScanExecTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = scan match {
    case fileScan: FileScan =>
      fileScan.dataFilters ++ pushdownFilters
    case _ =>
      throw new UnsupportedOperationException(s"${scan.getClass.toString} is not supported")
  }

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[Seq[InputPartition]] = filteredPartitions

  override def getFlattenPartitions: Seq[InputPartition] = filteredFlattenPartitions

  override def getPartitionSchemas: StructType = scan match {
    case fileScan: FileScan => fileScan.readPartitionSchema
    case _ => new StructType()
  }

  override def getInputFilePaths: Seq[String] = scan match {
    case fileScan: FileScan => fileScan.fileIndex.inputFiles.toSeq
    case _ => Seq.empty
  }

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def equals(other: Any): Boolean = other match {
    case that: BatchScanExecTransformer =>
      (that canEqual this) && super.equals(that) &&
        this.pushdownFilters == that.getPushdownFilters()
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters, pushdownFilters)

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

  def getPushdownFilters(): Seq[Expression] = {
    pushdownFilters
  }
}
