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
import io.glutenproject.vectorized.OperatorMetrics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class BatchScanExecTransformer(output: Seq[AttributeReference], @transient scan: Scan,
                               runtimeFilters: Seq[Expression],
                               pushdownFilters: Seq[Expression] = Seq())
  extends BatchScanExec(output, scan, runtimeFilters) with BasicScanExecTransformer {

  override lazy val metrics = Map(
    "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
    "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
    "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "count" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_batchscan"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "total scan time"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"),
    "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
      sparkContext, "number of dynamic filters accepted"))

  val inputRows: SQLMetric = longMetric("inputRows")
  val inputVectors: SQLMetric = longMetric("inputVectors")
  val inputBytes: SQLMetric = longMetric("inputBytes")
  val rawInputRows: SQLMetric = longMetric("rawInputRows")
  val rawInputBytes: SQLMetric = longMetric("rawInputBytes")
  val outputRows: SQLMetric = longMetric("outputRows")
  val outputVectors: SQLMetric = longMetric("outputVectors")
  val outputBytes: SQLMetric = longMetric("outputBytes")
  val count: SQLMetric = longMetric("count")
  val wallNanos: SQLMetric = longMetric("wallNanos")
  val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

  // Number of dynamic filters received.
  val numDynamicFiltersAccepted: SQLMetric = longMetric("numDynamicFiltersAccepted")

  override def filterExprs(): Seq[Expression] = if (scan.isInstanceOf[FileScan]) {
    scan.asInstanceOf[FileScan].dataFilters ++ pushdownFilters
  } else {
    throw new UnsupportedOperationException(s"${scan.getClass.toString} is not supported")
  }

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = partitions

  override def getPartitionSchemas: StructType = scan match {
    case fileScan: FileScan => fileScan.readPartitionSchema
    case _ => new StructType()
  }

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def equals(other: Any): Boolean = other match {
    case that: BatchScanExecTransformer =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

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

  override def getChild: SparkPlan = {
    null
  }

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    outputVectors += outNumBatches
    outputRows += outNumRows
  }

  override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
    if (operatorMetrics != null) {
      inputRows += operatorMetrics.inputRows
      inputVectors += operatorMetrics.inputVectors
      inputBytes += operatorMetrics.inputBytes
      rawInputRows += operatorMetrics.rawInputRows
      rawInputBytes += operatorMetrics.rawInputBytes
      outputRows += operatorMetrics.outputRows
      outputVectors += operatorMetrics.outputVectors
      outputBytes += operatorMetrics.outputBytes
      count += operatorMetrics.count
      wallNanos += operatorMetrics.wallNanos
      peakMemoryBytes += operatorMetrics.peakMemoryBytes
      numMemoryAllocations += operatorMetrics.numMemoryAllocations
      numDynamicFiltersAccepted += operatorMetrics.numDynamicFiltersAccepted
    }
  }
}
