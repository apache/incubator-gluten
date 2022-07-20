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
import io.glutenproject.vectorized.OperatorMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

class FileSourceScanExecTransformer(@transient relation: HadoopFsRelation,
                                    output: Seq[Attribute],
                                    requiredSchema: StructType,
                                    partitionFilters: Seq[Expression],
                                    optionalBucketSet: Option[BitSet],
                                    optionalNumCoalescedBuckets: Option[Int],
                                    dataFilters: Seq[Expression],
                                    tableIdentifier: Option[TableIdentifier],
                                    disableBucketedScan: Boolean = false)
  extends FileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
    with BasicScanExecTransformer {

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
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "cpu wall nanos"),
    "cpuNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "cpu nanos"),
    "blockedWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "block wall nanos"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"))

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
  val cpuNanos: SQLMetric = longMetric("cpuNanos")
  val blockedWallNanos: SQLMetric = longMetric("blockedWallNanos")
  val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

  override lazy val supportsColumnar: Boolean = {
    relation.fileFormat
      .supportBatch(relation.sparkSession, schema) && GlutenConfig.getConf.enableColumnarIterator
  }

  override def filterExprs(): Seq[Expression] = dataFilters

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(relation, selectedPartitions)

  override def equals(other: Any): Boolean = other match {
    case that: FileSourceScanExecTransformer =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[FileSourceScanExecTransformer]

  override def hashCode(): Int = super.hashCode()

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

  override def doValidate(): Boolean = {
    if (BackendsApiManager.getTransformerApiInstance.supportsReadFileFormat(relation.fileFormat)) {
      super.doValidate()
    } else {
      false
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
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
      cpuNanos += operatorMetrics.cpuNanos
      blockedWallNanos += operatorMetrics.blockedWallNanos
      peakMemoryBytes += operatorMetrics.peakMemoryBytes
      numMemoryAllocations += operatorMetrics.numMemoryAllocations
    }
  }
}
