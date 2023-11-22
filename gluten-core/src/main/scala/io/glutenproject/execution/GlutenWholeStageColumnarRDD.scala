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
import io.glutenproject.metrics.IMetrics
import io.glutenproject.substrait.plan.PlanBuilder

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ExecutorManager

import scala.collection.mutable

trait BaseGlutenPartition extends Partition with InputPartition {
  def plan: Array[Byte]
}

case class GlutenPartition(
    index: Int,
    plan: Array[Byte],
    locations: Array[String] = Array.empty[String])
  extends BaseGlutenPartition {

  override def preferredLocations(): Array[String] = locations
}

case class GlutenFilePartition(index: Int, files: Array[PartitionedFile], plan: Array[Byte])
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach {
      file =>
        file.locations.filter(_ != "localhost").foreach {
          host => hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
        }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq
      .sortBy { case (host, numBytes) => numBytes }
      .reverse
      .take(3)
      .map { case (host, numBytes) => host }
      .toArray
  }
}

case class GlutenMergeTreePartition(
    index: Int,
    engine: String,
    database: String,
    table: String,
    tablePath: String,
    minParts: Long,
    maxParts: Long,
    plan: Array[Byte] = PlanBuilder.EMPTY_PLAN)
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }
}

case class FirstZippedPartitionsPartition(
    index: Int,
    inputPartition: InputPartition,
    inputColumnarRDDPartitions: Seq[Partition] = Seq.empty)
  extends Partition

class GlutenWholeStageColumnarRDD(
    @transient sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    var rdds: ColumnarInputRDDsWrapper,
    pipelineTime: SQLMetric,
    updateInputMetrics: (InputMetricsWrapper) => Unit,
    updateNativeMetrics: IMetrics => Unit)
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)
    val (inputPartition, inputColumnarRDDPartitions) = castNativePartition(split)
    val inputIterators = rdds.getIterators(inputColumnarRDDPartitions, context)
    BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
      inputPartition,
      context,
      pipelineTime,
      updateInputMetrics,
      updateNativeMetrics,
      inputIterators
    )
  }

  private def castNativePartition(split: Partition): (BaseGlutenPartition, Seq[Partition]) = {
    split match {
      case FirstZippedPartitionsPartition(_, g: BaseGlutenPartition, p) => (g, p)
      case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castNativePartition(split)._1.preferredLocations()
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](inputPartitions.size) {
      i => FirstZippedPartitionsPartition(i, inputPartitions(i), rdds.getPartitions(i))
    }
  }

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
