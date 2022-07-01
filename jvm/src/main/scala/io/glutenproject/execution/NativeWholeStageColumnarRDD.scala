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

import java.io.Serializable

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.GlutenConfig
import org.apache.spark._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util._

trait BaseNativeFilePartition extends Partition with InputPartition {
  def substraitPlan: Array[Byte]
}

case class NativeFilePartition(index: Int, files: Array[PartitionedFile],
                               substraitPlan: Array[Byte])
  extends BaseNativeFilePartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

case class FirstZippedPartitionsPartition(
     idx: Int, inputPartition: InputPartition,
     @transient private val rdds: Seq[RDD[_]] = Seq()) extends Partition with Serializable {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))

  def partitions: Seq[Partition] = partitionValues
}

class NativeWholeStageColumnarRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    outputAttributes: Seq[Attribute],
    var rdds: Seq[RDD[ColumnarBatch]],
    pipelineTime: SQLMetric,
    val updateMetrics: (Long, Long) => Unit)
    extends RDD[ColumnarBatch](sc, rdds.map(x => new OneToOneDependency(x))) {
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val loadNative: Boolean = GlutenConfig.getConf.loadNative

  override protected def getPartitions: Array[Partition] = {
    if (rdds.isEmpty) {
      inputPartitions.zipWithIndex.map {
        case (inputPartition, index) => FirstZippedPartitionsPartition(index, inputPartition)
      }.toArray
    } else {
      val numParts = inputPartitions.size
      if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
        throw new IllegalArgumentException(
          s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
      }
      Array.tabulate[Partition](numParts) { i =>
        FirstZippedPartitionsPartition(i, inputPartitions(i), rdds) }
    }
  }

  private def castPartition(split: Partition): FirstZippedPartitionsPartition = split match {
    case p: FirstZippedPartitionsPartition => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  private def castNativePartition(split: Partition): BaseNativeFilePartition = split match {
    case FirstZippedPartitionsPartition(_, p: BaseNativeFilePartition, _) => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)

    val inputPartition = castNativePartition(split)
    if (rdds.isEmpty) {
      BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
        inputPartition,
        loadNative,
        outputAttributes,
        context,
        pipelineTime,
        updateMetrics)
    } else {
      val partitions = split.asInstanceOf[FirstZippedPartitionsPartition].partitions
      val inputIterators = (rdds zip partitions).map {
        case (rdd, partition) => rdd.iterator(partition, context)
      }
      BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
        inputPartition,
        loadNative,
        outputAttributes,
        context,
        pipelineTime,
        updateMetrics,
        inputIterators)
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

}
