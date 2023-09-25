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

import io.glutenproject.GlutenNumaBindingInfo
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.metrics.IMetrics

import org.apache.spark.{OneToOneDependency, Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable

private[glutenproject] class ZippedPartitionsPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]])
  extends Partition {

  override val index: Int = idx
  var partitionValues: Seq[Partition] = rdds.map(rdd => rdd.partitions(idx))

  def partitions: Seq[Partition] = partitionValues
}

class WholeStageZippedPartitionsRDD(
    @transient private val sc: SparkContext,
    var rdds: Seq[RDD[ColumnarBatch]],
    numaBindingInfo: GlutenNumaBindingInfo,
    sparkConf: SparkConf,
    resCtx: WholeStageTransformContext,
    pipelineTime: SQLMetric,
    buildRelationBatchHolder: mutable.ListBuffer[ColumnarBatch],
    updateNativeMetrics: IMetrics => Unit,
    materializeInput: Boolean)
  extends RDD[ColumnarBatch](sc, rdds.map(x => new OneToOneDependency(x))) {

  private val genFinalStageIterator = (inputIterators: Seq[Iterator[ColumnarBatch]]) => {
    BackendsApiManager.getIteratorApiInstance
      .genFinalStageIterator(
        inputIterators,
        numaBindingInfo,
        sparkConf,
        resCtx.root,
        pipelineTime,
        updateNativeMetrics,
        buildRelationBatchHolder,
        materializeInput
      )
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    val inputIterators: Seq[Iterator[ColumnarBatch]] =
      rdds.zip(partitions).map { case (rdd, partition) => rdd.iterator(partition, context) }
    genFinalStageIterator(inputIterators)
  }

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    Array.tabulate[Partition](numParts)(i => new ZippedPartitionsPartition(i, rdds))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
