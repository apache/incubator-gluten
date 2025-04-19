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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.columnarbatch.VeloxColumnarBatches
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShuffledColumnarBatchRDD, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.metric.SQLColumnarShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable

case class ColumnarCollectTailExec(
    limit: Int,
    child: SparkPlan
) extends ColumnarCollectTailBaseExec(limit, child) {

  private def collectTailRows(
      partitionIter: Iterator[ColumnarBatch],
      limit: Int
  ): Iterator[ColumnarBatch] = {

    if (!partitionIter.hasNext || limit <= 0) {
      return Iterator.empty
    }

    val tailQueue = new mutable.ListBuffer[ColumnarBatch]()
    var totalRowsInTail = 0L

    while (partitionIter.hasNext) {
      val batch = partitionIter.next()
      val batchRows = batch.numRows()
      ColumnarBatches.retain(batch)
      tailQueue += batch
      totalRowsInTail += batchRows

      var canDrop = true
      while (tailQueue.nonEmpty && canDrop) {
        val front = tailQueue.head
        val frontRows = front.numRows().toLong

        if (totalRowsInTail - frontRows >= limit) {
          val dropped = tailQueue.remove(0)
          dropped.close()
          totalRowsInTail -= frontRows
        } else {
          canDrop = false
        }
      }
    }

    if (tailQueue.nonEmpty) {
      val first = tailQueue.remove(0)
      val overflow = totalRowsInTail - limit
      if (overflow > 0) {
        val keep = first.numRows() - overflow
        val sliced = VeloxColumnarBatches.slice(first, overflow.toInt, keep.toInt)
        tailQueue.prepend(sliced)
        first.close()
      } else {
        tailQueue.prepend(first)
      }
    }

    tailQueue.iterator
  }

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)

  private lazy val readMetrics =
    SQLColumnarShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  private lazy val useSortBasedShuffle: Boolean =
    BackendsApiManager.getSparkPlanExecApiInstance
      .useSortBasedShuffle(outputPartitioning, child.output)

  @transient private lazy val serializer: Serializer =
    BackendsApiManager.getSparkPlanExecApiInstance
      .createColumnarBatchSerializer(child.schema, metrics, useSortBasedShuffle)

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genColumnarShuffleExchangeMetrics(sparkContext, useSortBasedShuffle) ++
      readMetrics ++ writeMetrics

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childExecution = child.executeColumnar()

    if (childExecution.getNumPartitions == 0) {
      return sparkContext.parallelize(Seq.empty[ColumnarBatch], 1)
    }

    val processedRDD =
      if (childExecution.getNumPartitions == 1) childExecution
      else shuffleLimitedPartitions(childExecution)

    processedRDD.mapPartitions(partition => collectTailRows(partition, limit))
  }

  private def shuffleLimitedPartitions(childRDD: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    val locallyLimited = if (limit >= 0) {
      childRDD.mapPartitions(partition => collectTailRows(partition, limit))
    } else {
      childRDD
    }
    new ShuffledColumnarBatchRDD(
      BackendsApiManager.getSparkPlanExecApiInstance.genShuffleDependency(
        locallyLimited,
        child.output,
        child.output,
        SinglePartition,
        serializer,
        writeMetrics,
        metrics,
        useSortBasedShuffle
      ),
      readMetrics
    )
  }

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
