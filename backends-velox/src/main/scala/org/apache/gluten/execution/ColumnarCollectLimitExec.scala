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

case class ColumnarCollectLimitExec(
    limit: Int,
    child: SparkPlan,
    offset: Int = 0
) extends ColumnarCollectLimitBaseExec(limit, child, offset) {

  override def batchType(): Convention.BatchType =
    BackendsApiManager.getSettings.primaryBatchType

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

  /**
   * Returns an iterator that gives offset to limit rows in total from the input partitionIter.
   * Either retain the entire batch if it fits within the remaining limit, or prune it if it
   * partially exceeds the remaining limit/offset.
   */
  private def collectWithOffsetAndLimit(
      inputIter: Iterator[ColumnarBatch],
      offset: Int,
      limit: Int): Iterator[ColumnarBatch] = {

    val unlimited = limit < 0
    var rowsToSkip = math.max(offset, 0)
    var rowsToCollect = if (unlimited) Int.MaxValue else limit

    new Iterator[ColumnarBatch] {
      private var nextBatch: Option[ColumnarBatch] = None

      override def hasNext: Boolean = {
        nextBatch.isDefined || fetchNextBatch()
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) throw new NoSuchElementException("No more batches available.")
        val batch = nextBatch.get
        nextBatch = None
        batch
      }

      /**
       * Advance the iterator until we find a batch (possibly sliced) that we can return, or exhaust
       * the input.
       */
      private def fetchNextBatch(): Boolean = {

        if (rowsToCollect <= 0) return false

        while (inputIter.hasNext) {
          val batch = inputIter.next()
          val batchSize = batch.numRows()

          if (rowsToSkip >= batchSize) {
            rowsToSkip -= batchSize
          } else {
            val startIndex = rowsToSkip
            val leftoverAfterSkip = batchSize - startIndex
            rowsToSkip = 0

            val needed = math.min(rowsToCollect, leftoverAfterSkip)

            val prunedBatch =
              if (startIndex == 0 && needed == batchSize) {
                ColumnarBatches.retain(batch)
                batch
              } else {
                VeloxColumnarBatches.slice(batch, startIndex, needed)
              }

            rowsToCollect -= needed
            nextBatch = Some(prunedBatch)
            return true
          }
        }
        false
      }
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()

    if (childRDD.getNumPartitions == 0) {
      return sparkContext.parallelize(Seq.empty[ColumnarBatch], 1)
    }

    val processedRDD =
      if (childRDD.getNumPartitions == 1) childRDD
      else shuffleLimitedPartitions(childRDD)

    processedRDD.mapPartitions(
      partition => {
        if (limit > 0) {
          val adjusted = math.max(0, limit - offset)
          collectWithOffsetAndLimit(partition, offset, adjusted)
        } else {
          collectWithOffsetAndLimit(partition, offset, -1)
        }
      })
  }

  private def shuffleLimitedPartitions(childRDD: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    val applyLocalLimit = (offset == 0 && limit >= 0)
    val locallyLimited = if (applyLocalLimit) {
      childRDD.mapPartitions {
        collectWithOffsetAndLimit(_, 0, limit)
      }
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
