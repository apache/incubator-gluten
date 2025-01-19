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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.utils.ColumnarBatchUtils

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
    child: SparkPlan
) extends ColumnarCollectLimitExecBaseTransformer(limit, child) {

  override def batchType(): Convention.BatchType =
    BackendsApiManager.getSettings.primaryBatchType

  /**
   * Returns an iterator that yields up to `limit` rows in total from the input partitionIter.
   * Either retain the entire batch if it fits within the remaining limit, or prune it if it
   * partially exceeds the remaining limit.
   */
  private def collectLimitedRows(
      partitionIter: Iterator[ColumnarBatch],
      limit: Int
  ): Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

    private var rowsCollected = 0
    private var nextBatch: Option[ColumnarBatch] = None

    override def hasNext: Boolean = {
      nextBatch.isDefined || fetchNext()
    }

    override def next(): ColumnarBatch = {
      if (!hasNext) {
        throw new NoSuchElementException("No more batches available.")
      }
      val batch = nextBatch.get
      nextBatch = None
      batch
    }

    /**
     * Attempt to fetch the next batch from the underlying iterator if we haven't yet hit the limit.
     * Returns true if we found a new batch, false otherwise.
     */
    private def fetchNext(): Boolean = {
      if (rowsCollected >= limit || !partitionIter.hasNext) {
        return false
      }

      val currentBatch = partitionIter.next()
      val currentBatchRowCount = currentBatch.numRows()
      val remaining = limit - rowsCollected

      if (currentBatchRowCount <= remaining) {
        ColumnarBatches.retain(currentBatch)
        rowsCollected += currentBatchRowCount
        nextBatch = Some(currentBatch)
      } else {
        val prunedBatch = pruneBatch(currentBatch, remaining)
        rowsCollected += remaining
        nextBatch = Some(prunedBatch)
      }
      true
    }
  }

  private def pruneBatch(batch: ColumnarBatch, neededRows: Int): ColumnarBatch = {
    val loadedBatch = ColumnarBatches.load(ArrowBufferAllocators.contextInstance, batch)
    val prunedBatch = ColumnarBatchUtils.pruneBatch(loadedBatch, neededRows)
    VeloxColumnarBatches.toVeloxBatch(
      ColumnarBatches.offload(ArrowBufferAllocators.contextInstance, prunedBatch)
    )
  }

  private def takeLocalLimit(inputRDD: RDD[ColumnarBatch], limit: Int): RDD[ColumnarBatch] =
    inputRDD.mapPartitions(partition => collectLimitedRows(partition, limit))

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
    val childRDD = child.executeColumnar()

    if (childRDD.getNumPartitions == 0) {
      return sparkContext.parallelize(Seq.empty[ColumnarBatch], 1)
    }

    val processedRDD =
      if (childRDD.getNumPartitions == 1) childRDD
      else shuffleLimitedPartitions(childRDD)

    takeLocalLimit(processedRDD, limit)
  }

  private def shuffleLimitedPartitions(childRDD: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    val locallyLimited = takeLocalLimit(childRDD, limit)
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

  override protected def doExecute(): RDD[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException("doExecute is not supported for this operator")

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
