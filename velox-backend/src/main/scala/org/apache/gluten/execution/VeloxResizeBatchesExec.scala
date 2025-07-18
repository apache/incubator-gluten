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
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.utils.VeloxBatchResizer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

/**
 * An operator to resize input batches by appending the later batches to the one that comes earlier,
 * or splitting one batch to smaller ones.
 *
 * FIXME: Code duplication with ColumnarToColumnarExec.
 */
case class VeloxResizeBatchesExec(
    override val child: SparkPlan,
    minOutputBatchSize: Int,
    maxOutputBatchSize: Int)
  extends GlutenPlan
  with UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "selfTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to append / split batches")
  )

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val selfTime = longMetric("selfTime")

    child.executeColumnar().mapPartitions {
      in =>
        // Append millis = Out millis - In millis.
        val appendMillis = new AtomicLong(0L)
        val appender = VeloxBatchResizer.create(
          minOutputBatchSize,
          maxOutputBatchSize,
          Iterators
            .wrap(in)
            .collectReadMillis(inMillis => appendMillis.getAndAdd(-inMillis))
            .create()
            .map {
              inBatch =>
                numInputRows += inBatch.numRows()
                numInputBatches += 1
                inBatch
            }
            .asJava
        )

        val out = Iterators
          .wrap(appender.asScala)
          .protectInvocationFlow()
          .collectReadMillis(outMillis => appendMillis.getAndAdd(outMillis))
          .recyclePayload(_.close())
          .recycleIterator {
            appender.close()
            selfTime += appendMillis.get()
          }
          .create()
          .map {
            outBatch =>
              numOutputRows += outBatch.numRows()
              numOutputBatches += 1
              outBatch
          }

        out
    }
  }

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
