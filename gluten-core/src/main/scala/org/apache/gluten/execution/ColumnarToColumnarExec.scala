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

import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.KnownBatchType
import org.apache.gluten.extension.columnar.transition.ConventionReq.KnownChildrenConventions
import org.apache.gluten.iterator.Iterators

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicLong

abstract class ColumnarToColumnarExec(from: Convention.BatchType, to: Convention.BatchType)
  extends ColumnarToColumnarTransition
  with KnownBatchType
  with KnownChildrenConventions {

  def child: SparkPlan
  protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "selfTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert batches")
    )

  override def supportsColumnar: Boolean = true
  override def batchType(): Convention.BatchType = to
  override def requiredChildrenConventions(): Seq[ConventionReq] = List(
    ConventionReq.of(ConventionReq.RowType.Any, ConventionReq.BatchType.Is(from)))

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val selfTime = longMetric("selfTime")

    child.executeColumnar().mapPartitions {
      in =>
        // Self millis = Out millis - In millis.
        val selfMillis = new AtomicLong(0L)
        val wrappedIn = Iterators
          .wrap(in)
          .collectReadMillis(inMillis => selfMillis.getAndAdd(-inMillis))
          .create()
          .map {
            inBatch =>
              numInputRows += inBatch.numRows()
              numInputBatches += 1
              inBatch
          }
        val out = mapIterator(wrappedIn)
        val wrappedOut = Iterators
          .wrap(out)
          .collectReadMillis(outMillis => selfMillis.getAndAdd(outMillis))
          .recycleIterator {
            selfTime += selfMillis.get()
          }
          .create()
          .map {
            outBatch =>
              numOutputRows += outBatch.numRows()
              numOutputBatches += 1
              outBatch
          }
        wrappedOut
    }

  }

  override def output: Seq[Attribute] = child.output
}
