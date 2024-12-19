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

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * RangeExecTransformer is a concrete implementation of RangeExecBaseTransformer that executes the
 * Range operation and supports columnar processing. It generates columnar batches for the specified
 * range.
 *
 * @param start
 *   Starting value of the range.
 * @param end
 *   Ending value of the range.
 * @param step
 *   Step size for the range.
 * @param numSlices
 *   Number of slices for partitioning the range.
 * @param numElements
 *   Total number of elements in the range.
 * @param outputAttributes
 *   Attributes defining the output schema of the operator.
 * @param child
 *   Child SparkPlan nodes for this operator, if any.
 */
case class RangeExecTransformer(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Int,
    numElements: BigInt,
    outputAttributes: Seq[Attribute],
    child: Seq[SparkPlan]
) extends RangeExecBaseTransformer(
    start,
    end,
    step,
    numSlices,
    numElements,
    outputAttributes,
    child) {

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (start == end || (start < end ^ 0 < step)) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex {
          (partitionIndex, _) =>
            val allocator = ArrowBufferAllocators.contextInstance()
            val sessionLocalTimeZone = SQLConf.get.sessionLocalTimeZone
            val arrowSchema = SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)

            val batchSize = 1000
            val safePartitionStart =
              start + step * (partitionIndex * numElements.toLong / numSlices)
            val safePartitionEnd =
              start + step * ((partitionIndex + 1) * numElements.toLong / numSlices)

            /**
             * Generates the columnar batches for the specified range. Each batch contains a subset
             * of the range values, managed using Arrow column vectors.
             */
            val iterator = new Iterator[ColumnarBatch] {
              var current = safePartitionStart

              override def hasNext: Boolean = {
                if (step > 0) {
                  current < safePartitionEnd
                } else {
                  current > safePartitionEnd
                }
              }

              override def next(): ColumnarBatch = {
                val numRows = math.min(
                  ((safePartitionEnd - current) / step).toInt.max(1),
                  batchSize
                )

                val vectors = ArrowWritableColumnVector.allocateColumns(numRows, schema)

                for (i <- 0 until numRows) {
                  val value = current + i * step
                  vectors(0).putLong(i, value)
                }
                vectors.foreach(_.setValueCount(numRows))
                current += numRows * step

                val batch = new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]], numRows)
                val offloadedBatch = ColumnarBatches.offload(allocator, batch)
                offloadedBatch
              }
            }
            Iterators
              .wrap(iterator)
              .recyclePayload(
                batch => {
                  batch.close()
                })
              .recycleIterator {
                allocator.close()
              }
              .create()

        }
    }
  }

  override protected def doExecute(): RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException("doExecute is not supported for this operator")
  }
}
