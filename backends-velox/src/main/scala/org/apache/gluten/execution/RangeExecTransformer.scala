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
  // scalastyle:off println
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    println(s"[arnavb] RangeExecBaseTransformer 47")
    if (start == end || (start < end ^ 0 < step)) {
      println(s"[arnavb] RangeExecBaseTransformer 49")
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      println(s"[arnavb] RangeExecBaseTransformer 52")
      // scalastyle:off println
      println(s"[arnavb] RangeExecBaseTransformer 55")
      sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex {
          (partitionIndex, _) =>
            println(s"[arnavb] RangeExecBaseTransformer 62")
            val sessionLocalTimeZone = SQLConf.get.sessionLocalTimeZone
            val allocator = ArrowBufferAllocators.contextInstance()
            val arrowSchema = SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
            val batchSize = 2
            val safePartitionStart =
              start + step * (partitionIndex * numElements.toLong / numSlices)
            val safePartitionEnd =
              start + step * ((partitionIndex + 1) * numElements.toLong / numSlices)

            val iterator = new Iterator[ColumnarBatch] {
              var current = safePartitionStart

              override def hasNext: Boolean = {
                if (step > 0) current < safePartitionEnd
                else current > safePartitionEnd
              }

              override def next(): ColumnarBatch = {
                val numRows = math.min(
                  ((safePartitionEnd - current) / step).toInt.max(1),
                  batchSize
                )

                val vectors = ArrowWritableColumnVector.allocateColumns(numRows, schema)

                for (i <- 0 until numRows) {
                  // val value = current + i * step
                  val value = 1
                  vectors(0).putLong(i, value)
                }
                vectors.foreach(_.setValueCount(numRows))
                val batch = new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]], numRows)
                current += numRows * step
                val offloadedBatch = ColumnarBatches.offload(allocator, batch)
                println(s"[arnavb] returning offloaded batch 94")
                println(s"[arnavb] going to return the wrapped batch 110")
                offloadedBatch
              }
              println(s"[arnavb] returning offloaded batch 112")
            }
            Iterators
              .wrap(iterator)
              .recyclePayload(_.close())
              .recyclePayload(
                batch => {
                  println(s"[arnavb] Closing batch with rows1.")
                  batch.close()
                })
              .recyclePayload(
                offloadedBatch => {
                  println(s"[arnavb] Closing batch with rows2.")
                  offloadedBatch.close()
                })
              .recycleIterator(
                () => {
                  println(s"[arnavb] Closing allocator for partition1.")
                  allocator.close()
                  try {
                    println(s"[arnavb] Closing iterator2.")
                    println(s"[arnavb] Iterator successfully closed3.")
                  } catch {
                    case e: Exception =>
                      println(s"[arnavb] Error while closing iterator3: ${e.getMessage}")
                      throw e
                  }
                })
              .create()

        }
    }
  }

  override protected def doExecute(): RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException("Custom execution is not implemented yet.")
  }
}
