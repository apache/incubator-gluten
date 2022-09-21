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

import java.util.concurrent.TimeUnit.NANOSECONDS

import io.glutenproject.expression.ConverterUtils
import io.glutenproject.vectorized.{BlockNativeConverter, CHNativeBlock}

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * this class is only for test use, not yet suggested to use in non-test code
 * the performance is not optimized
 *
 * @param child
 *
 */
case class RowToCHNativeColumnarExec(child: SparkPlan)
    extends RowToArrowColumnarExec(child = child) {

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val processTime = longMetric("processTime")
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    val fieldNames = output.map(ConverterUtils.genColumnNameWithExprId(_)).toArray
    val fieldTypes = output.map(_.dataType.typeName).toArray
    val nullables = output.map(_.nullable).toArray
    child.execute().mapPartitions { rowIterator =>
      val projection = UnsafeProjection.create(localSchema)
      val cvt = new BlockNativeConverter
      if (rowIterator.hasNext) {
        val res = new Iterator[ColumnarBatch] {
          private val byteArrayIterator = rowIterator.map {
            case u: UnsafeRow => u.getBytes
            case i: InternalRow => projection.apply(i).getBytes
          }

          private var last_address: Long = 0
          private var elapse: Long = 0

          override def hasNext: Boolean = {
            if (last_address != 0) {
              cvt.freeBlock(last_address)
              last_address = 0
            }
            byteArrayIterator.hasNext
          }

          override def next(): ColumnarBatch = {
            val start = System.nanoTime()
            val slice = byteArrayIterator.take(8192);
            val sparkRowIterator = new SparkRowIterator(slice)
            last_address = cvt.convertSparkRowsToCHColumn(
              sparkRowIterator,
              fieldNames,
              fieldTypes,
              nullables)
            elapse += System.nanoTime() - start
            val block = new CHNativeBlock(last_address)

            processTime += NANOSECONDS.toMillis(elapse)
            numInputRows += block.numRows()
            numOutputBatches += 1

            block.toColumnarBatch
          }
        }
        res
      } else {
        Iterator.empty
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = true

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowToCHNativeColumnarExec =
    copy(child = newChild)

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }
}
