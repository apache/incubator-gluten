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

package org.apache.spark.sql.execution

import scala.collection.JavaConverters.asScalaIteratorConverter

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, GlutenColumnarBatches, GlutenIndicatorVector}
import io.glutenproject.execution.BroadCastHashJoinContext
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.GlutenArrowUtil
import io.glutenproject.vectorized.{ArrowWritableColumnVector, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GlutenBuildSideRelation(mode: BroadcastMode,
  output: Seq[Attribute],
  batches: Array[Array[Byte]])
  extends BuildSideRelation {

  override def deserialized: Iterator[ColumnarBatch] = {
    GlutenArrowUtil.convertFromNetty(output, batches)
  }

  override def asReadOnlyCopy(broadCastContext: BroadCastHashJoinContext
  ): GlutenBuildSideRelation = this

  /**
   * Transform columnar broadcasted value to Array[InternalRow] by key and distinct.
   * @return
   */
  override def transform(key: Expression): Array[InternalRow] = {
    // convert batches: Array[Array[Byte]] to Array[InternalRow] by key and distinct.
    val batchIter = GlutenArrowUtil.convertFromNetty(output, batches)
    // Convert columnar to Row.
    batchIter.flatMap(batch => {
      if (batch.numRows == 0) {
        Iterator.empty
      } else if (this.output.isEmpty || (batch.numCols() > 0 &&
        !batch.column(0).isInstanceOf[ArrowWritableColumnVector] &&
        !batch.column(0).isInstanceOf[GlutenIndicatorVector])) {
        // Fallback to ColumnarToRow
        val localOutput = this.output
        val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
        val arrowBatch = ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
        arrowBatch.rowIterator().asScala.map(toUnsafe)
      } else {
        val jniWrapper = new NativeColumnarToRowJniWrapper()
        val offloaded =
          ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch)
        val batchHandle = GlutenColumnarBatches.getNativeHandle(offloaded)
        val info: NativeColumnarToRowInfo = jniWrapper.nativeConvertColumnarToRow(
          batchHandle,
          NativeMemoryAllocators.contextInstance().getNativeInstanceId)

        val columnNames = key.flatMap {
          case expression: AttributeReference =>
            Some(expression)
          case _ =>
            None
        }
        if (columnNames.isEmpty) {
          throw new IllegalArgumentException(s"Key column not found in expression: $key")
        }
        if (columnNames.size != 1) {
          throw new IllegalArgumentException(s"Multiple key column not found in expression: $key")
        }
        val columnExpr = columnNames.head

        val columnInOutput = output.zipWithIndex.filter {
          p: (Attribute, Int) =>
            p._1.name == columnExpr.name
        }
        if (columnInOutput.isEmpty) {
          throw new IllegalStateException(
            s"Key $key not found from build side relation output: $output")
        }
        if (columnInOutput.size != 1) {
          throw new IllegalStateException(
            s"More than one key $key found from build side relation output: $output")
        }
        val replacement =
          BoundReference(columnInOutput.head._2, columnExpr.dataType, columnExpr.nullable)

        val projExpr = key.transformDown {
          case _: AttributeReference =>
            replacement
        }

        val proj = UnsafeProjection.create(projExpr)

        new Iterator[InternalRow] {
          var rowId = 0
          val row = new UnsafeRow(batch.numCols())
          var closed = false

          override def hasNext: Boolean = {
            val result = rowId < batch.numRows()
            if (!result && !closed) {
              jniWrapper.nativeClose(info.instanceID)
              closed = true
            }
            result
          }

          override def next: UnsafeRow = {
            if (rowId >= batch.numRows()) throw new NoSuchElementException

            val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
            row.pointTo(null, info.memoryAddress + offset, length.toInt)
            rowId += 1
            row
          }
        }.map(proj).map(_.copy())
      }
    }).toArray
  }
}
