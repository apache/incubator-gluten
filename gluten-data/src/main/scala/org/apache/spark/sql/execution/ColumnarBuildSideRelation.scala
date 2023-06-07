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

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, GlutenColumnarBatches}
import io.glutenproject.execution.BroadCastHashJoinContext
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.ArrowAbiUtil
import io.glutenproject.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}
import org.apache.arrow.c.ArrowSchema

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.memory.TaskResources

case class ColumnarBuildSideRelation(mode: BroadcastMode,
                                   output: Seq[Attribute],
                                   batches: Array[Array[Byte]])
  extends BuildSideRelation with Logging {

  override def deserialized: Iterator[ColumnarBatch] = {
    try {
      new Iterator[ColumnarBatch] {
        var batchId = 0
        var closed = false
        private var finalBatch = -1L
        val serializeHandle = {
          val allocator = ArrowBufferAllocators.contextInstance()
          val cSchema = ArrowSchema.allocateNew(allocator)
          val arrowSchema = SparkArrowUtil.toArrowSchema(StructType.fromAttributes(output),
            SQLConf.get.sessionLocalTimeZone)
          ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
          val handle = ColumnarBatchSerializerJniWrapper.INSTANCE.init(cSchema.memoryAddress(),
            NativeMemoryAllocators.contextInstance().getNativeInstanceId)
          cSchema.close()
          handle
        }

        override def hasNext: Boolean = {
          val has = batchId < batches.length
          if (!has && !closed) {
            ArrowColumnarBatches.close(finalBatch)
            TaskResources.addRecycler(50) {
              ColumnarBatchSerializerJniWrapper.INSTANCE.close(serializeHandle)
            }
            closed = true
          }
          has
        }

        override def next: ColumnarBatch = {
          val handle = ColumnarBatchSerializerJniWrapper.INSTANCE.deserialize(
            serializeHandle, batches(batchId))
          if (batchId == batches.length - 1) {
            finalBatch = handle
          }
          batchId += 1
          GlutenColumnarBatches.create(handle)
        }
      }
    }
  }

  override def asReadOnlyCopy(broadCastContext: BroadCastHashJoinContext
  ): ColumnarBuildSideRelation = this

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct.
   * @return
   */
  override def transform(key: Expression): Array[InternalRow] = {
    // convert batches: Array[Array[Byte]] to Array[InternalRow] by key and distinct.
    val batchIter = ArrowUtil.convertFromNetty(output, batches)

    // Convert columnar to Row.
    val jniWrapper = new NativeColumnarToRowJniWrapper()
    var c2rId = -1L
    var closed = false
    val iterator = if (batchIter.hasNext) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchIter.hasNext
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batch = batchIter.next()

          if (batch.numRows == 0) {
            Iterator.empty
          } else if (output.isEmpty || (batch.numCols() > 0 &&
            !batch.column(0).isInstanceOf[ArrowWritableColumnVector] &&
            !batch.column(0).isInstanceOf[IndicatorVector])) {
            // Fallback to ColumnarToRow
            val localOutput = output
            val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
            val arrowBatch = ArrowColumnarBatches
              .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
            arrowBatch.rowIterator().asScala.map(toUnsafe)
          } else {
            val offloaded =
              ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch)
            val batchHandle = GlutenColumnarBatches.getNativeHandle(offloaded)

            if (c2rId == -1) {
              c2rId = jniWrapper.nativeColumnarToRowInit(
                batchHandle,
                NativeMemoryAllocators.contextInstance().getNativeInstanceId)
            }
            val info = jniWrapper.nativeColumnarToRowWrite(batchHandle, c2rId)

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
              throw new IllegalArgumentException(s"Multiple key columns found in expression: $key")
            }
            val columnExpr = columnNames.head

            val columnInOutput = output.zipWithIndex.filter {
              p: (Attribute, Int) =>
                if (output.size == 1) {
                  p._1.name == columnExpr.name
                } else {
                  // A case where output has multiple columns with same name
                  p._1.name == columnExpr.name && p._1.exprId == columnExpr.exprId
                }
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

              override def hasNext: Boolean = {
                rowId < batch.numRows()
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
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
    iterator.toArray
  }
}
