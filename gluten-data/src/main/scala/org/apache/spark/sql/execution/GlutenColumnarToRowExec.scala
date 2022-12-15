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

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, GlutenColumnarBatches, GlutenIndicatorVector}
import io.glutenproject.execution.GlutenColumnarToRowExecBase
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.{ArrowWritableColumnVector, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.NANOSECONDS

case class GlutenColumnarToRowExec(child: SparkPlan)
  extends GlutenColumnarToRowExecBase(child = child) {
  private val LOG = LoggerFactory.getLogger(classOf[GlutenColumnarToRowExec])

  override def nodeName: String = "GlutenColumnarToRowExec"

  override def supportCodegen: Boolean = false

  override def buildCheck(): Unit = {
    val schema = child.schema
    for (field <- schema.fields) {
      field.dataType match {
        case d: BooleanType =>
        case d: ByteType =>
        case d: ShortType =>
        case d: IntegerType =>
        case d: LongType =>
        case d: FloatType =>
        case d: DoubleType =>
        case d: StringType =>
        case d: TimestampType =>
        case d: DateType =>
        case d: BinaryType =>
        case _ =>
          throw new UnsupportedOperationException(s"${field.dataType} is not supported in " +
            s"GlutenColumnarToRowExecBase.")
      }
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")

    child.executeColumnar().mapPartitions { batches =>
      // TODO:: pass the jni jniWrapper and arrowSchema  and serializeSchema method by broadcast
      val jniWrapper = new NativeColumnarToRowJniWrapper()

      batches.flatMap { batch =>
        numInputBatches += 1
        numOutputRows += batch.numRows()

        if (batch.numRows == 0) {
          logInfo(s"Skip ColumnarBatch of ${batch.numRows} rows, ${batch.numCols} cols")
          Iterator.empty
        } else if (this.output.isEmpty || (batch.numCols() > 0 &&
          !batch.column(0).isInstanceOf[ArrowWritableColumnVector] &&
          !batch.column(0).isInstanceOf[GlutenIndicatorVector])) {
          // Fallback to ColumnarToRow
          val localOutput = this.output
          numInputBatches += 1
          numOutputRows += batch.numRows()

          val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
          ArrowColumnarBatches
            .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
            .rowIterator().asScala.map(toUnsafe)
        } else {
          var info: NativeColumnarToRowInfo = null
          val beforeConvert = System.nanoTime()
          val offloaded =
            ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch)
          val batchHandle = GlutenColumnarBatches.getNativeHandle(offloaded)
          info = jniWrapper.nativeConvertColumnarToRow(
            batchHandle,
            NativeMemoryAllocators.contextInstance().getNativeInstanceId)

          convertTime += NANOSECONDS.toMillis(System.nanoTime() - beforeConvert)

          new Iterator[InternalRow] {
            var rowId = 0
            val row = new UnsafeRow(batch.numCols())
            var closed = false

            TaskContext.get().addTaskCompletionListener[Unit](_ => {
              if (!closed) {
                jniWrapper.nativeClose(info.instanceID)
                closed = true
              }
            })

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
          }
        }
      }
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[GlutenColumnarToRowExec]

  override def equals(other: Any): Boolean = other match {
    case that: GlutenColumnarToRowExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]]) // Hack because of type erasure
  }

  override def output: Seq[Attribute] = child.output

  protected def doProduce(ctx: CodegenContext): String = {
    throw new RuntimeException("Codegen is not supported!")
  }

  protected def withNewChildInternal(newChild: SparkPlan): GlutenColumnarToRowExec =
    copy(child = newChild)
}
