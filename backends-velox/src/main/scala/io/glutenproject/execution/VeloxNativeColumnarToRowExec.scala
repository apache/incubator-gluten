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

import io.glutenproject.utils.ArrowAbiUtil
import io.glutenproject.vectorized.{ArrowWritableColumnVector, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}
import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class VeloxNativeColumnarToRowExec(child: SparkPlan)
  extends NativeColumnarToRowExec(child = child) {
  private val LOG = LoggerFactory.getLogger(classOf[VeloxNativeColumnarToRowExec])
  override def nodeName: String = "VeloxNativeColumnarToRowExec"

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
        case d: DateType =>
        case d: DecimalType =>
        case d: TimestampType =>
        case d: BinaryType =>
        case _ =>
          throw new UnsupportedOperationException(s"${field.dataType} is not supported in NativeColumnarToRowExec.")
      }
    }
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
          !batch.column(0).isInstanceOf[ArrowWritableColumnVector])) {
          // Fallback to ColumnarToRow
          val localOutput = this.output
          numInputBatches += 1
          numOutputRows += batch.numRows()

          val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
          batch.rowIterator().asScala.map(toUnsafe)
        } else {
          val allocator = SparkMemoryUtils.contextArrowAllocator()
          val cArray = ArrowArray.allocateNew(allocator)
          val cSchema = ArrowSchema.allocateNew(allocator)
          var info : NativeColumnarToRowInfo = null
          try {
            ArrowAbiUtil.exportFromSparkColumnarBatch(
              SparkMemoryUtils.contextArrowAllocator(), batch, cSchema, cArray)
            val beforeConvert = System.nanoTime()

            info = jniWrapper.nativeConvertColumnarToRow(
              cSchema.memoryAddress(), cArray.memoryAddress(),
              SparkMemoryUtils.contextNativeAllocator().getNativeInstanceId, wsChild)

            convertTime += NANOSECONDS.toMillis(System.nanoTime() - beforeConvert)
          } finally {
            cArray.close()
            cSchema.close()
          }

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
              return result
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

  override def canEqual(other: Any): Boolean = other.isInstanceOf[VeloxNativeColumnarToRowExec]

  override def equals(other: Any): Boolean = other match {
    case that: VeloxNativeColumnarToRowExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

