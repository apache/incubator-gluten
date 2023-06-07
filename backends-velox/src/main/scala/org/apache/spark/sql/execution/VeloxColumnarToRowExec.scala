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

import scala.collection.JavaConverters._
import scala.concurrent.duration.NANOSECONDS

import io.glutenproject.columnarbatch.GlutenColumnarBatches
import io.glutenproject.execution.ColumnarToRowExecBase
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.vectorized.{NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}
import org.slf4j.LoggerFactory

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.memory.TaskResources

case class VeloxColumnarToRowExec(child: SparkPlan)
  extends ColumnarToRowExecBase(child = child) {
  private val LOG = LoggerFactory.getLogger(classOf[VeloxColumnarToRowExec])

  override def nodeName: String = "VeloxColumnarToRowExec"

  override def buildCheck(): Unit = {
    val schema = child.schema
    // Depending on the input type, VeloxColumnarToRowConverter.
    for (field <- schema.fields) {
      field.dataType match {
        case _: BooleanType =>
        case _: ByteType =>
        case _: ShortType =>
        case _: IntegerType =>
        case _: LongType =>
        case _: FloatType =>
        case _: DoubleType =>
        case _: StringType =>
        case _: TimestampType =>
        case _: DateType =>
        case _: BinaryType =>
        case _: DecimalType =>
        case _ =>
          throw new UnsupportedOperationException(s"${field.dataType} is not supported in " +
            s"VeloxColumnarToRowExec.")
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

    new ColumnarToRowRDD(sparkContext, child.executeColumnar(), this.output,
      numOutputRows, numInputBatches, convertTime)
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected def withNewChildInternal(newChild: SparkPlan): VeloxColumnarToRowExec =
    copy(child = newChild)
}

class ColumnarToRowRDD(@transient sc: SparkContext, rdd: RDD[ColumnarBatch],
    output: Seq[Attribute], numOutputRows: SQLMetric, numInputBatches: SQLMetric,
    convertTime: SQLMetric)
  extends RDD[InternalRow](sc, Seq(new OneToOneDependency(rdd))) {

  private val cleanedF = sc.clean(f)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    cleanedF(firstParent[ColumnarBatch].iterator(split, context))
  }

  private def f: Iterator[ColumnarBatch] => Iterator[InternalRow] = { batches =>
    // TODO:: pass the jni jniWrapper and arrowSchema  and serializeSchema method by broadcast
    val jniWrapper = new NativeColumnarToRowJniWrapper()
    // Init NativeColumnarToRow with the first ColumnarBatch
    var c2rId = -1L
    var closed = false
    if (batches.hasNext) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {

        TaskResources.addRecycler(100) {
          if (!closed) {
            jniWrapper.nativeClose(c2rId)
            closed = true
          }
        }

        override def hasNext: Boolean = {
          val itHasNext = batches.hasNext
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batch = batches.next()
          numInputBatches += 1
          numOutputRows += batch.numRows()

          val nonGlutenBatch = batch.numCols() > 0 &&
            !batch.column(0).isInstanceOf[ArrowWritableColumnVector] &&
            !batch.column(0).isInstanceOf[IndicatorVector]
          if (batch.numRows == 0) {
            logInfo(s"Skip ColumnarBatch of ${batch.numRows} rows, ${batch.numCols} cols")
            Iterator.empty
          } else if (output.isEmpty || nonGlutenBatch) {
            // Fallback to ColumnarToRow of vanilla Spark.
            val localOutput = output
            numInputBatches += 1
            numOutputRows += batch.numRows()

            val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
            if (nonGlutenBatch) {
              batch.rowIterator().asScala.map(toUnsafe)
            } else {
              ArrowColumnarBatches
                .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
                .rowIterator().asScala.map(toUnsafe)
            }
          } else {
            val beforeConvert = System.currentTimeMillis()
            val offloaded =
              ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch)
            val batchHandle = GlutenColumnarBatches.getNativeHandle(offloaded)

            if (c2rId == -1) {
              c2rId = jniWrapper.nativeColumnarToRowInit(
                batchHandle,
                NativeMemoryAllocators.contextInstance().getNativeInstanceId)
            }
            val info = jniWrapper.nativeColumnarToRowWrite(batchHandle, c2rId)

            convertTime += (System.currentTimeMillis() - beforeConvert)

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
            }
          }
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
  }

  override def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions
}
