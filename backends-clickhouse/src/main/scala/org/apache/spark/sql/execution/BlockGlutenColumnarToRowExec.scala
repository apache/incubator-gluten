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

import io.glutenproject.execution.GlutenColumnarToRowExecBase
import io.glutenproject.vectorized.{BlockNativeConverter, CHNativeBlock}

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.concurrent.duration.NANOSECONDS

case class BlockGlutenColumnarToRowExec(child: SparkPlan)
  extends GlutenColumnarToRowExecBase(child = child) {
  override def nodeName: String = "CHNativeColumnarToRow"

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
        case d: DecimalType =>
        case d: StringType =>
        case d: BinaryType =>
        case d: DateType =>
        case d: TimestampType =>
        case d: ArrayType =>
        case d: StructType =>
        case d: MapType =>
        case d: NullType =>
        case _ =>
          throw new UnsupportedOperationException(
            s"${field.dataType} is not supported in GlutenColumnarToRowExecBase.")
      }
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    new CHColumnarToRowRDD(
      sparkContext,
      child.executeColumnar(),
      longMetric("numOutputRows"),
      longMetric("numInputBatches"),
      longMetric("convertTime"))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    // Hack because of type erasure
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]])
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def output: Seq[Attribute] = child.output

  protected def doProduce(ctx: CodegenContext): String = {
    throw new RuntimeException("Codegen is not supported!")
  }

  protected def withNewChildInternal(newChild: SparkPlan): BlockGlutenColumnarToRowExec =
    copy(child = newChild)
}

class CHColumnarToRowRDD(
    @transient sc: SparkContext,
    rdd: RDD[ColumnarBatch],
    numOutputRows: SQLMetric,
    numInputBatches: SQLMetric,
    convertTime: SQLMetric)
  extends RDD[InternalRow](sc, Seq(new OneToOneDependency(rdd))) {

  private val cleanedF = sc.clean(f)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    cleanedF(firstParent[ColumnarBatch].iterator(split, context))
  }

  private def f: Iterator[ColumnarBatch] => Iterator[InternalRow] = {
    batches =>
      val jniWrapper = new BlockNativeConverter()

      batches.flatMap {
        batch =>
          numInputBatches += 1
          numOutputRows += batch.numRows()

          if (batch.numRows == 0) {
            logInfo(s"Skip ColumnarBatch of ${batch.numRows} rows, ${batch.numCols} cols")
            Iterator.empty
          } else {
            val nativeBlock = CHNativeBlock.fromColumnarBatch(batch)
            val beforeConvert = System.nanoTime()
            val blockAddress = nativeBlock
              .orElseThrow(() => new IllegalStateException("Logic error"))
              .blockAddress()
            val info = jniWrapper.convertColumnarToRow(blockAddress)

            convertTime += NANOSECONDS.toMillis(System.nanoTime() - beforeConvert)

            new Iterator[InternalRow] {
              var rowId = 0
              val row = new UnsafeRow(batch.numCols())
              var closed = false

              override def hasNext: Boolean = {
                val result = rowId < batch.numRows()
                if (!result && !closed) {
                  jniWrapper.freeMemory(info.memoryAddress, info.totalSize)
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

  override def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions
}
