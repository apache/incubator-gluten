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

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import io.glutenproject.GazelleJniConfig
import io.glutenproject.vectorized.{ArrowWritableColumnVector, CHCoalesceOperator, CHNativeBlock, ColumnarFactory}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.VectorBatchAppender
import org.apache.spark.TaskContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

case class CoalesceBatchesExec(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "CoalesceBatches"

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_collectbatch"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_coalescebatch"),
    "avgCoalescedNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg coalesced batch num rows"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (GazelleJniConfig.getConf.isClickHouseBackend) {
      doCHInternalExecuteColumnar()
    } else {
      doInternalExecuteColumnar()
    }
  }

  def doInternalExecuteColumnar(): RDD[ColumnarBatch] = {
    import CoalesceBatchesExec._

    val recordsPerBatch = conf.arrowMaxRecordsPerBatch
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val avgCoalescedNumRows = longMetric("avgCoalescedNumRows")

    child.executeColumnar().mapPartitions { iter =>
      val beforeInput = System.nanoTime
      val hasInput = iter.hasNext
      collectTime += System.nanoTime - beforeInput
      val res = if (hasInput) {
        new Iterator[ColumnarBatch] {
          var numBatchesTotal: Long = _
          var numRowsTotal: Long = _
          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ =>
            if (numBatchesTotal > 0) {
              avgCoalescedNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
            }
          }

          override def hasNext: Boolean = {
            val beforeNext = System.nanoTime
            val hasNext = iter.hasNext
            collectTime += System.nanoTime - beforeNext
            hasNext
          }

          override def next(): ColumnarBatch = {

            if (!hasNext) {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }

            var rowCount = 0
            val batchesToAppend = ListBuffer[ColumnarBatch]()

            while (hasNext && rowCount < recordsPerBatch) {
              val delta = iter.next()
              delta.retain()
              rowCount += delta.numRows
              batchesToAppend += delta
            }

            // chendi: We need make sure target FieldTypes are exactly the same as src
            val expected_output_arrow_fields = if (batchesToAppend.size > 0) {
              (0 until batchesToAppend(0).numCols).map(i => {
                batchesToAppend(0).column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector.getField
              })
            } else {
              Nil
            }

            val resultStructType = ArrowUtils.fromArrowSchema(new Schema(expected_output_arrow_fields.asJava))
            val beforeConcat = System.nanoTime
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(rowCount, resultStructType).toArray
            val target =
              new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), rowCount)
            coalesce(target, batchesToAppend.toList)
            target.setNumRows(rowCount)

            concatTime += System.nanoTime - beforeConcat
            numOutputRows += rowCount
            numInputBatches += batchesToAppend.length
            numOutputBatches += 1

            // used for calculating avgCoalescedNumRows
            numRowsTotal += rowCount
            numBatchesTotal += 1

            batchesToAppend.foreach(cb => cb.close())

            target
          }
        }
      } else {
        Iterator.empty
      }
      ColumnarFactory.createClosableIterator(res)
    }
  }

  def doCHInternalExecuteColumnar(): RDD[ColumnarBatch] = {
    val recordsPerBatch = conf.arrowMaxRecordsPerBatch
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val avgCoalescedNumRows = longMetric("avgCoalescedNumRows")
    child.executeColumnar().mapPartitions(iter => {
      val operator = new CHCoalesceOperator(recordsPerBatch)
      val res = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val beforeNext = System.nanoTime
          val hasNext = iter.hasNext
          collectTime += System.nanoTime - beforeNext
          hasNext
        }
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((tc: TaskContext) => {
          operator.close()
        })
        override def next(): ColumnarBatch = {
          val c = iter.next()
          numInputBatches += 1
          val beforeConcat = System.nanoTime
          operator.mergeBlock(c)

          while(!operator.isFull && iter.hasNext) {
            val cb = iter.next();
            numInputBatches += 1;
            operator.mergeBlock(cb)
          }
          val res = operator.release().toColumnarBatch
          CHNativeBlock.fromColumnarBatch(res).ifPresent(block => {
            numOutputRows += block.numRows();
            numOutputBatches += 1;
          })
          res
        }
      }

      ColumnarFactory.createClosableIterator(res)
    })
  }
}

object CoalesceBatchesExec {
  implicit class ArrowColumnarBatchRetainer(val cb: ColumnarBatch) {
    def retain(): Unit = {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    }
  }

  def coalesce(targetBatch: ColumnarBatch, batchesToAppend: List[ColumnarBatch]): Unit = {
    (0 until targetBatch.numCols).toList.foreach { i =>
      val targetVector =
        targetBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      val vectorsToAppend = batchesToAppend.map { cb =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      }
      VectorBatchAppender.batchAppend(targetVector, vectorsToAppend: _*)
    }
  }
}
