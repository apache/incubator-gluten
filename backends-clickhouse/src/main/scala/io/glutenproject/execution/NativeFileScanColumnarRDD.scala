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

import scala.collection.JavaConverters._

import io.glutenproject.GlutenConfig
import io.glutenproject.vectorized.{CloseableCHColumnBatchIterator, ExpressionEvaluator, GeneralInIterator, GeneralOutIterator}

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class NativeFileScanColumnarRDD(sc: SparkContext,
                                @transient private val inputPartitions: Seq[InputPartition],
                                outputAttributes: Seq[Attribute],
                                numOutputRows: SQLMetric,
                                numOutputBatches: SQLMetric,
                                scanTime: SQLMetric)
  extends RDD[ColumnarBatch](sc, Nil) {

  val loadNative: Boolean = GlutenConfig.getConf.loadNative

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val inputPartition = castNativePartition(split)

    var resIter: GeneralOutIterator = null
    if (loadNative) {
      val startNs = System.nanoTime()
      val transKernel = new ExpressionEvaluator()
      val inBatchIters = new java.util.ArrayList[GeneralInIterator]()
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan, inBatchIters, outputAttributes.asJava)
      scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
      TaskContext.get().addTaskCompletionListener[Unit] { _ => resIter.close() }
    }
    val iter = new Iterator[Any] {
      var scanTotalTime = 0L
      var scanTimeAdded = false
      override def hasNext: Boolean = {
        if (loadNative) {
          val startNs = System.nanoTime()
          val res = resIter.hasNext
          scanTotalTime += System.nanoTime() - startNs
          if (!res && !scanTimeAdded) {
            scanTime += NANOSECONDS.toMillis(scanTotalTime)
            scanTimeAdded = true
          }
          res
        } else {
          false
        }
      }

      override def next(): Any = {
        val startNs = System.nanoTime()
        val cb = resIter.next()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        scanTotalTime += System.nanoTime() - startNs
        cb
      }
    }
    new CloseableCHColumnBatchIterator(iter.asInstanceOf[Iterator[ColumnarBatch]])
  }

  private def castNativePartition(split: Partition): BaseNativeFilePartition = split match {
    case FirstZippedPartitionsPartition(_, p: BaseNativeFilePartition, _) => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

  private def castPartition(split: Partition): FirstZippedPartitionsPartition = split match {
    case p: FirstZippedPartitionsPartition => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => FirstZippedPartitionsPartition(index, inputPartition)
    }.toArray
  }
}
