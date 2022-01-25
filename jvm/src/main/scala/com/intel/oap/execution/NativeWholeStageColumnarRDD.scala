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

package com.intel.oap.execution

import java.io.Serializable

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.intel.oap.GazelleJniConfig
import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.util.OASPackageBridge._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util._

case class NativeFilePartition(index: Int, files: Array[PartitionedFile],
                               val substraitPlan: Array[Byte])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

case class NativeSubstraitPartition(val index: Int, val inputPartition: InputPartition)
  extends Partition with Serializable

class NativeWholeStageColumnarRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    jarList: Seq[String],
    dependentKernelIterators: ListBuffer[BatchIterator])
    extends RDD[ColumnarBatch](sc, Nil) {
  val numaBindingInfo = GazelleJniConfig.getConf.numaBindingInfo
  val loadNative: Boolean = GazelleJniConfig.getConf.loadNative

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new NativeSubstraitPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): NativeSubstraitPartition = split match {
    case p: NativeSubstraitPartition => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  private def castNativePartition(split: Partition): NativeFilePartition = split match {
    case NativeSubstraitPartition(_, p: NativeFilePartition) => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)

    val inputPartition = castNativePartition(split)

    var inputSchema : Schema = null
    var outputSchema : Schema = null
    var resIter : BatchIterator = null
    if (loadNative) {
      // TODO: 'jarList' is kept for codegen
      val transKernel = new ExpressionEvaluator(jarList.toList.asJava)
      val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator]()
      outputSchema = ConverterUtils.toArrowSchema(outputAttributes)
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan, inBatchIters)
    }
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        if (loadNative) {
          resIter.hasNext
        } else {
          false
        }
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val rb = resIter.next()
        if (rb == null) {
          val resultStructType = ArrowUtils.fromArrowSchema(outputSchema)
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
        val outputNumRows = rb.getLength
        val output = ConverterUtils.fromArrowRecordBatch(outputSchema, rb)
        ConverterUtils.releaseArrowRecordBatch(rb)
        val cb = new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]), outputNumRows)
        val bytes: Long = cb match {
          case batch: ColumnarBatch =>
            (0 until batch.numCols()).map { i =>
              val vector = Option(batch.column(i))
              vector.map {
                case av: ArrowWritableColumnVector =>
                  av.getValueVector.getBufferSize.toLong
                case _ => 0L
              }.sum
            }.sum
          case _ => 0L
        }
        inputMetrics.bridgeIncBytesRead(bytes)
        cb
      }
    }
    val closeableColumnarBatchIterator = new CloseableColumnBatchIterator(
      iter.asInstanceOf[Iterator[ColumnarBatch]])
    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, closeableColumnarBatchIterator)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

}
