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

import io.glutenproject.vectorized.ColumnarFactory

import java.io.Serializable
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import io.glutenproject.GazelleJniConfig
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.vectorized._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.util.OASPackageBridge._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util._

trait BaseNativeFilePartition extends Partition with InputPartition {
  def substraitPlan: Array[Byte]
}

case class NativeMergeTreePartition(index: Int, engine: String,
                                    database: String,
                                    table: String, tablePath: String,
                                    minParts: Long, maxParts: Long,
                                    substraitPlan: Array[Byte] = Array.empty[Byte])
  extends BaseNativeFilePartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }

  def copySubstraitPlan(newSubstraitPlan: Array[Byte]): NativeMergeTreePartition = {
    this.copy(substraitPlan = newSubstraitPlan)
  }
}

case class NativeFilePartition(index: Int, files: Array[PartitionedFile],
                               substraitPlan: Array[Byte])
  extends BaseNativeFilePartition {
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
    columnarReads: Boolean,
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

  private def castNativePartition(split: Partition): BaseNativeFilePartition = split match {
    case NativeSubstraitPartition(_, p: NativeFilePartition) =>
      p.asInstanceOf[BaseNativeFilePartition]
    case NativeSubstraitPartition(_, p: NativeMergeTreePartition) =>
      p.asInstanceOf[BaseNativeFilePartition]
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
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ => resIter.close() }
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

      def nextArrowColumnarBatch(): Any = {
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

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        if (!GazelleJniConfig.getConf.loadch) {
          nextArrowColumnarBatch()
        } else {
          resIter.chNext()
        }

      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context,
      ColumnarFactory.createClosableIterator(iter.asInstanceOf[Iterator[ColumnarBatch]]))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

}
