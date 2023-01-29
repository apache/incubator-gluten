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

package io.glutenproject.backendsapi.glutendata

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import io.glutenproject.GlutenNumaBindingInfo
import io.glutenproject.backendsapi.IIteratorApi
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.execution._
import io.glutenproject.memory.{GlutenMemoryConsumer, TaskMemoryMetrics}
import io.glutenproject.memory.alloc._
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesBuilder
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.GlutenImplicitClass.{coalesce, ArrowColumnarBatchRetainer}
import io.glutenproject.vectorized._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.{InterruptibleIterator, SparkConf, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinityUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.ExecutorManager
import org.apache.spark.util.memory.TaskMemoryResources

import java.net.URLDecoder

abstract class GlutenIteratorApi extends IIteratorApi with Logging {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genFilePartition(index: Int,
                                partitions: Seq[InputPartition],
                                wsCxt: WholestageTransformContext
                               ): BaseGlutenPartition = {
    val localFilesNodesWithLocations = partitions.indices.map(i =>
      partitions(i) match {
        case f: FilePartition =>
          val paths = new java.util.ArrayList[String]()
          val starts = new java.util.ArrayList[java.lang.Long]()
          val lengths = new java.util.ArrayList[java.lang.Long]()
          val fileFormat = wsCxt.substraitContext.getFileFormat.get(0)
          f.files.foreach { file =>
            paths.add(URLDecoder.decode(file.filePath))
            starts.add(new java.lang.Long(file.start))
            lengths.add(new java.lang.Long(file.length))
          }
          (LocalFilesBuilder.makeLocalFiles(
            f.index, paths, starts, lengths, fileFormat),
            SoftAffinityUtil.getFilePartitionLocations(f))
      }
    )
    wsCxt.substraitContext.initLocalFilesNodesIndex(0)
    wsCxt.substraitContext.setLocalFilesNodes(localFilesNodesWithLocations.map(_._1))
    val substraitPlan = wsCxt.root.toProtobuf
    GlutenPartition(index, substraitPlan, localFilesNodesWithLocations.head._2)
  }

  /**
   * Generate Iterator[ColumnarBatch] for CoalesceBatchesExec.
   *
   * @param iter
   * @param recordsPerBatch
   * @param numOutputRows
   * @param numInputBatches
   * @param numOutputBatches
   * @param collectTime
   * @param concatTime
   * @param avgCoalescedNumRows
   * @return
   */
  override def genCoalesceIterator(iter: Iterator[ColumnarBatch],
                                   recordsPerBatch: Int,
                                   numOutputRows: SQLMetric = null,
                                   numInputBatches: SQLMetric = null,
                                   numOutputBatches: SQLMetric = null,
                                   collectTime: SQLMetric = null,
                                   concatTime: SQLMetric = null,
                                   avgCoalescedNumRows: SQLMetric = null)
  : Iterator[ColumnarBatch] = {

    val beforeInput = System.nanoTime
    val hasInput = iter.hasNext
    if (collectTime != null) {
      collectTime += System.nanoTime - beforeInput
    }
    val res = if (hasInput) {
      new Iterator[ColumnarBatch] {
        var numBatchesTotal: Long = _
        var numRowsTotal: Long = _
        TaskMemoryResources.addLeakSafeTaskCompletionListener[Unit] { _ =>
          if (avgCoalescedNumRows != null && numBatchesTotal > 0) {
            avgCoalescedNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
        }

        override def hasNext: Boolean = {
          val beforeNext = System.nanoTime
          val hasNext = iter.hasNext
          if (collectTime != null) {
            collectTime += System.nanoTime - beforeNext
          }
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
          val expectedOutputArrowFields = if (batchesToAppend.size > 0) {
            (0 until batchesToAppend(0).numCols).map(i => {
              ArrowColumnarBatches
                .ensureLoaded(
                  ArrowBufferAllocators.contextInstance(), batchesToAppend(0)).column(i)
                .asInstanceOf[ArrowWritableColumnVector]
                .getValueVector
                .getField
            })
          } else {
            Nil
          }

          val resultStructType =
            SparkArrowUtil.fromArrowSchema(new Schema(expectedOutputArrowFields.asJava))
          val beforeConcat = System.nanoTime
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(rowCount, resultStructType).toArray
          val target =
            new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), rowCount)
          coalesce(target, batchesToAppend.toList)
          target.setNumRows(rowCount)

          if (concatTime != null) {
            concatTime += System.nanoTime - beforeConcat
          }
          if (numOutputRows != null) {
            numOutputRows += rowCount
          }
          if (numInputBatches != null) {
            numInputBatches += batchesToAppend.length
          }
          if (numOutputBatches != null) {
            numOutputBatches += 1
          }
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
    new CloseableColumnBatchIterator(res)
  }

  /**
   * Generate closeable ColumnBatch iterator.
   *
   * @param iter
   * @return
   */
  override def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch])
  : Iterator[ColumnarBatch] = {
    new CloseableColumnBatchIterator(iter)
  }

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   *
   * @return
   */
  override def genFirstStageIterator(inputPartition: BaseGlutenPartition,
                                     outputAttributes: Seq[Attribute],
                                     context: TaskContext,
                                     pipelineTime: SQLMetric,
                                     updateOutputMetrics: (Long, Long) => Unit,
                                     updateNativeMetrics: Metrics => Unit,
                                     inputIterators: Seq[Iterator[ColumnarBatch]] = Seq())
  : Iterator[ColumnarBatch] = {
    val beforeBuild = System.nanoTime()
    val columnarNativeIterators =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ArrowInIterator(iter.asJava)
      }.asJava)
    val transKernel = new GlutenNativeExpressionEvaluator()
    val resIter: GeneralOutIterator = transKernel.createKernelWithBatchIterator(
      inputPartition.plan, columnarNativeIterators, outputAttributes.asJava)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    TaskMemoryResources.addLeakSafeTaskCompletionListener[Unit] { _ => resIter.close() }
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        val res = resIter.hasNext
        if (!res) {
          updateNativeMetrics(resIter.getMetrics)
        }
        res
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val cb = resIter.next()
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
        updateOutputMetrics(1, cb.numRows())
        cb
      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(
      context,
      new CloseableColumnBatchIterator(
        iter.asInstanceOf[Iterator[ColumnarBatch]], Some(pipelineTime)))
  }

  // scalastyle:off argcount

  /**
   * Generate Iterator[ColumnarBatch] for final stage.
   *
   * @return
   */
  override def genFinalStageIterator(inputIterators: Seq[Iterator[ColumnarBatch]],
                                     numaBindingInfo: GlutenNumaBindingInfo,
                                     sparkConf: SparkConf,
                                     outputAttributes: Seq[Attribute],
                                     rootNode: PlanNode,
                                     pipelineTime: SQLMetric,
                                     updateOutputMetrics: (Long, Long) => Unit,
                                     updateNativeMetrics: Metrics => Unit,
                                     buildRelationBatchHolder: Seq[ColumnarBatch])
  : Iterator[ColumnarBatch] = {

    ExecutorManager.tryTaskSet(numaBindingInfo)

    val beforeBuild = System.nanoTime()

    val transKernel = new GlutenNativeExpressionEvaluator()
    val columnarNativeIterator =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ArrowInIterator(iter.asJava)
      }.asJava)
    val nativeResultIterator =
      transKernel.createKernelWithBatchIterator(rootNode.toProtobuf, columnarNativeIterator,
        outputAttributes.asJava)

    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

    val resIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        val res = nativeResultIterator.hasNext
        if (!res) {
          updateNativeMetrics(nativeResultIterator.getMetrics)
        }
        res
      }

      override def next(): ColumnarBatch = {
        val cb = nativeResultIterator.next
        updateOutputMetrics(1, cb.numRows())
        cb
      }
    }

    TaskMemoryResources.addLeakSafeTaskCompletionListener[Unit](_ => {
      nativeResultIterator.close()
    })

    new CloseableColumnBatchIterator(resIter, Some(pipelineTime))
  }
  // scalastyle:on argcount

  /**
   * Generate NativeMemoryAllocatorManager.
   *
   * @return
   */
  override def genNativeMemoryAllocatorManager(taskMemoryManager: TaskMemoryManager,
                                               spiller: Spiller,
                                               taskMemoryMetrics: TaskMemoryMetrics
                                              ): NativeMemoryAllocatorManager = {
    val rl = new GlutenManagedReservationListener(
      new GlutenMemoryConsumer(taskMemoryManager, spiller),
      taskMemoryMetrics
    )
    new GlutenMemoryAllocatorManager(NativeMemoryAllocator.createListenable(rl))
  }

  /**
   * Generate Native FileScanRDD, currently only for ClickHouse Backend.
   */
  override def genNativeFileScanRDD(sparkContext: SparkContext,
                                    wsCxt: WholestageTransformContext,
                                    fileFormat: ReadFileFormat,
                                    inputPartitions: Seq[InputPartition],
                                    numOutputRows: SQLMetric,
                                    numOutputBatches: SQLMetric,
                                    scanTime: SQLMetric): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(
      "Cannot support to generate Native FileScanRDD.")
  }
}
