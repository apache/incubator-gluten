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

package io.glutenproject.backendsapi.clickhouse

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.glutenproject.{GlutenConfig, GlutenNumaBindingInfo}
import io.glutenproject.backendsapi.IIteratorApi
import io.glutenproject.execution._
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.vectorized._

import org.apache.spark.{InterruptibleIterator, SparkConf, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class CHIteratorApi extends IIteratorApi with Logging {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genNativeFilePartition(
      index: Int,
      partitions: Seq[InputPartition],
      wsCxt: WholestageTransformContext): BaseNativeFilePartition = {
    val localFilesNodes = partitions.indices.map(i =>
      partitions(i) match {
        case p: NativeMergeTreePartition =>
          ExtensionTableBuilder
            .makeExtensionTable(p.minParts, p.maxParts, p.database, p.table, p.tablePath)
        case FilePartition(index, files) =>
          val paths = new java.util.ArrayList[String]()
          val starts = new java.util.ArrayList[java.lang.Long]()
          val lengths = new java.util.ArrayList[java.lang.Long]()
          files.foreach { f =>
            paths.add(f.filePath)
            starts.add(java.lang.Long.valueOf(f.start))
            lengths.add(java.lang.Long.valueOf(f.length))
          }
          LocalFilesBuilder.makeLocalFiles(
            index,
            paths,
            starts,
            lengths,
            wsCxt.substraitContext.getFileFormat.get(i))
      })
    wsCxt.substraitContext.initLocalFilesNodesIndex(0)
    wsCxt.substraitContext.setLocalFilesNodes(localFilesNodes)
    val substraitPlan = wsCxt.root.toProtobuf
    if (index < 3) {
      logDebug(s"The substrait plan for partition ${index}:\n${substraitPlan.toString}")
    }
    NativePartition(index, substraitPlan.toByteArray)
  }

  /**
   * Generate Iterator[ColumnarBatch] for CoalesceBatchesExec.
   *
   */
  override def genCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      recordsPerBatch: Int,
      numOutputRows: SQLMetric,
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      collectTime: SQLMetric,
      concatTime: SQLMetric,
      avgCoalescedNumRows: SQLMetric): Iterator[ColumnarBatch] = {
    val res = if (GlutenConfig.getConf.enableCoalesceBatches) {
      val operator = new CHCoalesceOperator(recordsPerBatch)
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val beforeNext = System.nanoTime
          val hasNext = iter.hasNext
          collectTime += System.nanoTime - beforeNext
          hasNext
        }

        override def next(): ColumnarBatch = {
          val c = iter.next()
          numInputBatches += 1
          val beforeConcat = System.nanoTime
          operator.mergeBlock(c)

          while (!operator.isFull && iter.hasNext) {
            val cb = iter.next();
            numInputBatches += 1;
            operator.mergeBlock(cb)
          }
          val res = operator.release().toColumnarBatch
          CHNativeBlock
            .fromColumnarBatch(res)
            .ifPresent(block => {
              numOutputRows += block.numRows();
              numOutputBatches += 1;
              concatTime += System.nanoTime() - beforeConcat
            })
          res
        }

        TaskContext.get().addTaskCompletionListener[Unit] { _ => operator.close() }
      }
    } else {
      iter
    }

    new CloseableCHColumnBatchIterator(res)
  }

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   *
   * @return
   */
  override def genFirstStageIterator(
      inputPartition: BaseNativeFilePartition,
      loadNative: Boolean,
      outputAttributes: Seq[Attribute],
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateMetrics: (Long, Long) => Unit,
      updateNativeMetrics: GeneralOutIterator => Unit,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()): Iterator[ColumnarBatch] = {
    var resIter: GeneralOutIterator = null
    if (loadNative) {
      val beforeBuild = System.nanoTime()
      val transKernel = new ExpressionEvaluator()
      val inBatchIters = new java.util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
      }.asJava)
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan,
        inBatchIters,
        outputAttributes.asJava)
      pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
      TaskContext.get().addTaskCompletionListener[Unit] { _ => resIter.close() }
    }
    val iter = new Iterator[Any] {

      override def hasNext: Boolean = {
        if (loadNative) {
          resIter.hasNext
        } else {
          false
        }
      }

      override def next(): Any = {
        val cb = resIter.next()
        updateMetrics(1, cb.numRows())
        cb
      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(
      context,
      new CloseableCHColumnBatchIterator(
        iter.asInstanceOf[Iterator[ColumnarBatch]],
        Some(pipelineTime)))
  }

  // Generate Iterator[ColumnarBatch] for final stage.
  // scalastyle:off argcount
  override def genFinalStageIterator(
      inputIterators: Seq[Iterator[ColumnarBatch]],
      numaBindingInfo: GlutenNumaBindingInfo,
      listJars: Seq[String],
      signature: String,
      sparkConf: SparkConf,
      outputAttributes: Seq[Attribute],
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateMetrics: (Long, Long) => Unit,
      updateNativeMetrics: GeneralOutIterator => Unit,
      buildRelationBatchHolder: Seq[ColumnarBatch],
      dependentKernels: Seq[ExpressionEvaluator],
      dependentKernelIterators: Seq[GeneralOutIterator]): Iterator[ColumnarBatch] = {
    // scalastyle:on argcount
    GlutenConfig.getConf
    val beforeBuild = System.nanoTime()
    val transKernel = new ExpressionEvaluator()
    val columnarNativeIterator =
      new java.util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
      }.asJava)
    // we need to complete dependency RDD's firstly
    val nativeIterator = transKernel.createKernelWithBatchIterator(
      rootNode,
      columnarNativeIterator,
      outputAttributes.asJava)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    val resIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        nativeIterator.hasNext
      }

      override def next(): ColumnarBatch = {
        val cb = nativeIterator.next()
        updateMetrics(1, cb.numRows())
        cb
      }
    }
    var closed = false

    def close = {
      closed = true
      buildRelationBatchHolder.foreach(_.close) // fixing: ref cnt goes nagative
      dependentKernels.foreach(_.close)
      dependentKernelIterators.foreach(_.close)
      nativeIterator.close()
      // relationHolder.clear()
    }

    TaskContext.get().addTaskCompletionListener[Unit] { _ => close }
    new CloseableCHColumnBatchIterator(resIter, Some(pipelineTime))
  }

  /**
   * Generate closeable ColumnBatch iterator.
   *
   * @param iter
   * @return
   */
  override def genCloseableColumnBatchIterator(
      iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    if (iter.isInstanceOf[CloseableCHColumnBatchIterator]) iter
    else new CloseableCHColumnBatchIterator(iter)
  }

  /**
   * Generate columnar native iterator.
   *
   * @return
   */
  override def genColumnarNativeIterator(
      delegated: Iterator[ColumnarBatch]): ColumnarNativeIterator = {
    new ColumnarNativeIterator(delegated.asJava)
  }

  /**
   * Generate BatchIterator for ExpressionEvaluator.
   *
   * @return
   */
  override def genBatchIterator(
      wsPlan: Array[Byte],
      iterList: Seq[GeneralInIterator],
      jniWrapper: ExpressionEvaluatorJniWrapper,
      outAttrs: Seq[Attribute]): GeneralOutIterator = {
    val batchIteratorInstance =
      jniWrapper.nativeCreateKernelWithIterator(0L, wsPlan, iterList.toArray)
    new BatchIterator(batchIteratorInstance, outAttrs.asJava)
  }

  /**
   * Generate Native FileScanRDD, currently only for ClickHouse Backend.
   */
  override def genNativeFileScanRDD(
      sparkContext: SparkContext,
      wsCxt: WholestageTransformContext,
      fileFormat: ReadFileFormat,
      inputPartitions: Seq[InputPartition],
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      scanTime: SQLMetric): RDD[ColumnarBatch] = {
    val startTime = System.nanoTime()
    // the file format for each scan exec
    wsCxt.substraitContext.setFileFormat(Seq(fileFormat).asJava)

    // generate each partition of all scan exec
    val substraitPlanPartition = inputPartitions.indices.map(i => {
      genNativeFilePartition(i, Seq(inputPartitions(i)), wsCxt)
    })
    logInfo(
      s"Generating the Substrait plan took: ${(System.nanoTime() - startTime) / 1000000} ms.")
    new NativeFileScanColumnarRDD(
      sparkContext,
      substraitPlanPartition,
      wsCxt.outputAttributes,
      numOutputRows,
      numOutputBatches,
      scanTime)
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
