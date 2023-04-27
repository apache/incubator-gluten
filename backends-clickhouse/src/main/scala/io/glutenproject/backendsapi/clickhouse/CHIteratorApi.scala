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

import io.glutenproject.{GlutenConfig, GlutenNumaBindingInfo}
import io.glutenproject.backendsapi.IteratorApi
import io.glutenproject.execution._
import io.glutenproject.memory.{GlutenMemoryConsumer, TaskMemoryMetrics}
import io.glutenproject.memory.alloc._
import io.glutenproject.metrics.{IMetrics, NativeMetrics}
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.{LogLevelUtil, SubstraitPlanPrinterUtil}
import io.glutenproject.vectorized._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinityUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

class CHIteratorApi extends IteratorApi with Logging with LogLevelUtil {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genFilePartition(
      index: Int,
      partitions: Seq[InputPartition],
      wsCxt: WholestageTransformContext): BaseGlutenPartition = {
    val localFilesNodesWithLocations = partitions.indices.map(
      i =>
        partitions(i) match {
          case p: GlutenMergeTreePartition =>
            (
              ExtensionTableBuilder
                .makeExtensionTable(p.minParts, p.maxParts, p.database, p.table, p.tablePath),
              SoftAffinityUtil.getNativeMergeTreePartitionLocations(p))
          case f: FilePartition =>
            val paths = new java.util.ArrayList[String]()
            val starts = new java.util.ArrayList[java.lang.Long]()
            val lengths = new java.util.ArrayList[java.lang.Long]()
            f.files.foreach {
              file =>
                paths.add(file.filePath)
                starts.add(java.lang.Long.valueOf(file.start))
                lengths.add(java.lang.Long.valueOf(file.length))
            }
            (
              LocalFilesBuilder.makeLocalFiles(
                f.index,
                paths,
                starts,
                lengths,
                wsCxt.substraitContext.getFileFormat.get(i)),
              SoftAffinityUtil.getFilePartitionLocations(f))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupport operators.")
        })
    wsCxt.substraitContext.initLocalFilesNodesIndex(0)
    wsCxt.substraitContext.setLocalFilesNodes(localFilesNodesWithLocations.map(_._1))
    val substraitPlan = wsCxt.root.toProtobuf
    if (index < 3) {
      logOnLevel(
        GlutenConfig.getConf.substraitPlanLogLevel,
        s"The substrait plan for partition $index:\n${SubstraitPlanPrinterUtil
            .substraitPlanToJson(substraitPlan)}"
      )
    }
    GlutenPartition(index, substraitPlan, localFilesNodesWithLocations.head._2)
  }

  /** Generate Iterator[ColumnarBatch] for CoalesceBatchesExec. */
  override def genCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      recordsPerBatch: Int,
      numOutputRows: SQLMetric = null,
      numInputBatches: SQLMetric = null,
      numOutputBatches: SQLMetric = null,
      collectTime: SQLMetric = null,
      concatTime: SQLMetric = null,
      avgCoalescedNumRows: SQLMetric = null): Iterator[ColumnarBatch] = {
    val res = if (GlutenConfig.getConf.enableCoalesceBatches) {
      val operator = new CHCoalesceOperator(recordsPerBatch)
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val beforeNext = System.nanoTime
          val hasNext = iter.hasNext
          collectTime += System.nanoTime - beforeNext
          if (!hasNext) operator.close();
          hasNext
        }

        override def next(): ColumnarBatch = {
          val c = iter.next()
          numInputBatches += 1
          val beforeConcat = System.nanoTime
          operator.mergeBlock(c)

          concatTime += System.nanoTime() - beforeConcat
          var hasNext = true;
          while (!operator.isFull && hasNext) {
            val beforeNext = System.nanoTime
            hasNext = iter.hasNext
            if (hasNext) {
              val cb = iter.next();
              collectTime += System.nanoTime - beforeNext
              numInputBatches += 1;
              val beforeConcat = System.nanoTime
              operator.mergeBlock(cb)
              concatTime += System.nanoTime() - beforeConcat
            }
          }
          val res = operator.release().toColumnarBatch
          CHNativeBlock
            .fromColumnarBatch(res)
            .ifPresent(
              block => {
                numOutputRows += block.numRows();
                numOutputBatches += 1;
              })
          res
        }

        TaskContext.get().addTaskCompletionListener[Unit](_ => operator.close())
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
      inputPartition: BaseGlutenPartition,
      outputAttributes: Seq[Attribute],
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: (InputMetricsWrapper) => Unit,
      updateNativeMetrics: IMetrics => Unit,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()): Iterator[ColumnarBatch] = {
    val beforeBuild = System.nanoTime()
    val transKernel = new CHNativeExpressionEvaluator()
    val inBatchIters = new java.util.ArrayList[GeneralInIterator](inputIterators.map {
      iter => new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
    }.asJava)
    val resIter: GeneralOutIterator = transKernel.createKernelWithBatchIterator(
      inputPartition.plan,
      inBatchIters,
      outputAttributes.asJava)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    TaskContext.get().addTaskCompletionListener[Unit](_ => resIter.close())
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
      private var outputRowCount = 0L
      private var outputVectorCount = 0L

      override def hasNext: Boolean = {
        val res = resIter.hasNext
        if (!res) {
          // TODO: support collecting metrics from ch backend
          // val nativeMetrics = resIter.getMetrics.asInstanceOf[NativeMetrics]
          val nativeMetrics = new NativeMetrics(new java.util.HashMap())
          nativeMetrics.metrics.put("outputRows", outputRowCount)
          nativeMetrics.metrics.put("outputVectors", outputVectorCount)
          updateNativeMetrics(nativeMetrics)
          updateInputMetrics(inputMetrics)
        }
        res
      }

      override def next(): Any = {
        val cb = resIter.next()
        outputVectorCount += 1
        outputRowCount += cb.numRows()
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
      sparkConf: SparkConf,
      outputAttributes: Seq[Attribute],
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      buildRelationBatchHolder: Seq[ColumnarBatch]): Iterator[ColumnarBatch] = {
    // scalastyle:on argcount
    GlutenConfig.getConf
    val beforeBuild = System.nanoTime()
    val transKernel = new CHNativeExpressionEvaluator()
    val columnarNativeIterator =
      new java.util.ArrayList[GeneralInIterator](inputIterators.map {
        iter => new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
      }.asJava)
    // we need to complete dependency RDD's firstly
    val nativeIterator = transKernel.createKernelWithBatchIterator(
      rootNode.toProtobuf,
      columnarNativeIterator,
      outputAttributes.asJava)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    val resIter = new Iterator[ColumnarBatch] {
      private var outputRowCount = 0L
      private var outputVectorCount = 0L

      override def hasNext: Boolean = {
        val res = nativeIterator.hasNext
        if (!res) {
          // TODO: support collecting metrics from ch backend
          // val nativeMetrics = nativeIterator.getMetrics.asInstanceOf[NativeMetrics]
          val nativeMetrics = new NativeMetrics(new java.util.HashMap())
          nativeMetrics.metrics.put("outputRows", outputRowCount)
          nativeMetrics.metrics.put("outputVectors", outputVectorCount)
          updateNativeMetrics(nativeMetrics)
        }
        res
      }

      override def next(): ColumnarBatch = {
        val cb = nativeIterator.next()
        outputVectorCount += 1
        outputRowCount += cb.numRows()
        cb
      }
    }
    var closed = false

    def close = {
      closed = true
      buildRelationBatchHolder.foreach(_.close) // fixing: ref cnt goes nagative
      nativeIterator.close()
      // relationHolder.clear()
    }

    TaskContext.get().addTaskCompletionListener[Unit](_ => close)
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
   * Generate NativeMemoryAllocatorManager.
   *
   * @return
   */
  override def genNativeMemoryAllocatorManager(
      taskMemoryManager: TaskMemoryManager,
      spiller: Spiller,
      taskMemoryMetrics: TaskMemoryMetrics): NativeMemoryAllocatorManager = {
    val rl = new CHManagedReservationListener(
      new GlutenMemoryConsumer(taskMemoryManager, spiller),
      taskMemoryMetrics
    )
    new CHMemoryAllocatorManager(NativeMemoryAllocator.createListenable(rl))
  }

  /** Generate Native FileScanRDD, currently only for ClickHouse Backend. */
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
    val substraitPlanPartition = inputPartitions.indices.map(
      i => {
        genFilePartition(i, Seq(inputPartitions(i)), wsCxt)
      })
    logInfo(s"Generating the Substrait plan took: ${(System.nanoTime() - startTime)} ns.")
    new NativeFileScanColumnarRDD(
      sparkContext,
      substraitPlanPartition,
      wsCxt.outputAttributes,
      numOutputRows,
      numOutputBatches,
      scanTime)
  }

  /** Compute for BroadcastBuildSideRDD */
  override def genBroadcastBuildSideIterator(
      split: Partition,
      context: TaskContext,
      broadcasted: Broadcast[BuildSideRelation],
      broadCastContext: BroadCastHashJoinContext): Iterator[ColumnarBatch] = {
    CHBroadcastBuildSideCache.getOrBuildBroadcastHashTable(broadcasted, broadCastContext)
    genCloseableColumnBatchIterator(Iterator.empty)
  }
}
