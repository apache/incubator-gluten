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
import io.glutenproject.metrics.{GlutenTimeMetric, IMetrics, NativeMetrics}
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.{LogLevelUtil, SubstraitPlanPrinterUtil}
import io.glutenproject.vectorized.{CHNativeExpressionEvaluator, CloseableCHColumnBatchIterator, GeneralInIterator, GeneralOutIterator}

import org.apache.spark.{InterruptibleIterator, Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinityUtil
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.lang.{Long => JLong}
import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

class CHIteratorApi extends IteratorApi with Logging with LogLevelUtil {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genFilePartition(
      index: Int,
      partitions: Seq[InputPartition],
      partitionSchemas: Seq[StructType],
      fileFormats: Seq[ReadFileFormat],
      wsCxt: WholeStageTransformContext): BaseGlutenPartition = {
    val localFilesNodesWithLocations = partitions.indices.map(
      i =>
        partitions(i) match {
          case p: GlutenMergeTreePartition =>
            (
              ExtensionTableBuilder
                .makeExtensionTable(p.minParts, p.maxParts, p.database, p.table, p.tablePath),
              SoftAffinityUtil.getNativeMergeTreePartitionLocations(p))
          case f: FilePartition =>
            val paths = new util.ArrayList[String]()
            val starts = new util.ArrayList[JLong]()
            val lengths = new util.ArrayList[JLong]()
            val partitionColumns = mutable.ArrayBuffer.empty[Map[String, String]]
            f.files.foreach {
              file =>
                paths.add(new URI(file.filePath).toASCIIString)
                starts.add(JLong.valueOf(file.start))
                lengths.add(JLong.valueOf(file.length))
                // TODO: Support custom partition location
                val partitionColumn = mutable.Map.empty[String, String]
                partitionColumns.append(partitionColumn.toMap)
            }
            (
              LocalFilesBuilder.makeLocalFiles(
                f.index,
                paths,
                starts,
                lengths,
                partitionColumns.map(_.asJava).asJava,
                fileFormats(i)),
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

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   *
   * @return
   */
  override def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: InputMetricsWrapper => Unit,
      updateNativeMetrics: IMetrics => Unit,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()
  ): Iterator[ColumnarBatch] = {
    val resIter: GeneralOutIterator = GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        val transKernel = new CHNativeExpressionEvaluator()
        val inBatchIters = new util.ArrayList[GeneralInIterator](inputIterators.map {
          iter => new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
        }.asJava)
        transKernel.createKernelWithBatchIterator(inputPartition.plan, inBatchIters, false)
    }
    TaskContext.get().addTaskCompletionListener[Unit](_ => resIter.close())
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
      private var outputRowCount = 0L
      private var outputVectorCount = 0L

      override def hasNext: Boolean = {
        val res = resIter.hasNext
        if (!res) {
          val nativeMetrics = resIter.getMetrics.asInstanceOf[NativeMetrics]
          nativeMetrics.setFinalOutputMetrics(outputRowCount, outputVectorCount)
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
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      buildRelationBatchHolder: Seq[ColumnarBatch],
      materializeInput: Boolean): Iterator[ColumnarBatch] = {
    // scalastyle:on argcount
    GlutenConfig.getConf
    val nativeIterator = GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        val transKernel = new CHNativeExpressionEvaluator()
        val columnarNativeIterator =
          new java.util.ArrayList[GeneralInIterator](inputIterators.map {
            iter => new ColumnarNativeIterator(genCloseableColumnBatchIterator(iter).asJava)
          }.asJava)
        // we need to complete dependency RDD's firstly
        transKernel.createKernelWithBatchIterator(
          rootNode.toProtobuf,
          columnarNativeIterator,
          materializeInput)
    }

    val resIter = new Iterator[ColumnarBatch] {
      private var outputRowCount = 0L
      private var outputVectorCount = 0L

      override def hasNext: Boolean = {
        val res = nativeIterator.hasNext
        if (!res) {
          val nativeMetrics = nativeIterator.getMetrics.asInstanceOf[NativeMetrics]
          nativeMetrics.setFinalOutputMetrics(outputRowCount, outputVectorCount)
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

    def close(): Unit = {
      closed = true
      buildRelationBatchHolder.foreach(_.close) // fixing: ref cnt goes nagative
      nativeIterator.close()
      // relationHolder.clear()
    }

    TaskContext.get().addTaskCompletionListener[Unit](_ => close())
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

  /** Generate Native FileScanRDD, currently only for ClickHouse Backend. */
  override def genNativeFileScanRDD(
      sparkContext: SparkContext,
      wsCxt: WholeStageTransformContext,
      fileFormat: ReadFileFormat,
      inputPartitions: Seq[InputPartition],
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      scanTime: SQLMetric): RDD[ColumnarBatch] = {
    val substraitPlanPartition = GlutenTimeMetric.withMillisTime {
      // generate each partition of all scan exec
      inputPartitions.indices.map(
        i => {
          genFilePartition(i, Seq(inputPartitions(i)), null, Seq(fileFormat), wsCxt)
        })
    }(t => logInfo(s"Generating the Substrait plan took: $t ms."))

    new NativeFileScanColumnarRDD(
      sparkContext,
      substraitPlanPartition,
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
