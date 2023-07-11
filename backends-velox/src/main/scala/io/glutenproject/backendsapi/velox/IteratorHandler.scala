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

package io.glutenproject.backendsapi.velox

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.glutenproject.GlutenNumaBindingInfo
import io.glutenproject.backendsapi.IteratorApi
import io.glutenproject.execution._
import io.glutenproject.metrics.IMetrics
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesBuilder
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.vectorized._

import org.apache.spark.{InterruptibleIterator, Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinityUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{ExecutorManager, TaskResources}

class IteratorHandler extends IteratorApi with Logging {

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
            paths.add(URLDecoder.decode(file.filePath, StandardCharsets.UTF_8.name()))
            starts.add(java.lang.Long.valueOf(file.start))
            lengths.add(java.lang.Long.valueOf(file.length))
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
                                     updateInputMetrics: (InputMetricsWrapper) => Unit,
                                     updateNativeMetrics: IMetrics => Unit,
                                     inputIterators: Seq[Iterator[ColumnarBatch]] = Seq())
  : Iterator[ColumnarBatch] = {
    val beforeBuild = System.nanoTime()
    val columnarNativeIterators =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ColumnarBatchInIterator(iter.asJava)
      }.asJava)
    val transKernel = new NativePlanEvaluator()
    val resIter: GeneralOutIterator = transKernel.createKernelWithBatchIterator(
      inputPartition.plan, columnarNativeIterators, outputAttributes.asJava)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    TaskResources.addRecycler(100) { resIter.close() }
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        val res = resIter.hasNext
        if (!res) {
          updateNativeMetrics(resIter.getMetrics)
          updateInputMetrics(inputMetrics)
        }
        res
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val cb = resIter.next()
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
                                     updateNativeMetrics: IMetrics => Unit,
                                     buildRelationBatchHolder: Seq[ColumnarBatch])
  : Iterator[ColumnarBatch] = {

    ExecutorManager.tryTaskSet(numaBindingInfo)

    val beforeBuild = System.nanoTime()

    val transKernel = new NativePlanEvaluator()
    val columnarNativeIterator =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ColumnarBatchInIterator(iter.asJava)
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
        cb
      }
    }

    TaskResources.addRecycler(100) {
      nativeResultIterator.close()
    }

    new CloseableColumnBatchIterator(resIter, Some(pipelineTime))
  }
  // scalastyle:on argcount

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

  /**
   * Compute for BroadcastBuildSideRDD
   */
  override def genBroadcastBuildSideIterator(split: Partition,
                                             context: TaskContext,
                                             broadcasted: Broadcast[BuildSideRelation],
                                             broadCastContext: BroadCastHashJoinContext
                                            ): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy(broadCastContext)
    new CloseableColumnBatchIterator(relation.deserialized)
  }
}
