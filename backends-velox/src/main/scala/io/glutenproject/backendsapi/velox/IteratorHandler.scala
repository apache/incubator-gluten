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
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BinaryType, DateType, StructType, TimestampType}
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{ExecutorManager, TaskResources}

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

class IteratorHandler extends IteratorApi with Logging {

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

    def constructSplitInfo(schema: StructType, files: Array[PartitionedFile]) = {
      val paths = mutable.ArrayBuffer.empty[String]
      val starts = mutable.ArrayBuffer.empty[java.lang.Long]
      val lengths = mutable.ArrayBuffer.empty[java.lang.Long]
      val partitionColumns = mutable.ArrayBuffer.empty[Map[String, String]]
      files.foreach {
        file =>
          paths.append(URLDecoder.decode(file.filePath, StandardCharsets.UTF_8.name()))
          starts.append(java.lang.Long.valueOf(file.start))
          lengths.append(java.lang.Long.valueOf(file.length))

          val partitionColumn = mutable.Map.empty[String, String]
          for (i <- 0 until file.partitionValues.numFields) {
            val partitionColumnValue = if (file.partitionValues.isNullAt(i)) {
              ExternalCatalogUtils.DEFAULT_PARTITION_NAME
            } else {
              val pn = file.partitionValues.get(i, schema.fields(i).dataType)
              schema.fields(i).dataType match {
                case _: BinaryType =>
                  new String(pn.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8)
                case _: DateType =>
                  DateFormatter.apply().format(pn.asInstanceOf[Integer])
                case _: TimestampType =>
                  TimestampFormatter
                    .getFractionFormatter(ZoneOffset.UTC)
                    .format(pn.asInstanceOf[java.lang.Long])
                case _ => pn.toString
              }
            }
            partitionColumn.put(schema.names(i), partitionColumnValue)
          }
          partitionColumns.append(partitionColumn.toMap)
      }
      (paths, starts, lengths, partitionColumns)
    }

    val localFilesNodesWithLocations = partitions.indices.map(
      i =>
        partitions(i) match {
          case f: FilePartition =>
            val fileFormat = fileFormats(i)
            val partitionSchema = partitionSchemas(i)
            val (paths, starts, lengths, partitionColumns) =
              constructSplitInfo(partitionSchema, f.files)
            (
              LocalFilesBuilder.makeLocalFiles(
                f.index,
                paths.asJava,
                starts.asJava,
                lengths.asJava,
                partitionColumns.map(_.asJava).asJava,
                fileFormat),
              SoftAffinityUtil.getFilePartitionLocations(f))
        })
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
  override def genCloseableColumnBatchIterator(
      iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new CloseableColumnBatchIterator(iter)
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
      updateInputMetrics: (InputMetricsWrapper) => Unit,
      updateNativeMetrics: IMetrics => Unit,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()): Iterator[ColumnarBatch] = {
    val beforeBuild = System.nanoTime()
    val columnarNativeIterators =
      new util.ArrayList[GeneralInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(iter.asJava)
      }.asJava)
    val transKernel = new NativePlanEvaluator()
    val resIter: GeneralOutIterator =
      transKernel.createKernelWithBatchIterator(inputPartition.plan, columnarNativeIterators)
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
    TaskResources.addRecycler(s"FirstStageIterator_${resIter.getId}", 100)(resIter.close())
    val iter = new Iterator[ColumnarBatch] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        val res = resIter.hasNext
        if (!res) {
          updateNativeMetrics(resIter.getMetrics)
          updateInputMetrics(inputMetrics)
        }
        res
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        resIter.next()
      }
    }

    new InterruptibleIterator(context, new CloseableColumnBatchIterator(iter, Some(pipelineTime)))
  }

  // scalastyle:off argcount

  /**
   * Generate Iterator[ColumnarBatch] for final stage.
   *
   * @return
   */
  override def genFinalStageIterator(
      inputIterators: Seq[Iterator[ColumnarBatch]],
      numaBindingInfo: GlutenNumaBindingInfo,
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      buildRelationBatchHolder: Seq[ColumnarBatch],
      materializeInput: Boolean): Iterator[ColumnarBatch] = {

    ExecutorManager.tryTaskSet(numaBindingInfo)

    val beforeBuild = System.nanoTime()

    val transKernel = new NativePlanEvaluator()
    val columnarNativeIterator =
      new util.ArrayList[GeneralInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(iter.asJava)
      }.asJava)
    val nativeResultIterator =
      transKernel.createKernelWithBatchIterator(rootNode.toProtobuf, columnarNativeIterator)

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
        nativeResultIterator.next
      }
    }

    TaskResources.addRecycler(s"FinalStageIterator_${nativeResultIterator.getId}", 100) {
      nativeResultIterator.close()
    }

    new CloseableColumnBatchIterator(resIter, Some(pipelineTime))
  }
  // scalastyle:on argcount

  /** Generate Native FileScanRDD, currently only for ClickHouse Backend. */
  override def genNativeFileScanRDD(
      sparkContext: SparkContext,
      wsCxt: WholeStageTransformContext,
      fileFormat: ReadFileFormat,
      inputPartitions: Seq[InputPartition],
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      scanTime: SQLMetric): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot support to generate Native FileScanRDD.")
  }

  /** Compute for BroadcastBuildSideRDD */
  override def genBroadcastBuildSideIterator(
      split: Partition,
      context: TaskContext,
      broadcasted: Broadcast[BuildSideRelation],
      broadCastContext: BroadCastHashJoinContext): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy(broadCastContext)
    new CloseableColumnBatchIterator(relation.deserialized)
  }
}
