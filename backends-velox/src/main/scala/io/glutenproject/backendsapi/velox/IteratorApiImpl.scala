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
import io.glutenproject.substrait.rel.{LocalFilesBuilder, SplitInfo}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.Iterators
import io.glutenproject.vectorized._

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
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
import org.apache.spark.util.ExecutorManager

import java.lang.{Long => JLong}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._

class IteratorApiImpl extends IteratorApi with Logging {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genSplitInfo(
      partition: InputPartition,
      partitionSchemas: StructType,
      fileFormat: ReadFileFormat): SplitInfo = {
    partition match {
      case f: FilePartition =>
        val (paths, starts, lengths, partitionColumns) =
          constructSplitInfo(partitionSchemas, f.files)
        val preferredLocations =
          SoftAffinityUtil.getFilePartitionLocations(paths.asScala.toArray, f.preferredLocations())
        LocalFilesBuilder.makeLocalFiles(
          f.index,
          paths,
          starts,
          lengths,
          partitionColumns,
          fileFormat,
          preferredLocations.toList.asJava)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported input partition.")
    }
  }

  private def constructSplitInfo(schema: StructType, files: Array[PartitionedFile]) = {
    val paths = new JArrayList[String]()
    val starts = new JArrayList[JLong]
    val lengths = new JArrayList[JLong]()
    val partitionColumns = new JArrayList[JMap[String, String]]
    files.foreach {
      file =>
        paths.add(URLDecoder.decode(file.filePath.toString, StandardCharsets.UTF_8.name()))
        starts.add(JLong.valueOf(file.start))
        lengths.add(JLong.valueOf(file.length))

        val partitionColumn = new JHashMap[String, String]()
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
        partitionColumns.add(partitionColumn)
    }
    (paths, starts, lengths, partitionColumns)
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
    val columnarNativeIterators =
      new JArrayList[GeneralInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(iter.asJava)
      }.asJava)
    val transKernel = NativePlanEvaluator.create()
    val resIter: GeneralOutIterator =
      transKernel.createKernelWithBatchIterator(inputPartition.plan, columnarNativeIterators)

    Iterators
      .wrap(resIter.asScala)
      .protectInvocationFlow()
      .recycleIterator {
        updateNativeMetrics(resIter.getMetrics)
        updateInputMetrics(TaskContext.get().taskMetrics().inputMetrics)
        resIter.close()
      }
      .recyclePayload(batch => batch.close())
      .addToPipelineTime(pipelineTime)
      .asInterruptible(context)
      .create()
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

    val transKernel = NativePlanEvaluator.create()
    val columnarNativeIterator =
      new JArrayList[GeneralInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(iter.asJava)
      }.asJava)
    val nativeResultIterator =
      transKernel.createKernelWithBatchIterator(
        rootNode.toProtobuf.toByteArray,
        columnarNativeIterator)

    Iterators
      .wrap(nativeResultIterator.asScala)
      .recycleIterator {
        updateNativeMetrics(nativeResultIterator.getMetrics)
        nativeResultIterator.close()
      }
      .recyclePayload(batch => batch.close())
      .addToPipelineTime(pipelineTime)
      .create()
  }
  // scalastyle:on argcount

  /** Generate Native FileScanRDD, currently only for ClickHouse Backend. */
  override def genNativeFileScanRDD(
      sparkContext: SparkContext,
      wsCxt: WholeStageTransformContext,
      splitInfos: Seq[SplitInfo],
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      scanTime: SQLMetric): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot support to generate Native FileScanRDD.")
  }

  /** Compute for BroadcastBuildSideRDD */
  override def genBroadcastBuildSideIterator(
      broadcasted: Broadcast[BuildSideRelation],
      broadCastContext: BroadCastHashJoinContext): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy(broadCastContext)
    Iterators
      .wrap(relation.deserialized)
      .recyclePayload(batch => batch.close())
      .create()
  }
}
