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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenNumaBindingInfo
import org.apache.gluten.backendsapi.IteratorApi
import org.apache.gluten.execution._
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.memory.CHThreadGroup
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel._
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.vectorized.{BatchIterator, CHNativeExpressionEvaluator, CloseableCHColumnBatchIterator}

import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}
import org.apache.spark.affinity.CHAffinity
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.CHColumnarShuffleWriter
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.clickhouse.{ExtensionTableBuilder, ExtensionTableNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.lang.{Long => JLong}
import java.net.URI
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CHIteratorApi extends IteratorApi with Logging with LogLevelUtil {

  private def getFileSchema(schema: StructType, names: Seq[String]): StructType = {
    val dataSchema = ArrayBuffer[StructField]()
    schema.foreach {
      field =>
        // case-insensitive schema matching
        val newField = names.find(_.equalsIgnoreCase(field.name)) match {
          case Some(name) => StructField(name, field.dataType, field.nullable, field.metadata)
          case _ => field
        }
        dataSchema += newField
    }
    StructType(dataSchema.toSeq)
  }

  private def createNativeIterator(
      splitInfoByteArray: Array[Array[Byte]],
      wsPlan: Array[Byte],
      materializeInput: Boolean,
      inputIterators: Seq[Iterator[ColumnarBatch]]): BatchIterator = {

    /** Generate closeable ColumnBatch iterator. */
    val listIterator =
      inputIterators
        .map {
          case i: CloseableCHColumnBatchIterator => i
          case it => new CloseableCHColumnBatchIterator(it)
        }
        .map(it => new ColumnarNativeIterator(it.asJava))
        .asJava
    CHNativeExpressionEvaluator.createKernelWithBatchIterator(
      wsPlan,
      splitInfoByteArray,
      listIterator,
      materializeInput
    )

  }

  private def createCloseIterator(
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      updateInputMetrics: Option[InputMetricsWrapper => Unit] = None,
      nativeIter: BatchIterator): CloseableCHColumnBatchIterator = {

    val iter = new CollectMetricIterator(
      nativeIter,
      updateNativeMetrics,
      updateInputMetrics,
      updateInputMetrics.map(_ => context.taskMetrics().inputMetrics).orNull)

    context.addTaskFailureListener(
      (ctx, _) => {
        if (ctx.isInterrupted()) {
          iter.cancel()
        }
      })
    context.addTaskCompletionListener[Unit](_ => iter.close())
    new CloseableCHColumnBatchIterator(iter, Some(pipelineTime))
  }

  // only set file schema for text format table
  private def setFileSchemaForLocalFiles(
      localFilesNode: LocalFilesNode,
      scan: BasicScanExecTransformer): Unit = {
    if (scan.fileFormat == ReadFileFormat.TextReadFormat) {
      val names =
        ConverterUtils.collectAttributeNamesWithoutExprId(scan.outputAttributes())
      localFilesNode.setFileSchema(getFileSchema(scan.getDataSchema, names.asScala.toSeq))
    }
  }

  override def genSplitInfo(
      partition: InputPartition,
      partitionSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo = {
    partition match {
      case p: GlutenMergeTreePartition =>
        val partLists = new JArrayList[String]()
        val starts = new JArrayList[JLong]()
        val lengths = new JArrayList[JLong]()
        p.partList
          .foreach(
            parts => {
              partLists.add(parts.name)
              starts.add(parts.start)
              lengths.add(parts.length)
            })
        ExtensionTableBuilder
          .makeExtensionTable(
            -1L,
            -1L,
            p.database,
            p.table,
            p.snapshotId,
            p.relativeTablePath,
            p.absoluteTablePath,
            p.orderByKey,
            p.lowCardKey,
            p.minmaxIndexKey,
            p.bfIndexKey,
            p.setIndexKey,
            p.primaryKey,
            partLists,
            starts,
            lengths,
            p.tableSchemaJson,
            p.clickhouseTableConfigs.asJava,
            CHAffinity.getNativeMergeTreePartitionLocations(p).toList.asJava
          )
      case f: FilePartition =>
        val paths = new JArrayList[String]()
        val starts = new JArrayList[JLong]()
        val lengths = new JArrayList[JLong]()
        val fileSizes = new JArrayList[JLong]()
        val modificationTimes = new JArrayList[JLong]()
        val partitionColumns = new JArrayList[JMap[String, String]]
        f.files.foreach {
          file =>
            paths.add(new URI(file.filePath.toString()).toASCIIString)
            starts.add(JLong.valueOf(file.start))
            lengths.add(JLong.valueOf(file.length))
            // TODO: Support custom partition location
            val partitionColumn = new JHashMap[String, String]()
            partitionColumns.add(partitionColumn)
            val (fileSize, modificationTime) =
              SparkShimLoader.getSparkShims.getFileSizeAndModificationTime(file)
            (fileSize, modificationTime) match {
              case (Some(size), Some(time)) =>
                fileSizes.add(JLong.valueOf(size))
                modificationTimes.add(JLong.valueOf(time))
              case _ =>
                fileSizes.add(0)
                modificationTimes.add(0)
            }
        }
        val preferredLocations =
          CHAffinity.getFilePartitionLocations(paths.asScala.toArray, f.preferredLocations())
        LocalFilesBuilder.makeLocalFiles(
          f.index,
          paths,
          starts,
          lengths,
          fileSizes,
          modificationTimes,
          partitionColumns,
          new JArrayList[JMap[String, String]](),
          fileFormat,
          preferredLocations.toList.asJava,
          mapAsJavaMap(properties)
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported input partition: $partition.")
    }
  }

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      scans: Seq[BasicScanExecTransformer]): Seq[BaseGlutenPartition] = {
    // Only serialize plan once, save lots time when plan is complex.
    val planByteArray = wsCtx.root.toProtobuf.toByteArray
    splitInfos.zipWithIndex.map {
      case (splits, index) =>
        val files = ArrayBuffer[String]()
        val splitInfosByteArray = splits.zipWithIndex.map {
          case (split, i) =>
            split match {
              case filesNode: LocalFilesNode =>
                setFileSchemaForLocalFiles(filesNode, scans(i))
                filesNode.getPaths.forEach(f => files += f)
                filesNode.toProtobuf.toByteArray
              case extensionTableNode: ExtensionTableNode =>
                extensionTableNode.getPartList.forEach(
                  name => files += extensionTableNode.getAbsolutePath + "/" + name)
                extensionTableNode.toProtobuf.toByteArray
            }
        }

        GlutenPartition(
          index,
          planByteArray,
          splitInfosByteArray.toArray,
          locations = splits.flatMap(_.preferredLocations().asScala).toArray,
          files.toArray
        )
    }
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
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()
  ): Iterator[ColumnarBatch] = {

    require(
      inputPartition.isInstanceOf[GlutenPartition],
      "CH backend only accepts GlutenPartition in GlutenWholeStageColumnarRDD.")
    val splitInfoByteArray = inputPartition
      .asInstanceOf[GlutenPartition]
      .splitInfosByteArray
    val wsPlan = inputPartition.plan
    val materializeInput = false

    new InterruptibleIterator(
      context,
      createCloseIterator(
        context,
        pipelineTime,
        updateNativeMetrics,
        Some(updateInputMetrics),
        createNativeIterator(splitInfoByteArray, wsPlan, materializeInput, inputIterators))
    )
  }

  // Generate Iterator[ColumnarBatch] for final stage.
  // scalastyle:off argcount
  override def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      numaBindingInfo: GlutenNumaBindingInfo,
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean): Iterator[ColumnarBatch] = {
    // scalastyle:on argcount

    // Final iterator does not contain scan split, so pass empty split info to native here.
    val splitInfoByteArray = new Array[Array[Byte]](0)
    val wsPlan = rootNode.toProtobuf.toByteArray

    // we need to complete dependency RDD's firstly
    createCloseIterator(
      context,
      pipelineTime,
      updateNativeMetrics,
      None,
      createNativeIterator(splitInfoByteArray, wsPlan, materializeInput, inputIterators))
  }

  override def injectWriteFilesTempPath(path: String, fileName: String): Unit = {
    CHThreadGroup.registerNewThreadGroup()
    CHNativeExpressionEvaluator.injectWriteFilesTempPath(path, fileName)
  }
}

class CollectMetricIterator(
    val nativeIterator: BatchIterator,
    val updateNativeMetrics: IMetrics => Unit,
    val updateInputMetrics: Option[InputMetricsWrapper => Unit] = None,
    val inputMetrics: InputMetrics = null
) extends Iterator[ColumnarBatch] {
  private var outputRowCount = 0L
  private var outputVectorCount = 0L
  private var metricsUpdated = false
  // Whether the stage is executed completely using ClickHouse pipeline.
  private var wholeStagePipeline = true

  override def hasNext: Boolean = {
    // The hasNext call is triggered only when there is a fallback.
    wholeStagePipeline = false
    nativeIterator.hasNext
  }

  override def next(): ColumnarBatch = {
    val cb = nativeIterator.next()
    outputVectorCount += 1
    outputRowCount += cb.numRows()
    cb
  }

  def close(): Unit = {
    collectStageMetrics()
    nativeIterator.close()
  }

  def cancel(): Unit = {
    collectStageMetrics()
    nativeIterator.cancel()
  }

  private def collectStageMetrics(): Unit = {
    if (!metricsUpdated) {
      val nativeMetrics = nativeIterator.getMetrics
      if (wholeStagePipeline) {
        outputRowCount = Math.max(outputRowCount, CHColumnarShuffleWriter.getTotalOutputRows)
        outputVectorCount =
          Math.max(outputVectorCount, CHColumnarShuffleWriter.getTotalOutputBatches)
      }
      nativeMetrics.setFinalOutputMetrics(outputRowCount, outputVectorCount)
      updateNativeMetrics(nativeMetrics)
      updateInputMetrics.foreach(_(inputMetrics))
      metricsUpdated = true
    }
  }
}
