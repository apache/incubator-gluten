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

import org.apache.gluten.{GlutenConfig, GlutenNumaBindingInfo}
import org.apache.gluten.backendsapi.IteratorApi
import org.apache.gluten.execution._
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.metrics.{IMetrics, NativeMetrics}
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel._
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.LogLevelUtil
import org.apache.gluten.vectorized.{BatchIterator, CHNativeExpressionEvaluator, CloseableCHColumnBatchIterator, GeneralInIterator}

import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}
import org.apache.spark.affinity.CHAffinity
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

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
    StructType(dataSchema)
  }

  // only set file schema for text format table
  private def setFileSchemaForLocalFiles(
      localFilesNode: LocalFilesNode,
      scan: BasicScanExecTransformer): Unit = {
    if (scan.fileFormat == ReadFileFormat.TextReadFormat) {
      val names =
        ConverterUtils.collectAttributeNamesWithoutExprId(scan.outputAttributes())
      localFilesNode.setFileSchema(getFileSchema(scan.getDataSchema, names.asScala))
    }
  }

  override def genSplitInfo(
      partition: InputPartition,
      partitionSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      serializableHadoopConf: SerializableConfiguration): SplitInfo = {
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
        val partitionColumns = new JArrayList[JMap[String, String]]
        f.files.foreach {
          file =>
            paths.add(new URI(file.filePath.toString()).toASCIIString)
            starts.add(JLong.valueOf(file.start))
            lengths.add(JLong.valueOf(file.length))
            // TODO: Support custom partition location
            val partitionColumn = new JHashMap[String, String]()
            partitionColumns.add(partitionColumn)
        }
        val preferredLocations =
          CHAffinity.getFilePartitionLocations(paths.asScala.toArray, f.preferredLocations())
        LocalFilesBuilder.makeLocalFiles(
          f.index,
          paths,
          starts,
          lengths,
          new JArrayList[JLong](),
          new JArrayList[JLong](),
          partitionColumns,
          new JArrayList[JMap[String, String]](),
          fileFormat,
          preferredLocations.toList.asJava
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
                filesNode.setFileReadProperties(mapAsJavaMap(scans(i).getProperties))
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

    assert(
      inputPartition.isInstanceOf[GlutenPartition],
      "CH backend only accepts GlutenPartition in GlutenWholeStageColumnarRDD.")

    val transKernel = new CHNativeExpressionEvaluator()
    val inBatchIters = new JArrayList[GeneralInIterator](inputIterators.map {
      iter => new ColumnarNativeIterator(CHIteratorApi.genCloseableColumnBatchIterator(iter).asJava)
    }.asJava)

    val splitInfoByteArray = inputPartition
      .asInstanceOf[GlutenPartition]
      .splitInfosByteArray
    val nativeIter =
      transKernel.createKernelWithBatchIterator(
        inputPartition.plan,
        splitInfoByteArray,
        inBatchIters,
        false)

    val iter = new CollectMetricIterator(
      nativeIter,
      updateNativeMetrics,
      updateInputMetrics,
      context.taskMetrics().inputMetrics)

    context.addTaskFailureListener(
      (ctx, _) => {
        if (ctx.isInterrupted()) {
          iter.cancel()
        }
      })
    context.addTaskCompletionListener[Unit](_ => iter.close())

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
    GlutenConfig.getConf

    val transKernel = new CHNativeExpressionEvaluator()
    val columnarNativeIterator =
      new JArrayList[GeneralInIterator](inputIterators.map {
        iter =>
          new ColumnarNativeIterator(CHIteratorApi.genCloseableColumnBatchIterator(iter).asJava)
      }.asJava)
    // we need to complete dependency RDD's firstly
    val nativeIterator = transKernel.createKernelWithBatchIterator(
      rootNode.toProtobuf.toByteArray,
      // Final iterator does not contain scan split, so pass empty split info to native here.
      new Array[Array[Byte]](0),
      columnarNativeIterator,
      materializeInput
    )

    val iter = new CollectMetricIterator(nativeIterator, updateNativeMetrics, null, null)

    context.addTaskFailureListener(
      (ctx, _) => {
        if (ctx.isInterrupted()) {
          iter.cancel()
        }
      })
    context.addTaskCompletionListener[Unit](_ => iter.close())
    new CloseableCHColumnBatchIterator(iter, Some(pipelineTime))
  }
}

object CHIteratorApi {

  /** Generate closeable ColumnBatch iterator. */
  def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    iter match {
      case _: CloseableCHColumnBatchIterator => iter
      case _ => new CloseableCHColumnBatchIterator(iter)
    }
  }
}

class CollectMetricIterator(
    val nativeIterator: BatchIterator,
    val updateNativeMetrics: IMetrics => Unit,
    val updateInputMetrics: InputMetricsWrapper => Unit,
    val inputMetrics: InputMetrics
) extends Iterator[ColumnarBatch] {
  private var outputRowCount = 0L
  private var outputVectorCount = 0L
  private var metricsUpdated = false

  override def hasNext: Boolean = {
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
      val nativeMetrics = nativeIterator.getMetrics.asInstanceOf[NativeMetrics]
      nativeMetrics.setFinalOutputMetrics(outputRowCount, outputVectorCount)
      updateNativeMetrics(nativeMetrics)
      if (updateInputMetrics != null) {
        updateInputMetrics(inputMetrics)
      }
      metricsUpdated = true
    }
  }
}
