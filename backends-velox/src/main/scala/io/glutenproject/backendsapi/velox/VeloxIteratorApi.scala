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

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.spark.InterruptibleIterator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ExecutorManager
import org.apache.spark.util.UserAddedJarUtils
import io.glutenproject.GlutenConfig
import io.glutenproject.GlutenNumaBindingInfo
import io.glutenproject.backendsapi.IIteratorApi
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.execution._
import io.glutenproject.expression.ArrowConverterUtils
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesBuilder
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.vectorized._
import org.apache.arrow.vector.types.pojo.Schema

class VeloxIteratorApi extends IIteratorApi with Logging {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genNativeFilePartition(index: Int,
                                      partitions: Seq[InputPartition],
                                      wsCxt: WholestageTransformContext
                                     ): BaseNativeFilePartition = {
    val localFilesNodes = partitions.indices.map(i =>
      partitions(i) match {
        case FilePartition(index, files) =>
          val paths = new java.util.ArrayList[String]()
          val starts = new java.util.ArrayList[java.lang.Long]()
          val lengths = new java.util.ArrayList[java.lang.Long]()
          val fileFormat = wsCxt.substraitContext.getFileFormat.get(0)
          files.foreach { f =>
            paths.add(f.filePath)
            starts.add(new java.lang.Long(f.start))
            lengths.add(new java.lang.Long(f.length))
          }
          LocalFilesBuilder.makeLocalFiles(
            index, paths, starts, lengths, fileFormat)
      }
    )
    wsCxt.substraitContext.initLocalFilesNodesIndex(0)
    wsCxt.substraitContext.setLocalFilesNodes(localFilesNodes)
    val substraitPlan = wsCxt.root.toProtobuf
    logDebug(s"The substrait plan for partition ${index}:\n${substraitPlan.toString}")
    NativePartition(index, substraitPlan.toByteArray)
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
                                   numOutputRows: SQLMetric,
                                   numInputBatches: SQLMetric,
                                   numOutputBatches: SQLMetric,
                                   collectTime: SQLMetric,
                                   concatTime: SQLMetric,
                                   avgCoalescedNumRows: SQLMetric): Iterator[ColumnarBatch] = {
    import io.glutenproject.utils.VeloxImplicitClass._

    val beforeInput = System.nanoTime
    val hasInput = iter.hasNext
    collectTime += System.nanoTime - beforeInput
    val res = if (hasInput) {
      new Iterator[ColumnarBatch] {
        var numBatchesTotal: Long = _
        var numRowsTotal: Long = _
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ =>
          if (numBatchesTotal > 0) {
            avgCoalescedNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
        }

        override def hasNext: Boolean = {
          val beforeNext = System.nanoTime
          val hasNext = iter.hasNext
          collectTime += System.nanoTime - beforeNext
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
          val expected_output_arrow_fields = if (batchesToAppend.size > 0) {
            (0 until batchesToAppend(0).numCols).map(i => {
              ArrowColumnarBatches
                .ensureLoaded(
                  SparkMemoryUtils.contextArrowAllocator(), batchesToAppend(0)).column(i)
                .asInstanceOf[ArrowWritableColumnVector]
                .getValueVector
                .getField
            })
          } else {
            Nil
          }

          val resultStructType =
            ArrowUtils.fromArrowSchema(new Schema(expected_output_arrow_fields.asJava))
          val beforeConcat = System.nanoTime
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(rowCount, resultStructType).toArray
          val target =
            new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), rowCount)
          coalesce(target, batchesToAppend.toList)
          target.setNumRows(rowCount)

          concatTime += System.nanoTime - beforeConcat
          numOutputRows += rowCount
          numInputBatches += batchesToAppend.length
          numOutputBatches += 1

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
  override def genFirstStageIterator(inputPartition: BaseNativeFilePartition,
                                     loadNative: Boolean,
                                     outputAttributes: Seq[Attribute],
                                     context: TaskContext,
                                     pipelineTime: SQLMetric,
                                     updateMetrics: (Long, Long) => Unit,
                                     updateNativeMetrics: GeneralOutIterator => Unit,
                                     inputIterators: Seq[Iterator[ColumnarBatch]] = Seq())
  : Iterator[ColumnarBatch] = {
    import org.apache.spark.sql.util.OASPackageBridge._
    var inputSchema: Schema = null
    var outputSchema: Schema = null
    var resIter: GeneralOutIterator = null
    if (loadNative) {
      val beforeBuild = System.nanoTime()
      val columnarNativeIterators =
        new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
          new ArrowInIterator(iter.asJava)
        }.asJava)
      val transKernel = new ExpressionEvaluator()
      outputSchema = ArrowConverterUtils.toArrowSchema(outputAttributes)
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan, columnarNativeIterators, outputAttributes.asJava)
      pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ => resIter.close() }
    }
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        val res = if (loadNative) {
          resIter.hasNext
        } else {
          false
        }
        if (!res) {
          updateNativeMetrics(resIter)
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
        updateMetrics(1, cb.numRows())
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
                                     dependentKernelIterators: Seq[GeneralOutIterator])
  : Iterator[ColumnarBatch] = {

    ExecutorManager.tryTaskSet(numaBindingInfo)

    val beforeBuild = System.nanoTime()

    val execTempDir = GlutenConfig.getTempFile
    val jarList = listJars.map(jarUrl => {
      logWarning(s"Get Codegened library Jar ${jarUrl}")
      UserAddedJarUtils.fetchJarFromSpark(
        jarUrl,
        execTempDir,
        s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
        sparkConf)
      s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
    })

    val transKernel = new ExpressionEvaluator(jarList.asJava)
    val columnarNativeIterator =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ArrowInIterator(iter.asJava)
      }.asJava)
    val nativeResultIterator =
      transKernel.createKernelWithBatchIterator(rootNode, columnarNativeIterator,
        outputAttributes.asJava)

    pipelineTime += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

    val resIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        val res = nativeResultIterator.hasNext
        if (!res) {
          updateNativeMetrics(nativeResultIterator)
        }
        res
      }

      override def next(): ColumnarBatch = {
        val cb = nativeResultIterator.next
        updateMetrics(1, cb.numRows())
        cb
      }
    }

    SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
      nativeResultIterator.close()
    })

    new CloseableColumnBatchIterator(resIter, Some(pipelineTime))
  }
  // scalastyle:on argcount

  /**
   * Generate columnar native iterator.
   *
   * @return
   */
  override def genColumnarNativeIterator(delegated: Iterator[ColumnarBatch]): ArrowInIterator = {
    new ArrowInIterator(delegated.asJava)
  }

  /**
   * Generate BatchIterator for ExpressionEvaluator.
   *
   * @return
   */
  override def genBatchIterator(wsPlan: Array[Byte],
                                iterList: Seq[GeneralInIterator],
                                jniWrapper: ExpressionEvaluatorJniWrapper,
                                outAttrs: Seq[Attribute]): GeneralOutIterator = {
    val alloc = SparkMemoryUtils.contextNativeAllocator()
    val allocId = alloc.getNativeInstanceId
    val batchIteratorInstance =
      jniWrapper.nativeCreateKernelWithIterator(allocId, wsPlan, iterList.toArray)
    new ArrowOutIterator(batchIteratorInstance, outAttrs.asJava)
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

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_VELOX_BACKEND
}
