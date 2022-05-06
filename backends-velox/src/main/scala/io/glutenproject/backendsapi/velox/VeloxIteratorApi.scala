/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.backendsapi.velox

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import io.glutenproject.{GlutenConfig, GlutenNumaBindingInfo}
import io.glutenproject.backendsapi.IIteratorApi
import io.glutenproject.execution._
import io.glutenproject.expression.ArrowConverterUtils
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesBuilder
import io.glutenproject.vectorized._
import org.apache.arrow.dataset.jni.JniLoader
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.{ExecutorManager, UserAddedJarUtils}

class VeloxIteratorApi extends IIteratorApi with Logging {

  JniLoader.get().ensureLoaded()

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genNativeFilePartition(p: InputPartition,
                                      wsCxt: WholestageTransformContext
                                     ): BaseNativeFilePartition = {
    p match {
      case FilePartition(index, files) =>
        val paths = new java.util.ArrayList[String]()
        val starts = new java.util.ArrayList[java.lang.Long]()
        val lengths = new java.util.ArrayList[java.lang.Long]()
        files.foreach { f =>
          paths.add(f.filePath)
          starts.add(new java.lang.Long(f.start))
          lengths.add(new java.lang.Long(f.length))
        }
        val localFilesNode = LocalFilesBuilder.makeLocalFiles(index, paths, starts, lengths)
        wsCxt.substraitContext.setLocalFilesNode(localFilesNode)
        val substraitPlan = wsCxt.root.toProtobuf
        /*
        val out = new DataOutputStream(new FileOutputStream("/tmp/SubStraitTest-Q6.dat",
                    false));
        out.write(substraitPlan.toByteArray());
        out.flush();
         */
        // logWarning(s"The substrait plan for partition ${index}:\n${substraitPlan.toString}")
        NativeFilePartition(index, files, substraitPlan.toByteArray)
    }
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
              batchesToAppend(0).column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector.getField
            })
          } else {
            Nil
          }

          val resultStructType = ArrowUtils.fromArrowSchema(new Schema(expected_output_arrow_fields.asJava))
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
  override def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]
                                              ): Iterator[ColumnarBatch] = {
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
                                     jarList: Seq[String]): Iterator[ColumnarBatch] = {
    import org.apache.spark.sql.util.OASPackageBridge._
    var inputSchema : Schema = null
    var outputSchema : Schema = null
    var resIter : GeneralOutIterator = null
    if (loadNative) {
      // TODO: 'jarList' is kept for codegen
      val transKernel = new ExpressionEvaluator(jarList.asJava)
      val inBatchIters = new java.util.ArrayList[GeneralInIterator]()
      outputSchema = ArrowConverterUtils.toArrowSchema(outputAttributes)
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan, inBatchIters)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ => resIter.close() }
    }
    val iter = new Iterator[Any] {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        if (loadNative) {
          resIter.hasNext
        } else {
          false
        }
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val rb = resIter.next().asInstanceOf[ArrowRecordBatch]
        if (rb == null) {
          val resultStructType = ArrowUtils.fromArrowSchema(outputSchema)
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
        val outputNumRows = rb.getLength
        val output = ArrowConverterUtils.fromArrowRecordBatch(outputSchema, rb)
        ArrowConverterUtils.releaseArrowRecordBatch(rb)
        val cb = new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]), outputNumRows)
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
        cb
      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context,
      new CloseableColumnBatchIterator(iter.asInstanceOf[Iterator[ColumnarBatch]]))
  }

  /**
   * Generate Iterator[ColumnarBatch] for final stage.
   *
   * @return
   */
  override def genFinalStageIterator(iter: Iterator[ColumnarBatch],
                                     numaBindingInfo: GlutenNumaBindingInfo,
                                     listJars: Seq[String],
                                     signature: String,
                                     sparkConf: SparkConf,
                                     outputAttributes: Seq[Attribute],
                                     rootNode: PlanNode,
                                     streamedSortPlan: SparkPlan,
                                     pipelineTime: SQLMetric,
                                     buildRelationBatchHolder: Seq[ColumnarBatch],
                                     dependentKernels: Seq[ExpressionEvaluator],
                                     dependentKernelIterators: Seq[GeneralOutIterator]
                                    ): Iterator[ColumnarBatch] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)
    GlutenConfig.getConf
    var build_elapse: Long = 0
    var eval_elapse: Long = 0
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

    val transKernel = new ExpressionEvaluator(jarList.toList.asJava)
    val inBatchIter = new VeloxInIterator(iter.asJava)
    val inBatchIters = new java.util.ArrayList[GeneralInIterator]()
    inBatchIters.add(inBatchIter)
    // we need to complete dependency RDD's firstly
    val beforeBuild = System.nanoTime()
    val outputSchema = ArrowConverterUtils.toArrowSchema(outputAttributes)
    val nativeIterator = transKernel.createKernelWithBatchIterator(rootNode, inBatchIters)
    build_elapse += System.nanoTime() - beforeBuild
    val resultStructType = ArrowUtils.fromArrowSchema(outputSchema)
    val resIter = streamedSortPlan match {
      case t: TransformSupport =>
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = {
            val res = nativeIterator.hasNext
            // if (res == false) updateMetrics(nativeIterator)
            res
          }

          override def next(): ColumnarBatch = {
            val beforeEval = System.nanoTime()
            val output_rb = nativeIterator.next.asInstanceOf[ArrowRecordBatch]
            if (output_rb == null) {
              eval_elapse += System.nanoTime() - beforeEval
              val resultColumnVectors =
                ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
              return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
            }
            val outputNumRows = output_rb.getLength
            val outSchema = ArrowConverterUtils.toArrowSchema(outputAttributes)
            val output = ArrowConverterUtils.fromArrowRecordBatch(outSchema, output_rb)
            ArrowConverterUtils.releaseArrowRecordBatch(output_rb)
            eval_elapse += System.nanoTime() - beforeEval
            new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]), outputNumRows)
          }
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"streamedSortPlan should support transformation")
    }
    var closed = false

    def close = {
      closed = true
      pipelineTime += (eval_elapse + build_elapse) / 1000000
      buildRelationBatchHolder.foreach(_.close) // fixing: ref cnt goes nagative
      dependentKernels.foreach(_.close)
      dependentKernelIterators.foreach(_.close)
      nativeIterator.close()
      // relationHolder.clear()
    }

    SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
      close
    })
    new CloseableColumnBatchIterator(resIter)
  }

  /**
   * Generate columnar native iterator.
   *
   * @return
   */
  override def genColumnarNativeIterator(delegated: Iterator[ColumnarBatch]
                                        ): VeloxInIterator = {
    new VeloxInIterator(delegated.asJava)
  }

  /**
   * Generate BatchIterator for ExpressionEvaluator.
   *
   * @return
   */
  override def genBatchIterator(wsPlan: Array[Byte],
                                iterList: Seq[GeneralInIterator],
                                jniWrapper: ExpressionEvaluatorJniWrapper
                               ): GeneralOutIterator = {
    val memoryPool = SparkMemoryUtils.contextMemoryPool();
    val poolId = memoryPool.getNativeInstanceId();
    val batchIteratorInstance = jniWrapper.nativeCreateKernelWithIterator(
      poolId, wsPlan, iterList.toArray);
    new VeloxOutIterator(batchIteratorInstance)
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_VELOX_BACKEND
}
