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

package io.glutenproject.backendsapi.clickhouse

import scala.collection.JavaConverters._

import io.glutenproject.{GlutenConfig, GlutenNumaBindingInfo}
import io.glutenproject.backendsapi.IIteratorApi
import io.glutenproject.execution._
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder}
import io.glutenproject.vectorized.{ExpressionEvaluatorJniWrapper, _}
import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class CHIteratorApi extends IIteratorApi {

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genNativeFilePartition(p: InputPartition,
                                      wsCxt: WholestageTransformContext
                                     ): BaseNativeFilePartition = {
    p match {
      case p: NativeMergeTreePartition =>
        val extensionTableNode =
          ExtensionTableBuilder.makeExtensionTable(p.minParts,
            p.maxParts, p.database, p.table, p.tablePath)
        wsCxt.substraitContext.setExtensionTableNode(extensionTableNode)
        // logWarning(s"The substrait plan for partition " +
        //   s"${p.index}:\n${wsCxt.root.toProtobuf.toString}")
        p.copySubstraitPlan(wsCxt.root.toProtobuf.toByteArray)
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
    val operator = new CHCoalesceOperator(recordsPerBatch)
    val res = new Iterator[ColumnarBatch] {

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

        while(!operator.isFull && iter.hasNext) {
          val cb = iter.next();
          numInputBatches += 1;
          operator.mergeBlock(cb)
        }
        val res = operator.release().toColumnarBatch
        CHNativeBlock.fromColumnarBatch(res).ifPresent(block => {
          numOutputRows += block.numRows();
          numOutputBatches += 1;
        })
        res
      }

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        operator.close()
      }
    }

    new CloseableCHColumnBatchIterator(res)
  }


  /**
   * Generate closeable ColumnBatch iterator.
   *
   * @param iter
   * @return
   */
  override def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]
                                              ): Iterator[ColumnarBatch] = {
    new CloseableCHColumnBatchIterator(iter)
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
    var resIter : GeneralOutIterator = null
    if (loadNative) {
      // TODO: 'jarList' is kept for codegen
      val transKernel = new ExpressionEvaluator(jarList.asJava)
      val inBatchIters = new java.util.ArrayList[GeneralInIterator]()
      resIter = transKernel.createKernelWithBatchIterator(
        inputPartition.substraitPlan, inBatchIters)
      TaskContext.get().addTaskCompletionListener[Unit] { _ => resIter.close() }
    }
    val iter = new Iterator[Any] {
      // private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

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
        resIter.next()
      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context,
      new CloseableCHColumnBatchIterator(iter.asInstanceOf[Iterator[ColumnarBatch]]))
  }

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
                                     streamedSortPlan: SparkPlan,
                                     pipelineTime: SQLMetric,
                                     buildRelationBatchHolder: Seq[ColumnarBatch],
                                     dependentKernels: Seq[ExpressionEvaluator],
                                     dependentKernelIterators: Seq[GeneralOutIterator]
                                    ): Iterator[ColumnarBatch] = {
    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    GlutenConfig.getConf
    val transKernel = new ExpressionEvaluator()
    val columnarNativeIterator =
      new util.ArrayList[GeneralInIterator](inputIterators.map { iter =>
        new ColumnarNativeIterator(iter.asJava)
      }.asJava)
    // we need to complete dependency RDD's firstly
    val beforeBuild = System.nanoTime()
    val nativeIterator = transKernel.createKernelWithBatchIterator(rootNode, columnarNativeIterator)
    build_elapse += System.nanoTime() - beforeBuild
    val resIter = streamedSortPlan match {
      case t: TransformSupport =>
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = {
            val res = nativeIterator.hasNext
            res
          }

          override def next(): ColumnarBatch = {
            val beforeEval = System.nanoTime()
            nativeIterator.next()
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

    TaskContext.get().addTaskCompletionListener[Unit] { _ =>
      close
    }
    new CloseableCHColumnBatchIterator(resIter)
  }

  /**
   * Generate columnar native iterator.
   *
   * @return
   */
  override def genColumnarNativeIterator(delegated: Iterator[ColumnarBatch]
                                        ): ColumnarNativeIterator = {
    new ColumnarNativeIterator(delegated.asJava)
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
    val batchIteratorInstance = jniWrapper.nativeCreateKernelWithIterator(
      0L, wsPlan, iterList.toArray);
    new BatchIterator(batchIteratorInstance)
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
