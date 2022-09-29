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

package io.glutenproject.backendsapi

import io.glutenproject.GlutenNumaBindingInfo
import io.glutenproject.execution.{BaseNativeFilePartition, WholestageTransformContext}
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.vectorized.{ExpressionEvaluator, ExpressionEvaluatorJniWrapper, GeneralInIterator, GeneralOutIterator}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class IteratorApiImplSuite extends IIteratorApi {
  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genNativeFilePartition(index: Int,
                                      partitions: Seq[InputPartition],
                                      wsCxt: WholestageTransformContext
                                     ): BaseNativeFilePartition = null

  /**
   * Generate Iterator[ColumnarBatch] for CoalesceBatchesExec.
   *
   * @return
   */
  override def genCoalesceIterator(iter: Iterator[ColumnarBatch], recordsPerBatch: Int,
                                   numOutputRows: SQLMetric, numInputBatches: SQLMetric,
                                   numOutputBatches: SQLMetric, collectTime: SQLMetric,
                                   concatTime: SQLMetric, avgCoalescedNumRows: SQLMetric
                                  ): Iterator[ColumnarBatch] = null

  /**
   * Generate closeable ColumnBatch iterator.
   *
   * @return
   */
  override def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]
                                              ): Iterator[ColumnarBatch] = null

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   *
   * @return
   */
  override def genFirstStageIterator(inputPartition: BaseNativeFilePartition,
                                     loadNative: Boolean, outputAttributes: Seq[Attribute],
                                     context: TaskContext,
                                     pipelineTime: SQLMetric,
                                     updateMetrics: (Long, Long) => Unit,
                                     updateNativeMetrics: GeneralOutIterator => Unit,
                                     inputIterators: Seq[Iterator[ColumnarBatch]] = Seq())
  : Iterator[ColumnarBatch] = null

  /**
   * Generate Iterator[ColumnarBatch] for final stage.
   *
   * @return
   */
    // scalastyle:off argcount
  override def genFinalStageIterator(inputIterators: Seq[Iterator[ColumnarBatch]],
                                     numaBindingInfo: GlutenNumaBindingInfo,
                                     listJars: Seq[String], signature: String,
                                     sparkConf: SparkConf, outputAttributes: Seq[Attribute],
                                     rootNode: PlanNode,
                                     pipelineTime: SQLMetric,
                                     updateMetrics: (Long, Long) => Unit,
                                     updateNativeMetrics: GeneralOutIterator => Unit,
                                     buildRelationBatchHolder: Seq[ColumnarBatch],
                                     dependentKernels: Seq[ExpressionEvaluator],
                                     dependentKernelIterators: Seq[GeneralOutIterator]
                                    ): Iterator[ColumnarBatch] = null
  // scalastyle:on argcount
  /**
   * Generate columnar native iterator.
   *
   * @return
   */
  override def genColumnarNativeIterator(delegated: Iterator[ColumnarBatch]
                                        ): GeneralInIterator = null

  /**
   * Generate BatchIterator for ExpressionEvaluator.
   *
   * @return
   */
  override def genBatchIterator(wsPlan: Array[Byte],
                                iterList: Seq[GeneralInIterator],
                                jniWrapper: ExpressionEvaluatorJniWrapper,
                                outAttrs: Seq[Attribute]): GeneralOutIterator = null

  /**
   * Generate Native FileScanRDD, currently only for ClickHouse Backend.
   */
  override def genNativeFileScanRDD(sparkContext: SparkContext,
                                    wsCxt: WholestageTransformContext,
                                    fileFormat: ReadFileFormat,
                                    inputPartitions: Seq[InputPartition],
                                    numOutputRows: SQLMetric,
                                    numOutputBatches: SQLMetric,
                                    scanTime: SQLMetric): RDD[ColumnarBatch] = null

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = "default"
}
