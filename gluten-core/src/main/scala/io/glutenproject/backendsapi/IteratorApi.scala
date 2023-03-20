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
import io.glutenproject.execution.{BaseGlutenPartition, BroadCastHashJoinContext, WholestageTransformContext}
import io.glutenproject.memory.TaskMemoryMetrics
import io.glutenproject.memory.alloc.{NativeMemoryAllocatorManager, Spiller}
import io.glutenproject.metrics.IMetrics
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.spark.{broadcast, Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

trait IteratorApi {

  /**
   * Generate native row partition.
   *
   * @return
   */
  def genFilePartition(index: Int,
                       partitions: Seq[InputPartition],
                       wsCxt: WholestageTransformContext): BaseGlutenPartition

  /**
   * Generate Iterator[ColumnarBatch] for CoalesceBatchesExec.
   *
   * @return
   */
  def genCoalesceIterator(iter: Iterator[ColumnarBatch],
                          recordsPerBatch: Int,
                          numOutputRows: SQLMetric = null,
                          numInputBatches: SQLMetric = null,
                          numOutputBatches: SQLMetric = null,
                          collectTime: SQLMetric = null,
                          concatTime: SQLMetric = null,
                          avgCoalescedNumRows: SQLMetric = null): Iterator[ColumnarBatch]

  /**
   * Generate closeable ColumnBatch iterator.
   *
   * @return
   */
  def genCloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   * ("first" means it does not depend on other SCAN inputs)
   *
   * @return
   */
  def genFirstStageIterator(inputPartition: BaseGlutenPartition,
                            outputAttributes: Seq[Attribute], context: TaskContext,
                            pipelineTime: SQLMetric,
                            updateInputMetrics: (InputMetricsWrapper) => Unit,
                            updateNativeMetrics: IMetrics => Unit,
                            inputIterators: Seq[Iterator[ColumnarBatch]] = Seq())
  : Iterator[ColumnarBatch]

  /**
   * Generate Iterator[ColumnarBatch] for final stage.
   * ("Final" means it depends on other SCAN inputs, maybe it was a mistake to use the word "final")
   *
   * @return
   */
  // scalastyle:off argcount
  def genFinalStageIterator(inputIterators: Seq[Iterator[ColumnarBatch]],
                            numaBindingInfo: GlutenNumaBindingInfo,
                            sparkConf: SparkConf,
                            outputAttributes: Seq[Attribute],
                            rootNode: PlanNode,
                            pipelineTime: SQLMetric,
                            updateNativeMetrics: IMetrics => Unit,
                            buildRelationBatchHolder: Seq[ColumnarBatch])
  : Iterator[ColumnarBatch]
  // scalastyle:on argcount

  /**
   * Generate Native FileScanRDD, currently only for ClickHouse Backend.
   */
  def genNativeFileScanRDD(sparkContext: SparkContext,
                           wsCxt: WholestageTransformContext,
                           fileFormat: ReadFileFormat,
                           inputPartitions: Seq[InputPartition],
                           numOutputRows: SQLMetric,
                           numOutputBatches: SQLMetric,
                           scanTime: SQLMetric
                          ): RDD[ColumnarBatch]

  /**
   * Generate NativeMemoryAllocatorManager. If the backend contains no native
   * components, then just throwing from this method is a recommended way.
   */
  def genNativeMemoryAllocatorManager(taskMemoryManager: TaskMemoryManager,
                                      spiller: Spiller,
                                      taskMemoryMetrics: TaskMemoryMetrics
                                     ): NativeMemoryAllocatorManager

  /**
   * Compute for BroadcastBuildSideRDD
   */
  def genBroadcastBuildSideIterator(split: Partition,
                                    context: TaskContext,
                                    broadcasted: broadcast.Broadcast[BuildSideRelation],
                                    broadCastContext: BroadCastHashJoinContext
                                   ): Iterator[ColumnarBatch]
}
