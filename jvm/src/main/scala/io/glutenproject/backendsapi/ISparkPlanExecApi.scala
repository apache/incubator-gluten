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

package io.glutenproject.backendsapi

import io.glutenproject.execution.{NativeColumnarToRowExec, RowToArrowColumnarExec}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ISparkPlanExecApi extends IBackendsApi {

  /**
   * Generate NativeColumnarToRowExec.
   *
   * @param child
   * @return
   */
  def genNativeColumnarToRowExec(child: SparkPlan): NativeColumnarToRowExec

  /**
   * Generate RowToArrowColumnarExec.
   *
   * @param child
   * @return
   */
  def genRowToArrowColumnarExec(child: SparkPlan): RowToArrowColumnarExec

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def genShuffleDependency(rdd: RDD[ColumnarBatch], outputAttributes: Seq[Attribute],
                           newPartitioning: Partitioning, serializer: Serializer,
                           writeMetrics: Map[String, SQLMetric], dataSize: SQLMetric,
                           bytesSpilled: SQLMetric, numInputRows: SQLMetric,
                           computePidTime: SQLMetric, splitTime: SQLMetric,
                           spillTime: SQLMetric, compressTime: SQLMetric, prepareTime: SQLMetric
                          ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  def genColumnarShuffleWriter[K, V](parameters: GenShuffleWriterParameters[K, V]
                                    ): GlutenShuffleWriterWrapper[K, V]

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def createColumnarBatchSerializer(schema: StructType, readBatchNumRows: SQLMetric,
                                    numOutputRows: SQLMetric): Serializer
}
