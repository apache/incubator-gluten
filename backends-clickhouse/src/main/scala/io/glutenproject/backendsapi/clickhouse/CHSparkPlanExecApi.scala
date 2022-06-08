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

import io.glutenproject.backendsapi.ISparkPlanExecApi
import io.glutenproject.GlutenConfig
import io.glutenproject.execution._
import io.glutenproject.vectorized.{BlockNativeWriter, CHColumnarBatchSerializer}
import org.apache.spark.{ShuffleDependency, SparkException}

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class CHSparkPlanExecApi extends ISparkPlanExecApi {

  /**
   * Generate NativeColumnarToRowExec.
   *
   * @param child
   * @return
   */
  override def genNativeColumnarToRowExec(child: SparkPlan): NativeColumnarToRowExec = {
    new BlockNativeColumnarToRowExec(child);
  }


  /**
   * Generate RowToArrowColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToArrowColumnarExec(child: SparkPlan): RowToArrowColumnarExec =
    throw new UnsupportedOperationException(
      "Cannot support RowToArrowColumnarExec operation with ClickHouse backend.")

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition: the filter condition
   * @param child: the chid of FilterExec
   * @return the transformer of FilterExec
   */
  override def genFilterExecTransformer(condition: Expression, child: SparkPlan)
    : FilterExecBaseTransformer = FilterExecTransformer(condition, child)

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def genShuffleDependency(rdd: RDD[ColumnarBatch],
                                    outputAttributes: Seq[Attribute],
                                    newPartitioning: Partitioning,
                                    serializer: Serializer,
                                    writeMetrics: Map[String, SQLMetric],
                                    dataSize: SQLMetric,
                                    bytesSpilled: SQLMetric,
                                    numInputRows: SQLMetric,
                                    computePidTime: SQLMetric,
                                    splitTime: SQLMetric,
                                    spillTime: SQLMetric,
                                    compressTime: SQLMetric,
                                    prepareTime: SQLMetric
                                   ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(rdd, outputAttributes, newPartitioning, serializer,
      writeMetrics, dataSize, bytesSpilled, numInputRows, computePidTime, splitTime,
      spillTime, compressTime, prepareTime)
  }

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](parameters: GenShuffleWriterParameters[K, V]
                                             ): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(schema: StructType,
                                             readBatchNumRows: SQLMetric,
                                             numOutputRows: SQLMetric): Serializer = {
    new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows)
  }


  /**
   * Create broadcast relation for BroadcastExchangeExec
   */
  override def createBroadcastRelation(child: SparkPlan,
                                       numOutputRows: SQLMetric,
                                       dataSize: SQLMetric): BuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions { iter =>
        var _numRows: Long = 0

        // Use for reading bytes array from block
        val blockNativeWriter = new BlockNativeWriter()
        while (iter.hasNext) {
          val batch = iter.next
          blockNativeWriter.write(batch)
          _numRows += batch.numRows
        }
        Iterator((_numRows, blockNativeWriter.collectAsByteArray()))
      }
      .collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += countsAndBytes.map(_._1).sum
    dataSize += rawSize
    ClickHouseBuildSideRelation(child.output, batches)
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
