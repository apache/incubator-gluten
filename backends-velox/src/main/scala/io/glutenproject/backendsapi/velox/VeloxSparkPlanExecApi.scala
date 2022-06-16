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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.ISparkPlanExecApi
import io.glutenproject.execution.{FilterExecBaseTransformer, FilterExecTransformer, NativeColumnarToRowExec, RowToArrowColumnarExec, VeloxFilterExecTransformer, VeloxNativeColumnarToRowExec, VeloxRowToArrowColumnarExec}
import io.glutenproject.expression.ArrowConverterUtils
import io.glutenproject.vectorized.{ArrowColumnarBatchSerializer, ArrowWritableColumnVector}
import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.VeloxShuffleUtil
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, VeloxBuildSideRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.VeloxExecUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ArrayBuffer

class VeloxSparkPlanExecApi extends ISparkPlanExecApi {

  /**
   * Generate NativeColumnarToRowExec.
   *
   * @param child
   * @return
   */
  override def genNativeColumnarToRowExec(child: SparkPlan): NativeColumnarToRowExec =
    new VeloxNativeColumnarToRowExec(child)

  /**
   * Generate RowToArrowColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToArrowColumnarExec(child: SparkPlan): RowToArrowColumnarExec =
    new VeloxRowToArrowColumnarExec(child)

  // scalastyle:off argcount
  /**
   * Generate FilterExecTransformer.
   *
   * @param condition: the filter condition
   * @param child: the chid of FilterExec
   * @return the transformer of FilterExec
   */
  override def genFilterExecTransformer(condition: Expression, child: SparkPlan)
    : FilterExecBaseTransformer =
    if (GlutenConfig.getSessionConf.isGazelleBackend) {
      // Use the original Filter for Arrow backend.
      FilterExecTransformer(condition, child)
    } else {
      VeloxFilterExecTransformer(condition, child)
    }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
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
      prepareTime: SQLMetric): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    VeloxExecUtil.genShuffleDependency(
      rdd,
      outputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      dataSize,
      bytesSpilled,
      numInputRows,
      computePidTime,
      splitTime,
      spillTime,
      compressTime,
      prepareTime)
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    VeloxShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      readBatchNumRows: SQLMetric,
      numOutputRows: SQLMetric): Serializer = {
    new ArrowColumnarBatchSerializer(schema, readBatchNumRows, numOutputRows)
  }

  /**
   * Create broadcast relation for BroadcastExchangeExec
   */
  override def createBroadcastRelation(
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions { iter =>
        var _numRows: Long = 0
        val _input = new ArrayBuffer[ColumnarBatch]()

        while (iter.hasNext) {
          val batch = iter.next
          (0 until batch.numCols).foreach(i =>
            batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
          _numRows += batch.numRows
          _input += batch
        }
        val bytes = ArrowConverterUtils.convertToNetty(_input.toArray)
        _input.foreach(_.close)

        Iterator((_numRows, bytes))
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

    VeloxBuildSideRelation(child.output, batches)
  }

  /**
   * Generate extended DataSourceV2 Strategy.
   * Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategy(spark: SparkSession): Strategy = {
    throw new UnsupportedOperationException(
      "Cannot support extending DataSourceV2 strategy for Velox backend.")
  }

  /**
   * Generate extended Analyzer.
   * Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzer(spark: SparkSession, conf: SQLConf): Rule[LogicalPlan] = {
    throw new UnsupportedOperationException(
      "Cannot support extending Analyzer for Velox backend.")
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_VELOX_BACKEND
}
