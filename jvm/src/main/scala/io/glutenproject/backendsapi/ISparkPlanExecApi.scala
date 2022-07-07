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

import io.glutenproject.execution.{FilterExecBaseTransformer, HashAggregateExecBaseTransformer, NativeColumnarToRowExec, RowToArrowColumnarExec}
import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{SparkSession, Strategy}

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
   * Generate FilterExecTransformer.
   *
   * @param condition: the filter condition
   * @param child: the chid of FilterExec
   * @return the transformer of FilterExec
   */
  def genFilterExecTransformer(condition: Expression, child: SparkPlan)
    : FilterExecBaseTransformer

  /**
   * Generate HashAggregateExecTransformer.
   */
  def genHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan): HashAggregateExecBaseTransformer

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

  /**
   * Create broadcast relation for BroadcastExchangeExec
   */
  def createBroadcastRelation(
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation

  /**
   * Generate extended DataSourceV2 Strategy.
   * Currently only for ClickHouse backend.
   * @return
   */
  def genExtendedDataSourceV2Strategy(spark: SparkSession): Strategy

  /**
   * Generate extended Analyzer.
   * Currently only for ClickHouse backend.
   *
   * @return
   */
  def genExtendedAnalyzer(spark: SparkSession, conf: SQLConf): Rule[LogicalPlan]

  /**
   * Generate extended Strategy.
   * Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedStrategy(): Strategy

  /**
   * Generate extended Rule.
   * Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedRule(spark: SparkSession): ColumnarRule
}
