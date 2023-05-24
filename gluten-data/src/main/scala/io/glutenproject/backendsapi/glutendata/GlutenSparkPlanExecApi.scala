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

package io.glutenproject.backendsapi.glutendata

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.{BackendsApiManager, SparkPlanExecApi}
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.execution.VeloxColumnarRules.LoadBeforeColumnarToRow
import io.glutenproject.execution._
import io.glutenproject.expression.{AliasBaseTransformer, ExpressionTransformer, GlutenAliasTransformer, GlutenHashExpressionTransformer}
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.GlutenArrowUtil
import io.glutenproject.vectorized.{ArrowWritableColumnVector, GlutenColumnarBatchSerializer}
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.utils.GlutenShuffleUtil
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.ColumnarToFakeRowStrategy
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.GlutenExecUtil
import org.apache.spark.sql.execution.{GlutenBuildSideRelation, SparkPlan, VeloxColumnarToRowExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.{ShuffleDependency, SparkException}

import scala.collection.mutable.ArrayBuffer

abstract class GlutenSparkPlanExecApi extends SparkPlanExecApi {

  /**
   * Generate GlutenColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): GlutenColumnarToRowExecBase =
    new VeloxColumnarToRowExec(child)

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): GlutenRowToColumnarExec =
    new GlutenRowToArrowColumnarExec(child)

  // scalastyle:off argcount

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition : the filter condition
   * @param child     : the chid of FilterExec
   * @return the transformer of FilterExec
   */
  override def genFilterExecTransformer(condition: Expression, child: SparkPlan)
  : FilterExecBaseTransformer =
    if (BackendsApiManager.getSettings.avoidOverwritingFilterTransformer()) {
      // Use the original Filter for Arrow backend.
      FilterExecTransformer(condition, child)
    } else {
      GlutenFilterExecTransformer(condition, child)
    }

  /**
   * Generate HashAggregateExecTransformer.
   */
  override def genHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan): HashAggregateExecBaseTransformer =
    GlutenHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /**
   * Generate ShuffledHashJoinExecTransformer.
   */
  def genShuffledHashJoinExecTransformer(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean): ShuffledHashJoinExecTransformer =
    GlutenShuffledHashJoinExecTransformer(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkewJoin)

  /**
   * Generate BroadcastHashJoinExecTransformer.
   */
  def genBroadcastHashJoinExecTransformer(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean = false)
  : BroadcastHashJoinExecTransformer = GlutenBroadcastHashJoinExecTransformer(
    leftKeys, rightKeys, joinType, buildSide, condition, left, right, isNullAwareAntiJoin)

  /**
   * Generate Alias transformer.
   *
   * @return a transformer for alias
   */
  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): AliasBaseTransformer =
    new GlutenAliasTransformer(substraitExprName, child, original)

  override def genHashExpressionTransformer(substraitExprName: String,
                                            exps: Seq[ExpressionTransformer],
                                            original: Expression): ExpressionTransformer = {
    GlutenHashExpressionTransformer(substraitExprName, exps, original)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(rdd: RDD[ColumnarBatch], childOutputAttributes: Seq[Attribute],
    projectOutputAttributes: Seq[Attribute],
    newPartitioning: Partitioning,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric],
    metrics: Map[String, SQLMetric])
  : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    GlutenExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics)
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](parameters: GenShuffleWriterParameters[K, V])
  : GlutenShuffleWriterWrapper[K, V] = {
    GlutenShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric): Serializer = {
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CelebornColumnarBatchSerializer")
      val constructor = clazz.getConstructor(classOf[StructType],
        classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(schema, readBatchNumRows, numOutputRows).asInstanceOf[Serializer]
    } else {
      new GlutenColumnarBatchSerializer(schema, readBatchNumRows, numOutputRows)
    }
  }

  /**
   * Create broadcast relation for BroadcastExchangeExec
   */
  override def createBroadcastRelation(mode: BroadcastMode,
    child: SparkPlan,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric): BuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions { iter =>
        var _numRows: Long = 0
        val _input = new ArrayBuffer[ColumnarBatch]()

        try {
          while (iter.hasNext) {
            val batch = iter.next
            val acb = ArrowColumnarBatches
              .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
            (0 until acb.numCols).foreach(i => {
              acb.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
            })
            _numRows += acb.numRows
            _input += acb
          }

          val bytes = GlutenArrowUtil.convertToNetty(_input.toArray)
          Iterator((_numRows, bytes))
        } finally {
          _input.foreach(_.close)
        }
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

    GlutenBuildSideRelation(mode, child.output, batches)
  }

  /**
   * Generate extended DataSourceV2 Strategy.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession =>
    Strategy] = List()

  /**
   * Generate extended Analyzer.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession =>
    Rule[LogicalPlan]] = List()

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarPreRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(_ => LoadBeforeColumnarToRow())

  /**
   * Generate extended Strategy.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] = {
    List(ColumnarToFakeRowStrategy)
  }
}
