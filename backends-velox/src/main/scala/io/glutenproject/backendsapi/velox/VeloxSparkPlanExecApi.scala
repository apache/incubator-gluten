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

import scala.collection.mutable.ArrayBuffer

import io.glutenproject.backendsapi.{BackendsApiManager, ISparkPlanExecApi}
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.execution._
import io.glutenproject.expression.{AliasBaseTransformer, ArrowConverterUtils, VeloxAliasTransformer}
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.{ArrowWritableColumnVector, GlutenColumnarBatchSerializer}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.VeloxShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.VeloxColumnarRules._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, VeloxBuildSideRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.VeloxExecUtil
import org.apache.spark.sql.types.{Metadata, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class VeloxSparkPlanExecApi extends ISparkPlanExecApi {

  /**
   * Whether support gluten for current SparkPlan
   *
   * @return
   */
  override def supportedGluten(nativeEngineEnabled: Boolean, plan: SparkPlan): Boolean =
    nativeEngineEnabled

  /**
   * Generate NativeColumnarToRowExec.
   *
   * @param child
   * @return
   */
  override def genNativeColumnarToRowExec(child: SparkPlan): NativeColumnarToRowExec =
    new VeloxNativeColumnarToRowExec(child)

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): GlutenRowToColumnarExec =
    new VeloxRowToArrowColumnarExec(child)

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
      VeloxFilterExecTransformer(condition, child)
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
    VeloxHashAggregateExecTransformer(
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
    right: SparkPlan): ShuffledHashJoinExecTransformer =
    VeloxShuffledHashJoinExecTransformer(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right)

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
  : BroadcastHashJoinExecTransformer = VeloxBroadcastHashJoinExecTransformer(
    leftKeys, rightKeys, joinType, buildSide, condition, left, right, isNullAwareAntiJoin)

  /**
   * Generate Alias transformer.
   *
   * @param child The computation being performed
   * @param name  The name to be associated with the result of computing.
   * @param exprId
   * @param qualifier
   * @param explicitMetadata
   * @return a transformer for alias
   */
  def genAliasTransformer(child: Expression, name: String, exprId: ExprId,
    qualifier: Seq[String], explicitMetadata: Option[Metadata])
  : AliasBaseTransformer =
    new VeloxAliasTransformer(child, name)(exprId, qualifier, explicitMetadata)

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
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
    prepareTime: SQLMetric)
  : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
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
  override def genColumnarShuffleWriter[K, V](parameters: GenShuffleWriterParameters[K, V])
  : GlutenShuffleWriterWrapper[K, V] = {
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
    numOutputRows: SQLMetric,
    dataSize: SQLMetric): Serializer = {
    new GlutenColumnarBatchSerializer(schema, readBatchNumRows, numOutputRows)
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

    VeloxBuildSideRelation(mode, child.output, batches)
  }

  /**
   * Generate extended DataSourceV2 Strategy.
   * Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession =>
    Strategy] = List()

  /**
   * Generate extended Analyzer.
   * Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession =>
    Rule[LogicalPlan]] = List()

  /**
   * Generate extended columnar pre-rules.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedColumnarPreRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended columnar post-rules.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => OtherWritePostRule(spark), _ => LoadBeforeColumnarToRow())

  /**
   * Generate extended Strategy.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] = {
    List(_ => SimpleStrategy())
  }
}
