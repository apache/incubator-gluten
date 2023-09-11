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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.SparkPlanExecApi
import io.glutenproject.execution._
import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import io.glutenproject.utils.CHJoinValidateUtil
import io.glutenproject.vectorized.CHColumnarBatchSerializer

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules.NativeWritePostRule
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.ClickHouseScan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.extension.ClickHouseAnalysis
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import org.apache.commons.lang3.ClassUtils

import java.{lang, util}

import scala.collection.mutable.ArrayBuffer

class CHSparkPlanExecApi extends SparkPlanExecApi {

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemExpressionNode(
      substraitExprName: String,
      functionMap: java.util.HashMap[String, java.lang.Long],
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      original: GetArrayItem): ExpressionNode = {
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(original.left.dataType, original.right.dataType),
      FunctionConfig.OPT)
    val exprNodes = Lists.newArrayList(leftNode, rightNode)
    ExpressionBuilder.makeScalarFunction(
      ExpressionBuilder.newScalarFunction(functionMap, functionName),
      exprNodes,
      ConverterUtils.getTypeNode(original.dataType, original.nullable))
  }

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase = {
    CHColumnarToRowExec(child)
  }

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase = {
    RowToCHNativeColumnarExec(child)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    child match {
      case scan: FileSourceScanExec if scan.relation.location.isInstanceOf[ClickHouseFileIndex] =>
        CHFilterExecTransformer(condition, child)
      case scan: BatchScanExec if scan.batch.isInstanceOf[ClickHouseScan] =>
        CHFilterExecTransformer(condition, child)
      case _ =>
        FilterExecTransformer(condition, child)
    }
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    CHHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    CHShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformer =
    CHBroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer =
    new GetMapValueTransformer(substraitExprName, left, right, original.failOnError, original)

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]
  ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      projectOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics
    )
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric]): Serializer = {
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val numOutputRows = metrics("numOutputRows")
    val dataSize = metrics("dataSize")
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CHCelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[SQLMetric], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(readBatchNumRows, numOutputRows, dataSize).asInstanceOf[Serializer]
    } else {
      new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows, dataSize)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val hashedRelationBroadcastMode = mode.asInstanceOf[HashedRelationBroadcastMode]
    val (newChild, newOutput, newBuildKeys) =
      if (
        hashedRelationBroadcastMode.key
          .forall(k => k.isInstanceOf[AttributeReference] || k.isInstanceOf[BoundReference])
      ) {
        (child, child.output, Seq.empty[Expression])
      } else {
        // pre projection in case of expression join keys
        val buildKeys = hashedRelationBroadcastMode.key
        val appendedProjections = new ArrayBuffer[NamedExpression]()
        val preProjectionBuildKeys = buildKeys.zipWithIndex.map {
          case (e, idx) =>
            e match {
              case b: BoundReference => child.output(b.ordinal)
              case o: Expression =>
                val newExpr = Alias(o, "col_" + idx)()
                appendedProjections += newExpr
                newExpr
            }
        }

        def wrapChild(child: SparkPlan): WholeStageTransformer = {
          WholeStageTransformer(ProjectExecTransformer(child.output ++ appendedProjections, child))(
            ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet()
          )
        }

        val newChild = child match {
          case wt: WholeStageTransformer =>
            wt.withNewChildren(
              Seq(ProjectExecTransformer(child.output ++ appendedProjections, wt.child)))
          case w: WholeStageCodegenExec =>
            w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections, w.child)))
          case columnarAQEShuffleReadExec: ColumnarAQEShuffleReadExec =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            wrapChild(columnarAQEShuffleReadExec)
          case r2c: RowToCHNativeColumnarExec =>
            wrapChild(r2c)
          case union: UnionExecTransformer =>
            wrapChild(union)
          case other =>
            throw new UnsupportedOperationException(
              s"Not supported operator ${other.nodeName} for BroadcastRelation")
        }
        (newChild, (child.output ++ appendedProjections).map(_.toAttribute), preProjectionBuildKeys)
      }
    val countsAndBytes =
      CHExecUtil.buildSideRDD(dataSize, newChild).collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += countsAndBytes.map(_._1).sum
    ClickHouseBuildSideRelation(mode, newOutput, batches.flatten, newBuildKeys)
  }

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = {
    List.empty
  }

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(spark => new ClickHouseAnalysis(spark, spark.sessionState.conf))
  }

  /**
   * Generate extended Optimizers.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] = {
    List.empty
  }

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
    List(spark => NativeWritePostRule(spark))

  /**
   * Generate extended Strategies.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] =
    List()

  override def genEqualNullSafeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: EqualNullSafe): ExpressionTransformer = {
    CHEqualNullSafeTransformer(substraitExprName, left, right, original)
  }

  override def genStringLocateTransformer(
      substraitExprName: String,
      first: ExpressionTransformer,
      second: ExpressionTransformer,
      third: ExpressionTransformer,
      original: StringLocate): ExpressionTransformer = {
    CHStringLocateTransformer(substraitExprName, first, second, third, original)
  }

  override def genMd5Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Md5): ExpressionTransformer = {
    CHMd5Transformer(substraitExprName, child, original)
  }

  /** Generate an ExpressionTransformer to transform Sha2 expression. */
  override def genSha2Transformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Sha2): ExpressionTransformer = {
    CHSha2Transformer(substraitExprName, left, right, original)
  }

  /** Generate an ExpressionTransformer to transform Sha1 expression. */
  override def genSha1Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Sha1): ExpressionTransformer = {
    CHSha1Transformer(substraitExprName, child, original)
  }

  override def genSizeExpressionTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Size): ExpressionTransformer = {
    CHSizeExpressionTransformer(substraitExprName, child, original)
  }

  /** Generate an ExpressionTransformer to transform TruncTimestamp expression for CH. */
  override def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String],
      original: TruncTimestamp): ExpressionTransformer = {
    CHTruncTimestampTransformer(substraitExprName, format, timestamp, timeZoneId, original)
  }

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  override def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      condition: Option[Expression]): Boolean = {
    CHJoinValidateUtil.shouldFallback(JoinType, leftOutputSet, rightOutputSet, condition)
  }

  /** Generate window function node */
  override def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: util.ArrayList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      args: util.HashMap[String, lang.Long]): Unit = {

    windowExpression.map {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = s"${aliasExpr.name}_${aliasExpr.exprId.id}"
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
            val aggWindowFunc = wf.asInstanceOf[AggregateWindowFunction]
            val frame = aggWindowFunc.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, aggWindowFunc).toInt,
              new util.ArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(aggWindowFunc.dataType, aggWindowFunc.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val aggregateFunc = aggExpression.aggregateFunction
            val substraitAggFuncName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
            if (substraitAggFuncName.isEmpty) {
              throw new UnsupportedOperationException(s"Not currently supported: $aggregateFunc.")
            }

            val childrenNodeList = new util.ArrayList[ExpressionNode]()
            aggregateFunc.children.foreach(
              expr =>
                childrenNodeList.add(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(expr, originalInputAttributes)
                    .doTransform(args)))

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(args, aggExpression.aggregateFunction).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ (Lead(_, _, _, _) | Lag(_, _, _, _)) =>
            val (offsetWf, frame) = wf match {
              case lead @ Lead(input, offset, default, ignoreNulls) =>
                // When the offset value of the lead is negative, will convert to lag function
                lead.offset match {
                  case IntegerLiteral(value) if value < 0 =>
                    val newWf = Lag(input, Literal(math.abs(value)), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lead, lead.frame.asInstanceOf[SpecifiedWindowFrame])
                }
              case lag @ Lag(input, offset, default, ignoreNulls) =>
                // When the offset value of the lag is negative, will convert to lead function
                lag.offset match {
                  case IntegerLiteral(value) if value > 0 =>
                    val newWf = Lead(input, Literal(value), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lag, lag.frame.asInstanceOf[SpecifiedWindowFrame])
                }
            }

            val childrenNodeList = new util.ArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.offset,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.default,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, offsetWf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(offsetWf.dataType, offsetWf.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case _ =>
            throw new UnsupportedOperationException(
              "unsupported window function type: " +
                wExpression.windowFunction)
        }
    }
  }
}
