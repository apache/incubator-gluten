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

import io.glutenproject.backendsapi.SparkPlanExecApi
import io.glutenproject.execution._
import io.glutenproject.expression.{AliasBaseTransformer, AliasTransformer, ConverterUtils, ExpressionTransformer}
import io.glutenproject.expression.CHSha1Transformer
import io.glutenproject.expression.CHSha2Transformer
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.rel.RelBuilder
import io.glutenproject.vectorized.{CHBlockWriterJniWrapper, CHColumnarBatchSerializer}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.ColumnarToFakeRowStrategy
import org.apache.spark.sql.execution.datasources.GlutenColumnarRules.NativeWritePostRule
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

import java.util

import scala.collection.mutable.ArrayBuffer

class CHSparkPlanExecApi extends SparkPlanExecApi with Logging {

  /**
   * Generate GlutenColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): GlutenColumnarToRowExecBase = {
    CHColumnarToRowExec(child);
  }

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): GlutenRowToColumnarExec = {
    new RowToCHNativeColumnarExec(child)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the chid of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecBaseTransformer = {
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
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformer =
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

  /**
   * Generate Alias transformer.
   *
   * @param child
   *   : The computation being performed
   * @param name
   *   : The name to be associated with the result of computing.
   * @param exprId
   * @param qualifier
   * @param explicitMetadata
   * @return
   *   a transformer for alias
   */
  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): AliasBaseTransformer =
    new AliasTransformer(substraitExprName, child, original)

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
      readBatchNumRows: SQLMetric,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): Serializer = {
    new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows, dataSize)
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

        val newChild = child match {
          case wt: WholeStageTransformerExec =>
            wt.withNewChildren(
              Seq(ProjectExecTransformer(child.output ++ appendedProjections.toSeq, wt.child)))
          case w: WholeStageCodegenExec =>
            w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections.toSeq, w.child)))
          case c: CoalesceBatchesExec =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            WholeStageTransformerExec(
              ProjectExecTransformer(child.output ++ appendedProjections.toSeq, c))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
          case columnarAQEShuffleReadExec: ColumnarAQEShuffleReadExec =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            WholeStageTransformerExec(
              ProjectExecTransformer(
                child.output ++ appendedProjections.toSeq,
                columnarAQEShuffleReadExec))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
          case r2c: RowToCHNativeColumnarExec =>
            WholeStageTransformerExec(
              ProjectExecTransformer(child.output ++ appendedProjections.toSeq, r2c))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet()
            )
        }
        (
          newChild,
          (child.output ++ appendedProjections.toSeq).map(_.toAttribute),
          preProjectionBuildKeys)
      }
    val countsAndBytes = newChild
      .executeColumnar()
      .mapPartitions {
        iter =>
          var _numRows: Long = 0

          // Use for reading bytes array from block
          val blockNativeWriter = new CHBlockWriterJniWrapper()
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
    ClickHouseBuildSideRelation(mode, newOutput, batches, newBuildKeys)
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
    List(ColumnarToFakeRowStrategy)

  /** Generate an ExpressionTransformer to transform Sha2 expression. */
  override def genSha2Transformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Sha2): ExpressionTransformer = {
    new CHSha2Transformer(substraitExprName, left, right, original)
  }

  /** Generate an ExpressionTransformer to transform Sha1 expression. */
  override def genSha1Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Sha1): ExpressionTransformer = {
    new CHSha1Transformer(substraitExprName, child, original)
  }

  override def genOutputSchema(
      plan: SparkPlan): (util.ArrayList[TypeNode], util.ArrayList[String]) = {
    def genOutputSchemaFromOutputAtrributes(
        attrs: Seq[Attribute]): (util.ArrayList[TypeNode], util.ArrayList[String]) = {
      val typeNodes = new util.ArrayList[TypeNode]()
      val names = new util.ArrayList[String]()
      attrs.foreach {
        attr =>
          typeNodes.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
          names.add(ConverterUtils.genColumnNameWithExprId(attr))
          names.addAll(RelBuilder.collectStructFieldNamesDFS(attr.dataType))
      }
      (typeNodes, names)
    }

    /** The output scheme for the aggregate operator is special. */
    def genOutputSchemeForAggregate(
        agg: CHHashAggregateExecTransformer): (util.ArrayList[TypeNode], util.ArrayList[String]) = {
      val distinctAggregateFunctionModes = agg.aggregateExpressions.map(_.mode).distinct
      if (
        distinctAggregateFunctionModes.contains(Final) && (distinctAggregateFunctionModes.contains(
          Partial) ||
          distinctAggregateFunctionModes.contains(PartialMerge))
      ) {
        throw new IllegalStateException("Aggregate Final co-exists with Partial or PartialMerge")
      }
      val typeNodes = new util.ArrayList[TypeNode]()
      val names = new util.ArrayList[String]()
      // The output result schema of final stage and partial stage are different.
      // Final stage: the output result schema is the same as the select clause.
      // Partial stage: the output result schema is, the grouping keys + the aggregate expressions.
      // For example, "select avg(n_nationkey), n_regionkey, sum(n_nationkey ) from nation group by
      // n_regionkey" .the output result schema of final stage is: avg(n_nationkey), n_regionkey,
      // sum(n_nationkey ), but the output result schema of partial stage is: n_regionkey,
      // partial_avg(n_nationkey), partial_sum(n_nationkey)
      //
      // How to know whether it is final stage or partial stage?
      // 1. If the aggregateExpressions contains Final mode, it is final stage.
      // 2. If the aggregateExpressions is empty, use the output as schema anyway.
      if (distinctAggregateFunctionModes.contains(Final) || agg.aggregateExpressions.isEmpty) {
        // Final aggregage stage
        genOutputSchemaFromOutputAtrributes(agg.output)
      } else {
        // Partial aggregage stage

        // add grouping keys
        agg.groupingExpressions.foreach(
          expr => {
            val attr = ConverterUtils.getAttrFromExpr(expr).toAttribute
            val columnName = ConverterUtils.genColumnNameWithExprId(attr)
            names.add(columnName)
            val (dataType, nullable) =
              agg.getColumnType(columnName, attr, agg.aggregateExpressions)
            typeNodes.add(ConverterUtils.getTypeNode(dataType, nullable))
            names.addAll(RelBuilder.collectStructFieldNamesDFS(dataType))
          })
        // add aggregate expressions
        // Special cases:
        // 1. avg, the partial result is two columns in spark, sum and count. but in clickhouse, it
        //    has only one column avg.
        agg.aggregateExpressions.foreach(
          expr => {
            val attr = expr.resultAttribute
            val columnName = expr.mode match {
              case Partial | PartialMerge => agg.genPartialAggregateResultColumnName(attr)
              case _ => ConverterUtils.genColumnNameWithExprId(attr)
            }
            names.add(columnName)
            val (dataType, nullable) =
              agg.getColumnType(columnName, attr, agg.aggregateExpressions)
            typeNodes.add(ConverterUtils.getTypeNode(dataType, nullable))
            names.addAll(RelBuilder.collectStructFieldNamesDFS(dataType))
          })
        (typeNodes, names)
      }
    }

    // Try to find the aggregate node for generating proper output schema.
    // otherwise, use the output schema of the plan directly.
    plan match {
      case shuffle: ColumnarShuffleExchangeExec =>
        if (shuffle.projectOutputAttributes != null) {
          // This is would not be the case that shuffle for aggregate operator.
          val typeNodes = new util.ArrayList[TypeNode]()
          val names = new util.ArrayList[String]()
          shuffle.projectOutputAttributes.foreach {
            attr =>
              val typeNode = ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
              typeNodes.add(typeNode)
              names.add(ConverterUtils.genColumnNameWithExprId(attr))
              names.addAll(RelBuilder.collectStructFieldNamesDFS(attr.dataType))
          }
          (typeNodes, names)
        } else {
          genOutputSchema(shuffle.child)
        }
      case aqeShuffle: ColumnarAQEShuffleReadExec =>
        genOutputSchema(aqeShuffle.child)
      case wholeStage: WholeStageTransformerExec =>
        genOutputSchema(wholeStage.child)
      case inputAdapter: InputAdapter =>
        genOutputSchema(inputAdapter.child)
      case hashAgg: CHHashAggregateExecTransformer =>
        genOutputSchemeForAggregate(hashAgg)
      case coalesce: CoalesceBatchesExec =>
        genOutputSchema(coalesce.child)
      case shuffleStage: ShuffleQueryStageExec =>
        // enable adaptive execution
        genOutputSchema(shuffleStage.plan)
      case adpative: AdaptiveSparkPlanExec =>
        genOutputSchema(adpative.executedPlan)
      case _ =>
        genOutputSchemaFromOutputAtrributes(plan.output)
    }
  }
}
