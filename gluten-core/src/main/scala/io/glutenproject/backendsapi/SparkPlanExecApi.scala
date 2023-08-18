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

import io.glutenproject.execution._
import io.glutenproject.expression._
import io.glutenproject.substrait.expression.{ExpressionNode, WindowFunctionNode}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util

trait SparkPlanExecApi {

  /** Transform GetArrayItem to Substrait. */
  def genGetArrayItemExpressionNode(
      substraitExprName: String,
      functionMap: java.util.HashMap[String, java.lang.Long],
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      original: GetArrayItem): ExpressionNode

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase

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
  def genFilterExecTransformer(condition: Expression, child: SparkPlan): FilterExecTransformerBase

  def genHiveTableScanExecTransformer(plan: SparkPlan): HiveTableScanExecTransformer =
    HiveTableScanExecTransformer(plan)

  /** Generate HashAggregateExecTransformer. */
  def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformer

  /**
   * Generate Alias transformer.
   *
   * @param child
   *   The computation being performed
   * @param name
   *   The name to be associated with the result of computing.
   * @param exprId
   * @param qualifier
   * @param explicitMetadata
   * @return
   *   a transformer for alias
   */
  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): AliasTransformerBase

  /** Generate SplitTransformer. */
  def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): StringSplitTransformerBase = {
    StringSplitTransformerBase(substraitExprName, srcExpr, regexExpr, limitExpr, original)
  }

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * childOutputAttributes may be different from outputAttributes, for example, the
   * childOutputAttributes include additional shuffle key columns
   * @return
   */
  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]

  /** Generate ColumnarShuffleWriter for ColumnarShuffleManager. */
  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V]

  /** Generate ColumnarShuffleSerializer for ColumnarShuffleExchangeExec. */
  def createColumnarShuffleSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric]): Serializer

  /** Create broadcast relation for BroadcastExchangeExec */
  def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy]

  /** Generate extended Analyzers. Currently only for ClickHouse backend. */
  def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]]

  /** Generate extended Optimizers. Currently only for Velox backend. */
  def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]]

  /** Generate extended Strategies */
  def genExtendedStrategies(): List[SparkSession => Strategy]

  /** Generate extended columnar pre-rules. */
  def genExtendedColumnarPreRules(): List[SparkSession => Rule[SparkPlan]]

  /** Generate extended columnar post-rules. */
  def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    new GetStructFieldTransformerBase(substraitExprName, childTransformer, ordinal, original)
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  def genNamedStructTransformer(
      substraitExprName: String,
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer =
    new NamedStructTransformerBase(substraitExprName, original, attributeSeq)

  def genEqualNullSafeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: EqualNullSafe): ExpressionTransformer = {
    new BinaryExpressionTransformer(substraitExprName, left, right, original)
  }

  /**
   * Generate an ExpressionTransformer to transform Sha2 expression. Sha2Transformer is the default
   * implementation.
   */
  def genSha2Transformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Sha2): ExpressionTransformer = {
    new Sha2Transformer(substraitExprName, left, right, original)
  }

  /**
   * Generate an ExpressionTransformer to transform Sha1 expression. Sha1Transformer is the default
   * implementation.
   */
  def genSha1Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Sha1): ExpressionTransformer = {
    new Sha1Transformer(substraitExprName, child, original)
  }

  def genSizeExpressionTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Size): ExpressionTransformer = {
    new UnaryExpressionTransformer(substraitExprName, child, original)
  }

  /**
   * Generate an ExpressionTransformer to transform TruncTimestamp expression.
   * TruncTimestampTransformer is the default implementation.
   */
  def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String] = None,
      original: TruncTimestamp): ExpressionTransformer = {
    new TruncTimestampTransformer(substraitExprName, format, timestamp, timeZoneId, original)
  }

  def genCastWithNewChild(c: Cast): Cast = {
    c
  }

  def genHashExpressionTransformer(
      substraitExprName: String,
      exps: Seq[ExpressionTransformer],
      original: Expression): ExpressionTransformer = {
    new HashExpressionTransformerBase(substraitExprName, exps, original)
  }

  def genUnixTimestampTransformer(
      substraitExprName: String,
      timeExp: ExpressionTransformer,
      format: ExpressionTransformer,
      original: ToUnixTimestamp): ExpressionTransformer = {
    ToUnixTimestampTransformer(
      substraitExprName,
      timeExp,
      format,
      original.timeZoneId,
      original.failOnError,
      original)
  }

  /** Define backend specfic expression mappings. */
  def extraExpressionMappings: Seq[Sig] = Seq.empty

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      right: AttributeSet,
      condition: Option[Expression]): Boolean = false

  /** default function to generate window function node */
  def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: util.ArrayList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      args: util.HashMap[String, java.lang.Long]): Unit

  def genInjectedFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = Seq.empty

  def rewriteSpillPath(path: String): String = path
}
