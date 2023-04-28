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
import io.glutenproject.expression.{AliasBaseTransformer, ExpressionTransformer, GetStructFieldTransformer, NamedStructTransformer, Sha1Transformer, Sha2Transformer}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, Expression, GetStructField, NamedExpression, Sha1, Sha2}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait SparkPlanExecApi {

  /**
   * Generate GlutenColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  def genColumnarToRowExec(child: SparkPlan): GlutenColumnarToRowExecBase

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  def genRowToColumnarExec(child: SparkPlan): GlutenRowToColumnarExec

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
  def genFilterExecTransformer(condition: Expression, child: SparkPlan): FilterExecBaseTransformer

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
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformer

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
      original: Expression): AliasBaseTransformer

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
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V]

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def createColumnarBatchSerializer(
      schema: StructType,
      readBatchNumRows: SQLMetric,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): Serializer

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

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]]

  /**
   * Generate extended Strategies. Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedStrategies(): List[SparkSession => Strategy]

  /**
   * Generate extended columnar pre-rules. Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedColumnarPreRules(): List[SparkSession => Rule[SparkPlan]]

  /**
   * Generate extended columnar post-rules. Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  /**
   * Generate an ExpressionTransformer to transform GetStructFiled expression.
   * GetStructFieldTransformer is the default implementation.
   */
  def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    new GetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  def genNamedStructTransformer(
      substraitExprName: String,
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    new NamedStructTransformer(substraitExprName, original, attributeSeq)
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

}
