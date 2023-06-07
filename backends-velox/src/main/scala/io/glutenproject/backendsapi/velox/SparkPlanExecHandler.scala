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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.SparkPlanExecApi
import io.glutenproject.columnarbatch.GlutenColumnarBatches
import io.glutenproject.execution._
import io.glutenproject.execution.ColumnarRules.LoadBeforeColumnarToRow
import io.glutenproject.expression._
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializerJniWrapper}
import org.apache.commons.lang3.ClassUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.AggregateFunctionRewriteRule
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression, StringTrim}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HLLAdapter}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.datasources.ColumnarToFakeRowStrategy
import org.apache.spark.sql.execution.datasources.GlutenColumnarRules.NativeWritePostRule
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.execution.{ColumnarBuildSideRelation, SparkPlan, VeloxColumnarToRowExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.{ShuffleDependency, SparkException}

class SparkPlanExecHandler extends SparkPlanExecApi {

  /** * Plans. */

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase =
    VeloxColumnarToRowExec(child)

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase =
    RowToArrowColumnarExec(child)

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition : the filter condition
   * @param child     : the chid of FilterExec
   * @return the transformer of FilterExec
   */
  override def genFilterExecTransformer(condition: Expression, child: SparkPlan)
  : FilterExecBaseTransformer = FilterExecTransformer(condition, child)

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
    HashAggregateExecTransformer(
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
  override def genShuffledHashJoinExecTransformer(leftKeys: Seq[Expression],
                                                  rightKeys: Seq[Expression],
                                                  joinType: JoinType,
                                                  buildSide: BuildSide,
                                                  condition: Option[Expression],
                                                  left: SparkPlan,
                                                  right: SparkPlan,
                                                  isSkewJoin: Boolean)
  : ShuffledHashJoinExecTransformerBase =
    ShuffledHashJoinExecTransformer(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkewJoin)

  /**
   * Generate BroadcastHashJoinExecTransformer.
   */
  override def genBroadcastHashJoinExecTransformer(leftKeys: Seq[Expression],
                                                   rightKeys: Seq[Expression],
                                                   joinType: JoinType,
                                                   buildSide: BuildSide,
                                                   condition: Option[Expression],
                                                   left: SparkPlan,
                                                   right: SparkPlan,
                                                   isNullAwareAntiJoin: Boolean = false)
  : BroadcastHashJoinExecTransformer = GlutenBroadcastHashJoinExecTransformer(
    leftKeys, rightKeys, joinType, buildSide, condition, left, right, isNullAwareAntiJoin)

  override def genHashExpressionTransformer(substraitExprName: String,
                                            exps: Seq[ExpressionTransformer],
                                            original: Expression): ExpressionTransformer = {
    HashExpressionTransformer(substraitExprName, exps, original)
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
    ExecUtil.genShuffleDependency(
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
    ShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(schema: StructType,
                                             readBatchNumRows: SQLMetric,
                                             numOutputRows: SQLMetric,
                                             dataSize: SQLMetric): Serializer = {
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CelebornColumnarBatchSerializer")
      val constructor = clazz.getConstructor(classOf[StructType],
        classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(schema, readBatchNumRows, numOutputRows).asInstanceOf[Serializer]
    } else {
      new ColumnarBatchSerializer(schema, readBatchNumRows, numOutputRows)
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
        val input = new ArrayBuffer[Long]()
        // This iter is ClosableColumnarBatch, hasNext will remove it from native batch map,
        // and serialize function append(RowVector) may reserve the buffer,
        // so we can not release the batch before flush to OutputStream
        while (iter.hasNext) {
          val batch = iter.next
          if (batch.numCols() != 0) {
            val handle = GlutenColumnarBatches.getNativeHandle(batch)
            val newHandle = ColumnarBatchSerializerJniWrapper.INSTANCE.insertBatch(handle)
            input += newHandle
          }
        }

        if (input.isEmpty) {
          Iterator((0L, Array[Byte]()))
        } else {
          val serializeResult = ColumnarBatchSerializerJniWrapper.INSTANCE.serialize(input.toArray,
            NativeMemoryAllocators.contextInstance().getNativeInstanceId)
          ColumnarBatchSerializerJniWrapper.INSTANCE.closeBatches(input.toArray)
          Iterator((serializeResult.getNumRows, serializeResult.getSerialized))
        }
      }
      .collect

    val batches = countsAndBytes.filter(_._1 != 0).map(_._2)
    val rawSize = batches.map(_.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += countsAndBytes.map(_._1).sum
    dataSize += rawSize

    ColumnarBuildSideRelation(mode, child.output, batches)
  }

  /** * Expressions. */

  /**
   * Generate Alias transformer.
   *
   * @return a transformer for alias
   */
  override def genAliasTransformer(substraitExprName: String,
                                   child: ExpressionTransformer,
                                   original: Expression): AliasTransformerBase =
    new AliasTransformer(substraitExprName, child, original)

  /**
   * Generate an expression transformer to transform NamedStruct to Substrait.
   */
  override def genNamedStructTransformer(substraitExprName: String,
                                         original: CreateNamedStruct,
                                         attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    NamedStructTransformer(substraitExprName, original, attributeSeq)
  }


  /**
   * Generate an ExpressionTransformer to transform GetStructFiled expression.
   */
  override def genGetStructFieldTransformer(substraitExprName: String,
                                            childTransformer: ExpressionTransformer,
                                            ordinal: Int,
                                            original: GetStructField): ExpressionTransformer = {
    new GetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  /**
   * To align with spark in casting string type input to other types,
   * add trim node for trimming space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
      ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    c.dataType match {
      case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
        c
      case FloatType | DoubleType | _: DecimalType =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimSpaceStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
      case _ =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimWhitespaceStr +
              trimSpaceSepStr + trimLineSepStr + trimParaSepStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
    }
  }

  /** * Rules and strategies. */

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
   * Generate extended Optimizer.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] =
    List(AggregateFunctionRewriteRule)


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
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    (List(_ => LoadBeforeColumnarToRow()): List[SparkSession => Rule[SparkPlan]]) :::
      List(spark => NativeWritePostRule(spark))
  }

  /**
   * Generate extended Strategy.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] = {
    List(ColumnarToFakeRowStrategy)
  }

  /**
   * Define backend specfic expression mappings.
   */
  override def extraExpressionMappings: Seq[Sig] = {
    Seq(Sig[HLLAdapter](ExpressionNames.APPROX_DISTINCT))
  }
}
