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

package io.glutenproject.execution

import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder, RelNode}
import io.substrait.proto.JoinRel
import java.util
import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BaseJoinExec, HashJoin, ShuffledJoin}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    projectList: Seq[NamedExpression] = null)
    extends BaseJoinExec
    with TransformSupport
    with ShuffledJoin {

  val sparkConf = sparkContext.getConf

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"))

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  val (buildKeyExprs, streamedKeyExprs) = {
    require(
      leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys)
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  override def supportsColumnar: Boolean = true

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val getInputRDDs = (plan: SparkPlan) => {
      plan match {
        case c: TransformSupport =>
          c.columnarInputRDDs
        case _ =>
          Seq(plan.executeColumnar())
      }
    }
    getInputRDDs(streamedPlan) ++ getInputRDDs(buildPlan)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = buildPlan match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def getChild: SparkPlan = streamedPlan

  // TODO: enable doValidate
  override def doValidate(): Boolean = false

  override def doTransform(context: SubstraitContext): TransformContext = {
    val (joinRel: RelNode, buildOutputs: Seq[Attribute], streamedOutputs: Seq[Attribute]) =
      (buildPlan, streamedPlan) match {
        case (build: TransformSupport, streamed: TransformSupport) =>
          val streamedContext = streamed.doTransform(context)
          val buildContext = build.doTransform(context)
          (
            getJoinRel(streamedContext.root, buildContext.root, context),
            buildContext.outputAttributes,
            streamedContext.outputAttributes)

        case (build: TransformSupport, _) =>
          val streamedReadRel =
            RelBuilder.makeReadRel(
              new util.ArrayList[Attribute](streamedPlan.output.asJava),
              context)
          val buildContext = build.doTransform(context)
          (
            getJoinRel(streamedReadRel, buildContext.root, context),
            buildContext.outputAttributes,
            streamedPlan.output)

        case (_, streamed: TransformSupport) =>
          val streamedContext = streamed.doTransform(context)
          val buildReadRel = RelBuilder.makeReadRel(
            new util.ArrayList[Attribute](buildPlan.output.asJava),
            context)
          (
            getJoinRel(streamedContext.root, buildReadRel, context),
            buildPlan.output,
            streamedContext.outputAttributes)

        case (_, _) =>
          val streamedReadRel =
            RelBuilder.makeReadRel(
              new util.ArrayList[Attribute](streamedPlan.output.asJava),
              context)
          val buildReadRel = RelBuilder.makeReadRel(
            new util.ArrayList[Attribute](buildPlan.output.asJava),
            context)
          (
            getJoinRel(streamedReadRel, buildReadRel, context),
            buildPlan.output,
            streamedPlan.output)
      }

    val (rel, inputAttributes) = buildSide match {
      case BuildLeft =>
        val reorderedOutput = buildPlan.output.indices.map(idx =>
          ExpressionBuilder.makeSelection(idx + streamedPlan.output.size)) ++
          streamedPlan.output.indices
            .map(ExpressionBuilder.makeSelection(_))
        (
          RelBuilder.makeProjectRel(
            joinRel,
            new java.util.ArrayList[ExpressionNode](reorderedOutput.asJava)),
          buildOutputs ++ streamedOutputs)
      case BuildRight => (joinRel, streamedOutputs ++ buildOutputs)
    }

    TransformContext(inputAttributes, output, rel)
  }

  override def output: Seq[Attribute] =
    if (projectList == null || projectList.isEmpty) super.output
    else projectList.map(_.toAttribute)

  private def getJoinRel(left: RelNode, right: RelNode, context: SubstraitContext): RelNode = {
    val substraitJoinType = joinType match {
      case Inner =>
        JoinRel.JoinType.JOIN_TYPE_INNER
      case _ =>
        JoinRel.JoinType.UNRECOGNIZED
    }

    val joinExpression = (streamedKeyExprs zip buildKeyExprs)
      .map { case (l, r) => EqualTo(l, r) }
      .reduce(And)
    val joinExpressionNode = ExpressionConverter
      .replaceWithExpressionTransformer(joinExpression, output)
      .asInstanceOf[ExpressionTransformer]
      .doTransform(context.registeredFunction)

    val postJoinFilter = condition.map { expr =>
      ExpressionConverter
        .replaceWithExpressionTransformer(expr, output)
        .asInstanceOf[ExpressionTransformer]
        .doTransform(context.registeredFunction)
    }

    RelBuilder.makeJoinRel(
      left,
      right,
      substraitJoinType,
      joinExpressionNode,
      postJoinFilter.orNull)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }
}
