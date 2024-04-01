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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.CrossRel

abstract class BroadcastNestedLoopJoinExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression])
  extends BaseJoinExec
  with TransformSupport {

  def joinBuildSide: BuildSide = buildSide

  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  private lazy val substraitJoinType: CrossRel.JoinType =
    SubstraitUtil.toCrossRelSubstrait(joinType)

  private lazy val buildTableId: String = "BuildTable-" + buildPlan.id

  // Hint substrait to switch the left and right,
  // since we assume always build right side in substrait.
  private lazy val needSwitchChildren: Boolean = buildSide match {
    case BuildLeft => true
    case BuildRight => false
  }

  protected lazy val (buildPlan, streamedPlan) = if (needSwitchChildren) {
    (left, right)
  } else {
    (right, left)
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcastRDD = {
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      BackendsApiManager.getBroadcastApiInstance
        .collectExecutionBroadcastTableId(executionId, buildTableId)
      createBroadcastBuildSideRDD()
    }
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  protected def createBroadcastBuildSideRDD(): BroadcastBuildSideRDD

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetricsUpdater(metrics)
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case x =>
        throw new IllegalArgumentException(s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike => right.outputPartitioning
        case RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"BroadcastNestedLoopJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike => left.outputPartitioning
        case LeftOuter => left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"BroadcastNestedLoopJoin should not take $x as the JoinType with building right side")
      }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val streamedPlanContext = streamedPlan.asInstanceOf[TransformSupport].doTransform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)

    val buildPlanContext = buildPlan.asInstanceOf[TransformSupport].doTransform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (buildPlanContext.root, buildPlanContext.outputAttributes)

    val operatorId = context.nextOperatorId(this.nodeName)

    val crossRel = JoinUtils.createCrossRel(
      substraitJoinType,
      condition,
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId
    )

    val projectRelPostJoinRel = JoinUtils.createProjectRelPostJoinRel(
      needSwitchChildren,
      joinType,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId,
      crossRel,
      inputStreamedOutput,
      inputBuildOutput
    )

    JoinUtils.createTransformContext(
      needSwitchChildren,
      output,
      projectRelPostJoinRel,
      inputStreamedOutput,
      inputBuildOutput)
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportBroadcastNestedLoopJoinExec()) {
      return ValidationResult.notOk("Broadcast Nested Loop join is not supported in this backend")
    }
    if (substraitJoinType == CrossRel.JoinType.UNRECOGNIZED) {
      return ValidationResult.notOk(s"$joinType join is not supported with BroadcastNestedLoopJoin")
    }
    (joinType, buildSide) match {
      case (LeftOuter, BuildLeft) | (RightOuter, BuildRight) =>
        return ValidationResult.notOk(s"$joinType join is not supported with $buildSide")
      case _ => // continue
    }
    val substraitContext = new SubstraitContext

    val crossRel = JoinUtils.createCrossRel(
      substraitJoinType,
      condition,
      null,
      null,
      streamedPlan.output,
      buildPlan.output,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName),
      validation = true
    )
    doNativeValidation(substraitContext, crossRel)
  }
}
