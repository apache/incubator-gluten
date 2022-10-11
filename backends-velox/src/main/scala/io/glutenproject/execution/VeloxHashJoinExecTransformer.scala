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

import io.glutenproject.execution.HashJoinLikeExecTransformer.{makeAndExpression, makeEqualToExpression, makeIsNullExpression}
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.substrait.proto.JoinRel
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.types.{BooleanType, DataType}
import java.util

import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

import scala.collection.JavaConverters._

trait VeloxHashJoinLikeExecTransformer extends HashJoinLikeExecTransformer {

  // Direct output order of substrait join operation
  override protected val substraitJoinType: JoinRel.JoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      if (!antiJoinWorkaroundNeeded) {
        JoinRel.JoinType.JOIN_TYPE_ANTI
      } else {
        // Use Left to replace Anti as a workaround.
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  /**
   * Use a workaround for Anti join with 'not exists' semantics.
   * Firstly, add an aggregation rel over build rel to make the build keys distinct.
   * Secondly, add a project rel over the aggregation rel to append a column of true constant.
   * Then, this project rel is returned and will be used as the input rel for join build side.
   * @param buildInfo: the original build keys, build rel and build outputs.
   * @param context: Substrait context.
   * @param operatorId: operator id of this join.
   * @return original build keys, new build rel and build outputs.
   */
  private def createSpecialRelForAntiBuild(
      buildInfo: (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]),
      context: SubstraitContext,
      operatorId: java.lang.Long,
      validation: Boolean): (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    // Create an Aggregation Rel over build Rel.
    val groupingNodes = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    buildInfo._3.foreach(_ => {
      groupingNodes.add(ExpressionBuilder.makeSelection(colIdx))
      colIdx += 1
    })
    val aggNode = RelBuilder.makeAggregateRel(
      buildInfo._2,
      groupingNodes,
      new util.ArrayList[AggregateFunctionNode](),
      context,
      operatorId)
    // Create a Project Rel over Aggregation Rel.
    val expressionNodes = groupingNodes
    // Append a new column of true constant.
    expressionNodes.add(ExpressionBuilder.makeBooleanLiteral(true))
    val projectNode = RelBuilder.makeProjectRel(
      aggNode,
      expressionNodes,
      createExtensionNode(buildInfo._3, validation),
      context,
      operatorId)
    (
      buildInfo._1,
      projectNode,
      buildInfo._3 :+ AttributeReference(s"constant_true", BooleanType)()
    )
  }

  override def createJoinRel(inputStreamedRelNode: RelNode,
                             inputBuildRelNode: RelNode,
                             inputStreamedOutput: Seq[Attribute],
                             inputBuildOutput: Seq[Attribute],
                             substraitContext: SubstraitContext,
                             operatorId: java.lang.Long,
                             validation: Boolean = false): RelNode = {
    // Create pre-projection for build/streamed plan. Append projected keys to each side.
    val (streamedKeys, streamedRelNode, streamedOutput) = createPreProjectionIfNeeded(
      streamedKeyExprs,
      inputStreamedRelNode,
      inputStreamedOutput,
      inputStreamedOutput,
      substraitContext,
      operatorId,
      validation)

    val (buildKeys, buildRelNode, buildOutput) = {
      val (keys, relNode, output) = createPreProjectionIfNeeded(
        buildKeyExprs,
        inputBuildRelNode,
        inputBuildOutput,
        streamedOutput ++ inputBuildOutput,
        substraitContext,
        operatorId,
        validation)
      if (!antiJoinWorkaroundNeeded) {
        (keys, relNode, output)
      } else {
        // Use a workaround for Anti join.
        createSpecialRelForAntiBuild(
          (keys, relNode, output), substraitContext, operatorId, validation)
      }
    }

    // Combine join keys to make a single expression.
    val joinExpressionNode = (streamedKeys zip buildKeys).map {
      case ((leftKey, leftType), (rightKey, rightType)) =>
        makeEqualToExpression(
          leftKey, leftType, rightKey, rightType, substraitContext.registeredFunction)
    }.reduce((l, r) => makeAndExpression(l, r, substraitContext.registeredFunction))

    // Create post-join filter, which will be computed in hash join.
    val postJoinFilter = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, streamedOutput ++ buildOutput)
          .asInstanceOf[ExpressionTransformer]
          .doTransform(substraitContext.registeredFunction)
    }

    // Create JoinRel.
    val joinRel = {
      val joinNode = RelBuilder.makeJoinRel(
        streamedRelNode,
        buildRelNode,
        substraitJoinType,
        joinExpressionNode,
        postJoinFilter.orNull,
        createJoinExtensionNode(streamedOutput ++ buildOutput, validation),
        substraitContext,
        operatorId)
      if (!antiJoinWorkaroundNeeded) {
        joinNode
      } else {
        // Use an isNulll filter to select the rows needed by Anti join from Left join outputs.
        val isNullFilter = makeIsNullExpression(
          ExpressionBuilder.makeSelection(streamedOutput.size + buildOutput.size - 1),
          substraitContext.registeredFunction)
        RelBuilder.makeFilterRel(
          joinNode,
          isNullFilter,
          createJoinExtensionNode(streamedOutput ++ buildOutput, validation),
          substraitContext,
          operatorId)
      }
    }

    // Result projection will drop the appended keys, and exchange columns order if BuildLeft.
    val resultProjection = if (exchangeTable) {
      val (leftOutput, rightOutput) =
        getResultProjectionOutput(inputBuildOutput, inputStreamedOutput)
      // Exchange the order of build and streamed.
      leftOutput.indices.map(idx =>
        ExpressionBuilder.makeSelection(idx + streamedOutput.size)) ++
        rightOutput.indices
          .map(ExpressionBuilder.makeSelection(_))
    } else {
      val (leftOutput, rightOutput) =
        getResultProjectionOutput(inputStreamedOutput, inputBuildOutput)
      leftOutput.indices.map(ExpressionBuilder.makeSelection(_)) ++
        rightOutput.indices.map(idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size))
    }

    RelBuilder.makeProjectRel(
      joinRel,
      new java.util.ArrayList[ExpressionNode](resultProjection.asJava),
      createExtensionNode(streamedOutput ++ buildOutput, validation),
      substraitContext,
      operatorId)
  }
}

case class VeloxShuffledHashJoinExecTransformer(leftKeys: Seq[Expression],
                                                rightKeys: Seq[Expression],
                                                joinType: JoinType,
                                                buildSide: BuildSide,
                                                condition: Option[Expression],
                                                left: SparkPlan,
                                                right: SparkPlan)
  extends ShuffledHashJoinExecTransformer(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right) with VeloxHashJoinLikeExecTransformer {

  // Used to specify the preferred build side in backend's real execution.
  object PreferredBuildSide extends Serializable {
    val LEFT = "left table"
    val RIGHT = "right table"
    val NON = "none"
  }

  private def matchCondition(plan: SparkPlan): Boolean =
    plan.isInstanceOf[FilterExecBaseTransformer] || plan.isInstanceOf[FilterExec] ||
    plan.isInstanceOf[BaseAggregateExec]

  private def isPreferred(plan: SparkPlan): Boolean =
    matchCondition(plan) || plan.children.exists(child => matchCondition(child))

  private lazy val preferredBuildSide: String =
    if ((isPreferred(left) && isPreferred(right)) || (!isPreferred(left) && !isPreferred(right))) {
      PreferredBuildSide.NON
    } else if (isPreferred(left)) {
      PreferredBuildSide.LEFT
    } else {
      PreferredBuildSide.RIGHT
    }

  override lazy val exchangeTable: Boolean = hashJoinType match {
    case LeftOuter | LeftSemi => joinBuildSide match {
      case BuildLeft =>
        // Exchange build and stream side when left side or none is preferred as the build side,
        // and RightOuter or RightSemi wil be used.
        !(preferredBuildSide == PreferredBuildSide.RIGHT)
      case _ =>
        // Do not exchange build and stream side when right side or none is preferred
        // as the build side, and LeftOuter or LeftSemi wil be used.
        preferredBuildSide == PreferredBuildSide.LEFT
    }
    case RightOuter => joinBuildSide match {
      case BuildRight =>
        // Do not exchange build and stream side when right side or none is preferred
        // as the build side, and RightOuter will be used.
        preferredBuildSide == PreferredBuildSide.LEFT
      case _ =>
        // Exchange build and stream side when left side or none is preferred as the build side,
        // and LeftOuter will be used.
        !(preferredBuildSide == PreferredBuildSide.RIGHT)
    }
    case _ => joinBuildSide match {
      case BuildLeft => true
      case BuildRight => false
    }
  }

  // Direct output order of Substrait join operation.
  override protected val substraitJoinType: JoinRel.JoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter => joinBuildSide match {
      case BuildLeft => if (preferredBuildSide == PreferredBuildSide.RIGHT) {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      }
      case _ => if (preferredBuildSide == PreferredBuildSide.LEFT) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    }
    case RightOuter => joinBuildSide match {
      case BuildRight => if (preferredBuildSide == PreferredBuildSide.LEFT) {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      }
      case _ => if (preferredBuildSide == PreferredBuildSide.RIGHT) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    }
    case LeftSemi => joinBuildSide match {
      case BuildLeft => if (preferredBuildSide == PreferredBuildSide.RIGHT) {
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
      }
      case _ => if (preferredBuildSide == PreferredBuildSide.LEFT) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      }
    }
    case LeftAnti => if (!antiJoinWorkaroundNeeded) {
      JoinRel.JoinType.JOIN_TYPE_ANTI
    } else {
      // Use Left to replace Anti as a workaround.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    }
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  /**
   * Returns whether a workaround for Anti join is needed. True for 'not exists' semantics.
   * For SHJ, always returns true for Anti join.
   */
  override def antiJoinWorkaroundNeeded: Boolean = {
    joinType match {
      case LeftAnti => true
      case _ => false
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): VeloxShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}

case class VeloxBroadcastHashJoinExecTransformer(leftKeys: Seq[Expression],
                                                 rightKeys: Seq[Expression],
                                                 joinType: JoinType,
                                                 buildSide: BuildSide,
                                                 condition: Option[Expression],
                                                 left: SparkPlan,
                                                 right: SparkPlan,
                                                 isNullAwareAntiJoin: Boolean = false)
  extends BroadcastHashJoinExecTransformer(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right) with VeloxHashJoinLikeExecTransformer {

  /**
   * Returns whether a workaround for Anti join is needed. True for 'not exists' semantics.
   * For BHJ, only when isNullAwareAntiJoin is disabled, true is returned.
   */
  override def antiJoinWorkaroundNeeded: Boolean = {
    joinType match {
      case LeftAnti =>
        if (isNullAwareAntiJoin) {
          false
        } else {
          // Velox's Anti semantics are matched with the case when isNullAwareAntiJoin is enabled.
          // So a workaround is needed if isNullAwareAntiJoin is disabled.
          true
        }
      case _ =>
        false
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): VeloxBroadcastHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}
