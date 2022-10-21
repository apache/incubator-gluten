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

package io.glutenproject.extension

import io.glutenproject.BackendLib

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{JoinHint, LogicalPlan, SHUFFLE_MERGE}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

class JoinSelectionOverrideShim(val backendLib: BackendLib) extends Strategy with JoinSelectionHelper with SQLConfHelper{

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = ???

  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def extractEqualJoinKeyCondition(joinType: JoinType,
                                   leftKeys: Seq[Expression], rightKeys: Seq[Expression],
                                   condition: Option[Expression],
                                   left: LogicalPlan, right: LogicalPlan,
                                   hint: JoinHint,
                                   forceShuffledHashJoin: Boolean): Seq[SparkPlan] = {
    if (isBroadcastStage(left) || isBroadcastStage(right)) {
      // equal condition
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(
        BroadcastHashJoinExec(
          leftKeys,
          rightKeys,
          joinType,
          buildSide,
          condition,
          planLater(left),
          planLater(right)))
    } else {
      // non equal condition
      // Generate BHJ here, avoid to do match in `JoinSelection` again.
      val buildSide = getBroadcastBuildSide(left, right, joinType, hint, hintOnly = false, conf)
      if (buildSide.isDefined) {
        return Seq(
          joins.BroadcastHashJoinExec(
            leftKeys,
            rightKeys,
            joinType,
            buildSide.get,
            condition,
            planLater(left),
            planLater(right)))
      }

      if (forceShuffledHashJoin) {
        // Force use of ShuffledHashJoin in preference to SortMergeJoin. With no respect to
        // conf setting "spark.sql.join.preferSortMergeJoin".
        val (leftBuildable, rightBuildable) = if (backendLib.equals(BackendLib.CH)) {
          // Currently, ClickHouse backend can not support AQE, so it needs to use join hint
          // to decide the build side, after supporting AQE, will remove this.
          val leftHintEnabled = hintToShuffleHashJoinLeft(hint)
          val rightHintEnabled = hintToShuffleHashJoinRight(hint)
          val leftHintMergeEnabled = hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE))
          val rightHintMergeEnabled = hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
          if (leftHintEnabled || rightHintEnabled) {
            (leftHintEnabled, rightHintEnabled)
          } else if (leftHintMergeEnabled || rightHintMergeEnabled) {
            // hack: when set SHUFFLE_MERGE hint, it means that
            // it don't use this side as the build side
            (!leftHintMergeEnabled, !rightHintMergeEnabled)
          } else {
            (canBuildShuffledHashJoinLeft(joinType), canBuildShuffledHashJoinRight(joinType))
          }
        } else {
          (canBuildShuffledHashJoinLeft(joinType), canBuildShuffledHashJoinRight(joinType))
        }

        if (!leftBuildable && !rightBuildable) {
          return Nil
        }
        val buildSide = if (!leftBuildable) {
          BuildRight
        } else if (!rightBuildable) {
          BuildLeft
        } else {
          getSmallerSide(left, right)
        }

        return Option(buildSide)
          .map { buildSide =>
            Seq(
              joins.ShuffledHashJoinExec(
                leftKeys,
                rightKeys,
                joinType,
                buildSide,
                condition,
                planLater(left),
                planLater(right)))
          }
          .getOrElse(Nil)
      }
      Nil
    }
  }

  override def canBuildShuffledHashJoinLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | RightOuter | FullOuter => true
      // For Velox backend, build right and left are both supported for LeftOuter and LeftSemi.
      case LeftOuter | LeftSemi => backendLib.equals(BackendLib.VELOX)
      case _ => false
    }
  }


  override def canBuildShuffledHashJoinRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | FullOuter |
           LeftSemi | LeftAnti | _: ExistenceJoin => true
      // For Velox backend, build right and left are both supported for RightOuter.
      case RightOuter => backendLib.equals(BackendLib.VELOX)
      case _ => false
    }
  }
}
