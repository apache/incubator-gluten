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
package org.apache.gluten.extension

import org.apache.gluten.{GlutenConfig, GlutenSparkExtensionsInjector}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ShuffledHashJoinExecTemp
import org.apache.gluten.extension.columnar.TRANSFORM_UNSUPPORTED
import org.apache.gluten.extension.columnar.TransformHints.TAG
import org.apache.gluten.utils.LogicalPlanSelector

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{JoinSelectionShim, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}

object StrategyOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(JoinSelectionOverrides)
  }
}

// Use the smaller table to build hashmap in shuffled hash join. BuildSide needs to be generated
// in PlannerStrategy, otherwise larger table may be used.
case class JoinSelectionOverrides(session: SparkSession)
  extends Strategy
  with JoinSelectionHelper
  with SQLConfHelper {

  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def existsMultiJoins(plan: LogicalPlan, count: Int = 0): Boolean = {
    plan match {
      case plan: Join =>
        if ((count + 1) >= GlutenConfig.getConf.logicalJoinOptimizationThrottle) return true
        plan.children.exists(existsMultiJoins(_, count + 1))
      case plan: Project =>
        if ((count + 1) >= GlutenConfig.getConf.logicalJoinOptimizationThrottle) return true
        plan.children.exists(existsMultiJoins(_, count + 1))
      case _ => false
    }
  }

  def tagNotTransformable(plan: LogicalPlan, reason: String): LogicalPlan = {
    plan.setTagValue(TAG, TRANSFORM_UNSUPPORTED(Some(reason)))
    plan
  }

  def tagNotTransformable(plan: ShuffledJoin, reason: String): ShuffledJoin = {
    plan.setTagValue(TAG, TRANSFORM_UNSUPPORTED(Some(reason)))
    plan
  }

  def tagNotTransformableRecursive(plan: LogicalPlan, reason: String): LogicalPlan = {
    tagNotTransformable(
      plan.withNewChildren(plan.children.map(tagNotTransformableRecursive(_, reason))),
      reason)
  }

  def existLeftOuterJoin(plan: LogicalPlan): Boolean = {
    plan.collect {
      case join: Join if join.joinType.sql.equals("LEFT OUTER") =>
        return true
    }.size > 0
  }

  def genShuffledHashJoinExecTemp(
      joinType: JoinType,
      left: LogicalPlan,
      right: LogicalPlan,
      join: ShuffledJoin): ShuffledHashJoinExecTemp = {
    val leftBuildable = BackendsApiManager.getSettings
      .supportHashBuildJoinTypeOnLeft(joinType)
    val rightBuildable = BackendsApiManager.getSettings
      .supportHashBuildJoinTypeOnRight(joinType)
    val buildSide = if (!leftBuildable) {
      BuildRight
    } else if (!rightBuildable) {
      BuildLeft
    } else {
      getSmallerSide(left, right)
    }
    val child = tagNotTransformable(join, "child of ShuffledHashJoinExecTemp")
    ShuffledHashJoinExecTemp(child, buildSide)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    LogicalPlanSelector.maybeNil(session, plan) {
      // Ignore forceShuffledHashJoin if exist multi continuous joins
      if (
        GlutenConfig.getConf.enableLogicalJoinOptimize &&
        existsMultiJoins(plan) && existLeftOuterJoin(plan)
      ) {
        tagNotTransformableRecursive(plan, "exist multi continuous joins")
      }
      plan match {
        case j @ JoinSelectionShim.ExtractEquiJoinKeysShim(
              joinType,
              leftKeys,
              rightKeys,
              condition,
              left,
              right,
              _)
            if !isBroadcastStage(left) && !isBroadcastStage(right) &&
              !BackendsApiManager.getSparkPlanExecApiInstance.joinFallback(
                joinType,
                left.outputSet,
                right.outputSet,
                condition) =>
          val originalJoinExec = session.sessionState.planner.JoinSelection.apply(j)
          // TODO: ShuffledHashJoinExecTemp adapts to RAS
          if (
            GlutenConfig.getConf.enableRas && GlutenConfig.getConf.forceShuffledHashJoin &&
            !left.getTagValue(TAG).isDefined && !right.getTagValue(TAG).isDefined
          ) {
            originalJoinExec(0) match {
              case _: BroadcastHashJoinExec => originalJoinExec
              case _: ShuffledHashJoinExec => originalJoinExec
              case _ =>
                val leftBuildable = canBuildShuffledHashJoinLeft(joinType)
                val rightBuildable = canBuildShuffledHashJoinRight(joinType)
                val buildSide = if (!leftBuildable) {
                  BuildRight
                } else if (!rightBuildable) {
                  BuildLeft
                } else {
                  getSmallerSide(left, right)
                }
                Option(buildSide)
                  .map {
                    buildSide =>
                      Seq(
                        ShuffledHashJoinExec(
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
          } else {
            originalJoinExec(0) match {
              case shj: ShuffledHashJoinExec =>
                Seq(genShuffledHashJoinExecTemp(joinType, left, right, shj))
              case smj: SortMergeJoinExec
                  if GlutenConfig.getConf.forceShuffledHashJoin &&
                    !left.getTagValue(TAG).isDefined && !right.getTagValue(TAG).isDefined =>
                Seq(genShuffledHashJoinExecTemp(joinType, left, right, smj))
              case _ => originalJoinExec
            }
          }
        case _ => Nil
      }
    }
}
