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

import io.glutenproject.GlutenConfig
import io.glutenproject.GlutenSparkExtensionsInjector

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins


object JoinSelectionOverrides extends Strategy with JoinSelectionHelper with SQLConfHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // targeting equi-joins only
    case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond, left, right, hint) =>
      // Generate BHJ here, avoid to do match in `JoinSelection` again.
      val buildSide = getBroadcastBuildSide(left, right, joinType, hint, hintOnly = false, conf)
      if (buildSide.isDefined) {
        return Seq(joins.BroadcastHashJoinExec(
          leftKeys,
          rightKeys,
          joinType,
          buildSide.get,
          nonEquiCond,
          planLater(left),
          planLater(right)))
      }

      if (GlutenConfig.getSessionConf.forceShuffledHashJoin) {
        // Force use of ShuffledHashJoin in preference to SortMergeJoin. With no respect to
        // conf setting "spark.sql.join.preferSortMergeJoin".
        val leftBuildable = canBuildShuffledHashJoinLeft(joinType)
        val rightBuildable = canBuildShuffledHashJoinRight(joinType)
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

        return Option(buildSide).map {
          buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }.getOrElse(Nil)
      }

      Nil
    case _ => Nil
  }
}

object StrategyOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => JoinSelectionOverrides)
  }
}
