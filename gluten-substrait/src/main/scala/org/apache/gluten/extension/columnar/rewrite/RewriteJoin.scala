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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.OffloadJoin

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

/** If force ShuffledHashJoin, convert [[SortMergeJoinExec]] to [[ShuffledHashJoinExec]]. */
object RewriteJoin extends RewriteSingleNode with JoinSelectionHelper {
  private def getSmjBuildSide(join: SortMergeJoinExec): Option[BuildSide] = {
    val leftBuildable = canBuildShuffledHashJoinLeft(join.joinType)
    val rightBuildable = canBuildShuffledHashJoinRight(join.joinType)
    if (!leftBuildable && !rightBuildable) {
      return None
    }
    if (!leftBuildable) {
      return Some(BuildRight)
    }
    if (!rightBuildable) {
      return Some(BuildLeft)
    }
    val side = join.logicalLink
      .flatMap {
        case join: Join => Some(OffloadJoin.getOptimalBuildSide(join))
        case _ => None
      }
      .getOrElse {
        // If smj has no logical link, or its logical link is not a join,
        // then we always choose left as build side.
        BuildLeft
      }
    Some(side)
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case smj: SortMergeJoinExec if GlutenConfig.getConf.forceShuffledHashJoin =>
      getSmjBuildSide(smj) match {
        case Some(buildSide) =>
          ShuffledHashJoinExec(
            smj.leftKeys,
            smj.rightKeys,
            smj.joinType,
            buildSide,
            smj.condition,
            smj.left,
            smj.right,
            smj.isSkewJoin)
        case _ => plan
      }
    case _ => plan
  }
}
