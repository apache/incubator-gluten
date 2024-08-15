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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._

case class ReorderJoinTablesRule(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (CHBackendSettings.enableReorderHashJoinTables) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case hashShuffle: ColumnarShuffleExchangeExec =>
        hashShuffle.withNewChildren(hashShuffle.children.map(visitPlan))
      case hashJoin: CHShuffledHashJoinExecTransformer =>
        val newHashJoin = reorderHashJoin(hashJoin)
        newHashJoin.withNewChildren(newHashJoin.children.map(visitPlan))
      case _ =>
        plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  private def reorderHashJoin(hashJoin: CHShuffledHashJoinExecTransformer): SparkPlan = {
    val leftQueryStageRow = childShuffleQueryStageRows(hashJoin.left)
    val rightQueryStageRow = childShuffleQueryStageRows(hashJoin.right)
    if (leftQueryStageRow == None || rightQueryStageRow == None) {
      logError(s"Cannot reorder this hash join. Its children is not ShuffleQueryStageExec")
      hashJoin
    } else {
      val threshold = CHBackendSettings.reorderHashJoinTablesThreshold
      val isLeftLarger = leftQueryStageRow.get > rightQueryStageRow.get * threshold
      val isRightLarger = leftQueryStageRow.get * threshold < rightQueryStageRow.get
      hashJoin.joinType match {
        case Inner =>
          if (isRightLarger && hashJoin.buildSide == BuildRight) {
            CHShuffledHashJoinExecTransformer(
              hashJoin.rightKeys,
              hashJoin.leftKeys,
              hashJoin.joinType,
              hashJoin.buildSide,
              hashJoin.condition,
              hashJoin.right,
              hashJoin.left,
              hashJoin.isSkewJoin)
          } else if (isLeftLarger && hashJoin.buildSide == BuildLeft) {
            CHShuffledHashJoinExecTransformer(
              hashJoin.leftKeys,
              hashJoin.rightKeys,
              hashJoin.joinType,
              BuildRight,
              hashJoin.condition,
              hashJoin.left,
              hashJoin.right,
              hashJoin.isSkewJoin)
          } else {
            hashJoin
          }
        case LeftOuter =>
          // left outer + build right is the common case，other cases have not been covered by tests
          // and don't reroder them.
          if (isRightLarger && hashJoin.buildSide == BuildRight) {
            CHShuffledHashJoinExecTransformer(
              hashJoin.rightKeys,
              hashJoin.leftKeys,
              RightOuter,
              BuildRight,
              hashJoin.condition,
              hashJoin.right,
              hashJoin.left,
              hashJoin.isSkewJoin)
          } else {
            hashJoin
          }
        case RightOuter =>
          // right outer + build left is the common case，other cases have not been covered by tests
          // and don't reroder them.
          if (isLeftLarger && hashJoin.buildSide == BuildLeft) {
            CHShuffledHashJoinExecTransformer(
              hashJoin.leftKeys,
              hashJoin.rightKeys,
              RightOuter,
              BuildRight,
              hashJoin.condition,
              hashJoin.left,
              hashJoin.right,
              hashJoin.isSkewJoin)
          } else if (isRightLarger && hashJoin.buildSide == BuildLeft) {
            CHShuffledHashJoinExecTransformer(
              hashJoin.rightKeys,
              hashJoin.leftKeys,
              LeftOuter,
              BuildRight,
              hashJoin.condition,
              hashJoin.right,
              hashJoin.left,
              hashJoin.isSkewJoin)
          } else {
            hashJoin
          }
        case _ => hashJoin
      }
    }
  }

  private def childShuffleQueryStageRows(plan: SparkPlan): Option[BigInt] = {
    plan match {
      case queryStage: ShuffleQueryStageExec =>
        queryStage.getRuntimeStatistics.rowCount
      case _: ColumnarBroadcastExchangeExec =>
        None
      case _: ColumnarShuffleExchangeExec =>
        None
      case _ =>
        if (plan.children.length == 1) {
          childShuffleQueryStageRows(plan.children.head)
        } else {
          None
        }
    }
  }
}
