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

import org.apache.gluten.execution._
import org.apache.gluten.utils.{CHJoinValidateUtil, ShuffleHashJoinStrategy}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.joins._

// import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
// If a SortMergeJoinExec cannot be offloaded, try to replace it with ShuffledHashJoinExec
// instead.
// This is rule is applied after spark plan nodes are transformed into columnar ones.
case class RewriteSortMergeJoinToHashJoinRule(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    visitPlan(plan)
  }

  private def visitPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case smj: SortMergeJoinExec =>
        tryReplaceSortMergeJoin(smj)
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
  }

  private def tryReplaceSortMergeJoin(smj: SortMergeJoinExec): SparkPlan = {
    // cannot offload SortMergeJoin, try to replace it with ShuffledHashJoin
    val needFallback = CHJoinValidateUtil.shouldFallback(
      ShuffleHashJoinStrategy(smj.joinType),
      smj.left.outputSet,
      smj.right.outputSet,
      smj.condition)
    // also cannot offload HashJoin, don't replace it.
    if (needFallback) {
      logInfo(s"Cannot offload this join by hash join algorithm")
      return smj
    } else {
      replaceSortMergeJoinWithHashJoin(smj)
    }
  }

  private def replaceSortMergeJoinWithHashJoin(smj: SortMergeJoinExec): SparkPlan = {
    val newLeft = replaceSortMergeJoinChild(smj.left)
    val newRight = replaceSortMergeJoinChild(smj.right)
    // Some cases that we cannot handle.
    if (newLeft == null || newRight == null) {
      logInfo("Apply on sort merge children failed")
      return smj
    }

    var hashJoin = CHShuffledHashJoinExecTransformer(
      smj.leftKeys,
      smj.rightKeys,
      smj.joinType,
      BuildRight,
      smj.condition,
      newLeft,
      newRight,
      smj.isSkewJoin,
      false)
    val validateResult = hashJoin.doValidate()
    if (!validateResult.ok()) {
      logError(s"Validation failed for ShuffledHashJoinExec: ${validateResult.reason()}")
      return smj
    }
    hashJoin
  }

  private def replaceSortMergeJoinChild(plan: SparkPlan): SparkPlan = {
    plan match {
      case sort: SortExecTransformer =>
        sort.child match {
          case hashShuffle: ColumnarShuffleExchangeExec =>
            // drop sort node, return the shuffle node direclty
            hashShuffle.withNewChildren(hashShuffle.children.map(visitPlan))
          case aqeShuffle: AQEShuffleReadExec =>
            // drop sort node, return the shuffle node direclty
            aqeShuffle.withNewChildren(aqeShuffle.children.map(visitPlan))
          case columnarPlan: TransformSupport =>
            visitPlan(columnarPlan)
          case _ =>
            // other cases that we don't know
            logInfo(s"Expected ColumnarShuffleExchangeExec, got ${sort.child.getClass}")
            null
        }
      case smj: SortMergeJoinExec =>
        val newChild = replaceSortMergeJoinWithHashJoin(smj)
        if (newChild.isInstanceOf[SortMergeJoinExec]) {
          null
        } else {
          newChild
        }
      case _: TransformSupport => visitPlan(plan)
      case _ =>
        logInfo(s"Expected Columnar node, got ${plan.getClass}")
        null
    }
  }
}
