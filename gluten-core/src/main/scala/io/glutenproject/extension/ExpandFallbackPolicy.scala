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
import io.glutenproject.execution.BroadcastHashJoinExecTransformer
import io.glutenproject.extension.columnar.TransformHints

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarToRowExec, CommandResultExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.Exchange

// spotless:off
/**
 * Note, this rule should only fallback to row-based plan if there is no harm.
 * The follow case should be handled carefully
 *
 * 1. A BHJ and the previous broadcast exchange is columnar
 *    We should still make the BHJ columnar, otherwise it will fail if
 *    the vanilla BHJ accept a columnar broadcast exchange, e.g.,
 *
 *    Scan                Scan
 *      \                  |
 *        \     Columnar Broadcast Exchange
 *          \       /
 *             BHJ
 *              |
 *       VeloxColumnarToRow
 *              |
 *           Project (unsupport columnar)
 *
 * 2. The previous shuffle exchange stage is a columnar shuffle exchange
 *    We should use VeloxColumnarToRow rather than vanilla Spark ColumnarToRowExec, e.g.,
 *
 *             Scan
 *              |
 *    Columnar Shuffle Exchange
 *              |
 *       VeloxColumnarToRow
 *              |
 *           Project (unsupport columnar)
 *
 * @param isAdaptiveContext If is inside AQE
 * @param originalPlan The vanilla SparkPlan without apply gluten transform rules
 */
// spotless:on
case class ExpandFallbackPolicy(isAdaptiveContext: Boolean, originalPlan: SparkPlan)
  extends Rule[SparkPlan] {

  private def countFallbacks(plan: SparkPlan): Int = {
    var fallbacks = 0
    def countFallback(plan: SparkPlan): Unit = {
      plan match {
        case _: QueryStageExec => // Another stage.
        case _: CommandResultExec | _: ExecutedCommandExec => // ignore
        // we plan exchange to columnar exchange in columnar rules and the exchange does not
        // support columnar, so the output columnar is always false in AQE postStageCreationRules
        case ColumnarToRowExec(s: Exchange) if s.supportsColumnar && isAdaptiveContext =>
          countFallback(s)
        case ColumnarToRowExec(p: GlutenPlan) =>
          logDebug(s"Find a columnar to row for gluten plan:\n$p")
          fallbacks = fallbacks + 1
          countFallback(p)
        case leafPlan: LeafExecNode if !leafPlan.isInstanceOf[GlutenPlan] =>
          // Possible fallback for leaf node.
          fallbacks = fallbacks + 1
        case p => p.children.foreach(countFallback)
      }
    }
    countFallback(plan)
    fallbacks
  }

  private def hasColumnarBroadcastExchangeWithJoin(plan: SparkPlan): Boolean = {
    def isColumnarBroadcastExchange(p: SparkPlan): Boolean = p match {
      case BroadcastQueryStageExec(_, _: ColumnarBroadcastExchangeExec, _) => true
      case _ => false
    }

    plan.find {
      case j: BroadcastHashJoinExecTransformer
          if isColumnarBroadcastExchange(j.left) ||
            isColumnarBroadcastExchange(j.right) =>
        true
      case _ => false
    }.isDefined
  }

  private def fallback(plan: SparkPlan): Option[String] = {
    val fallbackThreshold = if (isAdaptiveContext) {
      GlutenConfig.getConf.wholeStageFallbackThreshold
    } else if (plan.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined) {
      // if we are here, that means we are now at `QueryExecution.preparations` and
      // AQE is actually applied. We do nothing for this case, and later in
      // AQE we can check `wholeStageFallbackThreshold`.
      return None
    } else {
      // AQE is not applied, so we use the whole query threshold to check if should fallback
      GlutenConfig.getConf.queryFallbackThreshold
    }
    if (fallbackThreshold < 0) {
      return None
    }

    // not safe to fallback row-based BHJ as the broadcast exchange is already columnar
    if (hasColumnarBroadcastExchangeWithJoin(plan)) {
      return None
    }

    val numFallback = countFallbacks(plan)
    if (numFallback >= fallbackThreshold) {
      Some(
        s"Fall back the plan due to fallback number $numFallback, " +
          s"threshold $fallbackThreshold")
    } else {
      None
    }
  }

  private def fallbackToRowBasedPlan(): SparkPlan = {
    val transformPostOverrides = TransformPostOverrides(isAdaptiveContext)
    val planWithColumnarToRow = InsertTransitions.insertTransitions(originalPlan, false)
    planWithColumnarToRow.transform {
      case c2r @ ColumnarToRowExec(_: ShuffleQueryStageExec) =>
        transformPostOverrides.transformColumnarToRowExec(c2r)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val reason = fallback(plan)
    if (reason.isDefined) {
      val fallbackPlan = fallbackToRowBasedPlan()
      TransformHints.tagAllNotTransformable(fallbackPlan, reason.get)
      FallbackNode(fallbackPlan)
    } else {
      plan
    }
  }
}

/** A wrapper to specify the plan is fallback plan, the caller side should unwrap it. */
case class FallbackNode(fallbackPlan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = fallbackPlan.output
}
