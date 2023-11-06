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
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarToRowExec, CommandResultExec, LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
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

  private def countFallback(plan: SparkPlan): Int = {
    var fallbacks = 0
    def countFallbackInternal(plan: SparkPlan): Unit = {
      plan match {
        case _: QueryStageExec => // Another stage.
        case _: CommandResultExec | _: ExecutedCommandExec => // ignore
        // we plan exchange to columnar exchange in columnar rules and the exchange does not
        // support columnar, so the output columnar is always false in AQE postStageCreationRules
        case ColumnarToRowExec(s: Exchange) if isAdaptiveContext =>
          countFallbackInternal(s)
        case u: UnaryExecNode
            if !u.isInstanceOf[GlutenPlan] && InMemoryTableScanHelper.isGlutenTableCache(u.child) =>
          // Vanilla Spark plan will call `InMemoryTableScanExec.convertCachedBatchToInternalRow`
          // which is a kind of `ColumnarToRowExec`.
          fallbacks = fallbacks + 1
          countFallbackInternal(u.child)
        case ColumnarToRowExec(p: GlutenPlan) =>
          logDebug(s"Find a columnar to row for gluten plan:\n$p")
          fallbacks = fallbacks + 1
          countFallbackInternal(p)
        case leafPlan: LeafExecNode if InMemoryTableScanHelper.isGlutenTableCache(leafPlan) =>
        case leafPlan: LeafExecNode if !leafPlan.isInstanceOf[GlutenPlan] =>
          // Possible fallback for leaf node.
          fallbacks = fallbacks + 1
        case p => p.children.foreach(countFallbackInternal)
      }
    }
    countFallbackInternal(plan)
    fallbacks
  }

  /**
   * When making a stage fall back, it's possible that we need a ColumnarToRow to adapt to last
   * stage's columnar output. So we need to evaluate the cost, i.e., the number of required
   * ColumnarToRow between entirely fallback stage and last stage(s). Thus, we can avoid possible
   * performance degradation caused by fallback policy.
   *
   * spotless:off
   *
   * Spark plan before applying fallback policy:
   *
   *        ColumnarExchange
   *  ----------- | --------------- last stage
   *    HashAggregateTransformer
   *              |
   *        ColumnarToRow
   *              |
   *           Project
   *
   * To illustrate the effect if cost is not taken into account, here is spark plan
   * after applying whole stage fallback policy (threshold = 1):
   *
   *        ColumnarExchange
   *  -----------  | --------------- last stage
   *         ColumnarToRow
   *               |
   *         HashAggregate
   *               |
   *            Project
   *
   *  So by considering the cost, the fallback policy will not be applied.
   *
   * spotless:on
   */
  private def countStageFallbackCost(plan: SparkPlan): Int = {
    var stageFallbackCost = 0

    /**
     * 1) Find a Gluten plan whose child is InMemoryTableScanExec. Then, increase stageFallbackCost
     * if InMemoryTableScanExec is gluten's table cache and decrease stageFallbackCost if not. 2)
     * Find a Gluten plan whose child is QueryStageExec. Then, increase stageFallbackCost if the
     * last query stage's plan is GlutenPlan and decrease stageFallbackCost if not.
     */
    def countStageFallbackCostInternal(plan: SparkPlan): Unit = {
      plan match {
        case _: GlutenPlan if plan.children.find(_.isInstanceOf[InMemoryTableScanExec]).isDefined =>
          plan.children
            .filter(_.isInstanceOf[InMemoryTableScanExec])
            .foreach {
              // For this case, table cache will internally execute ColumnarToRow if
              // we make the stage fall back.
              case child if InMemoryTableScanHelper.isGlutenTableCache(child) =>
                stageFallbackCost = stageFallbackCost + 1
              // For other case, table cache will save internal RowToColumnar if we make
              // the stage fall back.
              case _ =>
                stageFallbackCost = stageFallbackCost - 1
            }
        case _: GlutenPlan if plan.children.find(_.isInstanceOf[QueryStageExec]).isDefined =>
          plan.children
            .filter(_.isInstanceOf[QueryStageExec])
            .foreach {
              case stage: QueryStageExec
                  if stage.plan.isInstanceOf[GlutenPlan] ||
                    // For TableCacheQueryStageExec since spark 3.5.
                    InMemoryTableScanHelper.isGlutenTableCache(stage) =>
                stageFallbackCost = stageFallbackCost + 1
              // For other cases, RowToColumnar will be removed if stage falls back, so reduce
              // the cost.
              case _ =>
                stageFallbackCost = stageFallbackCost - 1
            }
        case _ => plan.children.foreach(countStageFallbackCostInternal)
      }
    }
    countStageFallbackCostInternal(plan)
    stageFallbackCost
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
      // AQE is actually not applied. We do nothing for this case, and later in
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

    val netFallbackNum = if (isAdaptiveContext) {
      countFallback(plan) - countStageFallbackCost(plan)
    } else {
      countFallback(plan)
    }
    if (netFallbackNum >= fallbackThreshold) {
      Some(
        s"Fallback policy is taking effect, net fallback number: $netFallbackNum, " +
          s"threshold: $fallbackThreshold")
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
      case c2r @ ColumnarToRowExec(_: AQEShuffleReadExec) =>
        transformPostOverrides.transformColumnarToRowExec(c2r)
      // `InMemoryTableScanExec` itself supports columnar to row
      case ColumnarToRowExec(child: SparkPlan)
          if InMemoryTableScanHelper.isGlutenTableCache(child) =>
        child
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
