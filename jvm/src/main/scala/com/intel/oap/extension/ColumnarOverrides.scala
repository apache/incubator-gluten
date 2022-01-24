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

package com.intel.oap.extension

import com.intel.oap.{GazelleJniConfig, GazelleSparkExtensionsInjector}
import com.intel.oap.execution._
import com.intel.oap.extension.columnar.{RowGuard, TransformGuardRule}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, ArrowEvalPythonExecTransformer}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

case class TransformPreOverrides() extends Rule[SparkPlan] {
  val columnarConf: GazelleJniConfig = GazelleJniConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case RowGuard(child: CustomShuffleReaderExec) =>
      replaceWithTransformerPlan(child)
    case plan: RowGuard =>
      val actualPlan = plan.child match {
        case p: BroadcastHashJoinExec =>
          p.withNewChildren(p.children.map {
            case plan: BroadcastExchangeExec =>
              // if BroadcastHashJoin is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case other =>
          other
      }
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithTransformerPlan))
    case plan: ArrowEvalPythonExec =>
      val columnarChild = replaceWithTransformerPlan(plan.child)
      ArrowEvalPythonExecTransformer(plan.udfs, plan.resultAttrs, columnarChild, plan.evalType)
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new BatchScanExecTransformer(plan.output, plan.scan)
    case plan: CoalesceExec =>
      CoalesceExecTransformer(plan.numPartitions, replaceWithTransformerPlan(plan.child))
    case plan: InMemoryTableScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarInMemoryTableScanExec(plan.attributes, plan.predicates, plan.relation)
    case plan: ProjectExec =>
      val columnarChild = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      columnarChild match {
        case ch: ConditionProjectExecTransformer =>
          if (ch.projectList == null) {
            ConditionProjectExecTransformer(ch.condition, plan.projectList, ch.child)
          } else {
            ConditionProjectExecTransformer(null, plan.projectList, columnarChild)
          }
        case _ =>
          ConditionProjectExecTransformer(null, plan.projectList, columnarChild)
      }
    case plan: FilterExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ConditionProjectExecTransformer(plan.condition, null, child)
    case plan: HashAggregateExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      HashAggregateExecTransformer(
        plan.requiredChildDistributionExpressions,
        plan.groupingExpressions,
        plan.aggregateExpressions,
        plan.aggregateAttributes,
        plan.initialInputBufferOffset,
        plan.resultExpressions,
        child)
    case plan: UnionExec =>
      val children = plan.children.map(replaceWithTransformerPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      UnionExecTransformer(children)
    case plan: ExpandExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ExpandExecTransformer(plan.projections, plan.output, child)
    case plan: SortExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      child match {
        case _ =>
          SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      }
    case plan: ShuffleExchangeExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if ((child.supportsColumnar || columnarConf.enablePreferColumnar) &&
           columnarConf.enableColumnarShuffle) {
        if (isSupportAdaptive) {
          new ColumnarShuffleExchangeAdaptor(
            plan.outputPartitioning,
            child)
        } else {
          CoalesceBatchesExec(
            ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child))
        }
      } else {
        plan.withNewChildren(Seq(child))
      }
    case plan: ShuffledHashJoinExec =>
      val left = replaceWithTransformerPlan(plan.left)
      val right = replaceWithTransformerPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ShuffledHashJoinExecTransformer(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    case plan: BroadcastQueryStageExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported," +
        s"actual plan is ${plan.plan.getClass}.")
      plan
    case plan: BroadcastExchangeExec =>
      plan
    case plan: BroadcastHashJoinExec =>
      plan
    case plan: SortMergeJoinExec =>
      if (columnarConf.enableColumnarSortMergeJoin) {
        val left = replaceWithTransformerPlan(plan.left)
        val right = replaceWithTransformerPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")

        SortMergeJoinExecTransformer(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      } else {
        val children = plan.children.map(replaceWithTransformerPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }
    case plan: ShuffleQueryStageExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      plan
    case plan: CustomShuffleReaderExec if columnarConf.enableColumnarShuffle =>
      plan.child match {
        case shuffle: ColumnarShuffleExchangeAdaptor =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs))
        case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs))
        case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
          reused match {
            case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              CoalesceBatchesExec(
                ColumnarCustomShuffleReaderExec(
                  plan.child,
                  plan.partitionSpecs))
            case _ =>
              plan
          }
        case _ =>
          plan
      }
    case plan: WindowExec =>
      WindowExecTransformer(
        plan.windowExpression,
        plan.partitionSpec,
        plan.orderSpec,
        replaceWithTransformerPlan(plan.child))
    case p =>
      val children = plan.children.map(replaceWithTransformerPlan)
      logDebug(s"Transformation for ${p.getClass} is currently not supported.")
      p.withNewChildren(children)
  }
  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithTransformerPlan(plan)
  }

}

case class TransformPostOverrides() extends Rule[SparkPlan] {
  val columnarConf = GazelleJniConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"ColumnarPostOverrides RowToArrowColumnarExec(${child.getClass})")
      RowToArrowColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithTransformerPlan(child.child)))
    case plan: ColumnarToRowExec =>
      if (columnarConf.enableArrowColumnarToRow) {
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"ColumnarPostOverrides ArrowColumnarToRowExec(${child.getClass})")
        new ArrowColumnarToRowExec(child)
      } else {
        val children = plan.children.map(replaceWithTransformerPlan)
        plan.withNewChildren(children)
      }
    case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
          c.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          if (columnarConf.enableArrowColumnarToRow) {
            try {
              val child = replaceWithTransformerPlan(c.child)
              new ArrowColumnarToRowExec(child)
            } catch {
              case _: Throwable =>
                logInfo("ArrowColumnarToRow : Falling back to ColumnarToRow...")
                c.withNewChildren(c.children.map(replaceWithTransformerPlan))
            }
          } else {
            c.withNewChildren(c.children.map(replaceWithTransformerPlan))
          }
        case other =>
          replaceWithTransformerPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithTransformerPlan)
      p.withNewChildren(children)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithTransformerPlan(plan)
  }

}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled: Boolean = session.sqlContext.getConf(
    "org.apache.spark.example.columnar.enabled", "true").trim.toBoolean
  def conf = session.sparkContext.getConf

  // Do not create rules in class initialization as we should access SQLConf while creating the rules.
  // At this time SQLConf may not be there yet.
  def rowGuardOverrides = TransformGuardRule()
  def preOverrides = TransformPreOverrides()
  def postOverrides = TransformPostOverrides()

  val columnarWholeStageEnabled: Boolean = conf.getBoolean(
    "spark.oap.sql.columnar.wholestagetransform", defaultValue = true)
  def collapseOverrides = ColumnarCollapseCodegenStages(columnarWholeStageEnabled)

  var isSupportAdaptive: Boolean = true

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    // TODO migrate dynamic-partition-pruning onto adaptive execution.
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case e: Exchange => true
      case other => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled && (sanityCheck(plan) &&
    !plan.logicalLink.exists(_.isStreaming) &&
    !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
    plan.children.forall(supportAdaptive)))
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      isSupportAdaptive = supportAdaptive(plan)
      val rule = preOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      rule(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      val rule = postOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      val tmpPlan = rule(plan)
      collapseOverrides(tmpPlan)
    } else {
      plan
    }
  }
}

object ColumnarOverrides extends GazelleSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(ColumnarOverrideRules)
  }
}
