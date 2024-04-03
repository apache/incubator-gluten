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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, TransformPostOverrides, TransformPreOverrides}
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.utils.{LogLevelUtil, PhysicalPlanSelector}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.util.SparkRuleUtil

import scala.collection.mutable.ListBuffer

class HeuristicApplier(session: SparkSession)
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {

  private val GLUTEN_IS_ADAPTIVE_CONTEXT = "gluten.isAdaptiveContext"

  private lazy val transformPlanLogLevel = GlutenConfig.getConf.transformPlanLogLevel
  private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // Holds the original plan for possible entire fallback.
  private val localOriginalPlans: ListBuffer[SparkPlan] = ListBuffer.empty
  private val localIsAdaptiveContextFlags: ListBuffer[Boolean] = ListBuffer.empty

  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 18

  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan =
    withTransformRules(transformRules(outputsColumnar)).apply(plan)

  // Visible for testing.
  def withTransformRules(transformRules: List[SparkSession => Rule[SparkPlan]]): Rule[SparkPlan] =
    plan =>
      PhysicalPlanSelector.maybe(session, plan) {
        val finalPlan = prepareFallback(plan) {
          p =>
            val suggestedPlan = transformPlan(transformRules, p, "transform")
            transformPlan(fallbackPolicies(), suggestedPlan, "fallback") match {
              case FallbackNode(fallbackPlan) =>
                // we should use vanilla c2r rather than native c2r,
                // and there should be no `GlutenPlan` any more,
                // so skip the `postRules()`.
                fallbackPlan
              case plan =>
                transformPlan(postRules(), plan, "post")
            }
        }
        transformPlan(finalRules(), finalPlan, "final")
      }

  private def transformPlan(
      getRules: List[SparkSession => Rule[SparkPlan]],
      plan: SparkPlan,
      step: String) = GlutenTimeMetric.withMillisTime {
    logOnLevel(
      transformPlanLogLevel,
      s"${step}ColumnarTransitions preOverriden plan:\n${plan.toString}")
    val overridden = getRules.foldLeft(plan) {
      (p, getRule) =>
        val rule = getRule(session)
        val newPlan = rule(p)
        planChangeLogger.logRule(rule.ruleName, p, newPlan)
        newPlan
    }
    logOnLevel(
      transformPlanLogLevel,
      s"${step}ColumnarTransitions afterOverriden plan:\n${overridden.toString}")
    overridden
  }(t => logOnLevel(transformPlanLogLevel, s"${step}Transform SparkPlan took: $t ms."))

  private def prepareFallback[T](plan: SparkPlan)(f: SparkPlan => T): T = {
    setAdaptiveContext()
    setOriginalPlan(plan)
    try {
      f(plan)
    } finally {
      resetOriginalPlan()
      resetAdaptiveContext()
    }
  }

  /**
   * Rules to let planner create a suggested Gluten plan being sent to `fallbackPolicies` in which
   * the plan will be breakdown and decided to be fallen back or not.
   */
  private def transformRules(outputsColumnar: Boolean): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (_: SparkSession) => RemoveTransitions,
      (spark: SparkSession) => FallbackOnANSIMode(spark),
      (spark: SparkSession) => FallbackMultiCodegens(spark),
      (spark: SparkSession) => PlanOneRowRelation(spark),
      (_: SparkSession) => FallbackEmptySchemaRelation()
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarValidationRules() :::
      List(
        (spark: SparkSession) => MergeTwoPhasesHashBaseAggregate(spark),
        (_: SparkSession) => RewriteSparkPlanRulesManager(),
        (_: SparkSession) => AddTransformHintRule(),
        (_: SparkSession) => FallbackBloomFilterAggIfNeeded()
      ) :::
      List((_: SparkSession) => TransformPreOverrides()) :::
      List(
        (_: SparkSession) => RemoveNativeWriteFilesSortAndProject(),
        (spark: SparkSession) => RewriteTransformer(spark),
        (_: SparkSession) => EnsureLocalSortRequirements,
        (_: SparkSession) => CollapseProjectExecTransformer
      ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarTransformRules() :::
      SparkRuleUtil
        .extendedColumnarRules(session, GlutenConfig.getConf.extendedColumnarTransformRules) :::
      List((_: SparkSession) => InsertTransitions(outputsColumnar))
  }

  /**
   * Rules to add wrapper `FallbackNode`s on top of the input plan, as hints to make planner fall
   * back the whole input plan to the original vanilla Spark plan.
   */
  private def fallbackPolicies(): List[SparkSession => Rule[SparkPlan]] = {
    List((_: SparkSession) => ExpandFallbackPolicy(isAdaptiveContext, originalPlan))
  }

  /**
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(): List[SparkSession => Rule[SparkPlan]] =
    List(
      (_: SparkSession) => TransformPostOverrides(),
      (s: SparkSession) => InsertColumnarToColumnarTransitions(s),
      (s: SparkSession) => RemoveTopmostColumnarToRow(s, isAdaptiveContext)
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPostRules() :::
      List((_: SparkSession) => ColumnarCollapseTransformStages(GlutenConfig.getConf)) :::
      SparkRuleUtil.extendedColumnarRules(session, GlutenConfig.getConf.extendedColumnarPostRules)

  /*
   * Rules consistently applying to all input plans after all other rules have been applied, despite
   * whether the input plan is fallen back or not.
   */
  private def finalRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      // The rule is required despite whether the stage is fallen back or not. Since
      // ColumnarCachedBatchSerializer is statically registered to Spark without a columnar rule
      // when columnar table cache is enabled.
      (s: SparkSession) => RemoveGlutenTableCacheColumnarToRow(s),
      (s: SparkSession) => GlutenFallbackReporter(GlutenConfig.getConf, s),
      (_: SparkSession) => RemoveTransformHintRule()
    )
  }

  // Just for test use.
  def enableAdaptiveContext(): HeuristicApplier = {
    session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, "true")
    this
  }

  private def isAdaptiveContext: Boolean =
    Option(session.sparkContext.getLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT))
      .getOrElse("false")
      .toBoolean ||
      localIsAdaptiveContextFlags.head

  private def setAdaptiveContext(): Unit = {
    val traceElements = Thread.currentThread.getStackTrace
    assert(
      traceElements.length > aqeStackTraceIndex,
      s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
    // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
    // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
    // columnar rule will be applied in adaptive execution context. This part of code
    // needs to be carefully checked when supporting higher versions of spark to make
    // sure the calling stack has not been changed.
    localIsAdaptiveContextFlags
      .prepend(
        traceElements(aqeStackTraceIndex).getClassName
          .equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def resetAdaptiveContext(): Unit =
    localIsAdaptiveContextFlags.remove(0)

  private def setOriginalPlan(plan: SparkPlan): Unit = {
    localOriginalPlans.prepend(plan)
  }

  private def originalPlan: SparkPlan = {
    val plan = localOriginalPlans.head
    assert(plan != null)
    plan
  }

  private def resetOriginalPlan(): Unit = localOriginalPlans.remove(0)
}
