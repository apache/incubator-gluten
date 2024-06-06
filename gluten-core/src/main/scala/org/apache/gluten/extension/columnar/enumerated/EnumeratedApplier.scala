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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow}
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.util.AdaptiveContext
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.utils.{LogLevelUtil, PhysicalPlanSelector}

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter, SparkPlan}
import org.apache.spark.util.SparkRuleUtil

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by enumerating on
 * all the possibilities of executable Gluten plans, then choose the best plan among them.
 *
 * NOTE: This is still working in progress. We still have a bunch of heuristic rules in this
 * implementation's rule list. Future work will include removing them from the list then
 * implementing them in EnumeratedTransform.
 */
@Experimental
class EnumeratedApplier(session: SparkSession)
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {
  // An empirical value.
  private val aqeStackTraceIndex = 16

  private lazy val transformPlanLogLevel = GlutenConfig.getConf.transformPlanLogLevel
  private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  private val adaptiveContext = AdaptiveContext(session, aqeStackTraceIndex)

  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan =
    PhysicalPlanSelector.maybe(session, plan) {
      val transformed = transformPlan(transformRules(outputsColumnar), plan, "transform")
      val postPlan = maybeAqe {
        transformPlan(postRules(), transformed, "post")
      }
      val finalPlan = transformPlan(finalRules(), postPlan, "final")
      finalPlan
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

  private def maybeAqe[T](f: => T): T = {
    adaptiveContext.setAdaptiveContext()
    try {
      f
    } finally {
      adaptiveContext.resetAdaptiveContext()
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
      (spark: SparkSession) => PlanOneRowRelation(spark),
      (_: SparkSession) => FallbackEmptySchemaRelation()
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarValidationRules() :::
      List((spark: SparkSession) => MergeTwoPhasesHashBaseAggregate(spark)) :::
      List(
        (session: SparkSession) => EnumeratedTransform(session, outputsColumnar),
        (_: SparkSession) => RemoveTransitions
      ) :::
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
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(): List[SparkSession => Rule[SparkPlan]] =
    List(
      (s: SparkSession) => RemoveTopmostColumnarToRow(s, adaptiveContext.isAdaptiveContext())) :::
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
}
