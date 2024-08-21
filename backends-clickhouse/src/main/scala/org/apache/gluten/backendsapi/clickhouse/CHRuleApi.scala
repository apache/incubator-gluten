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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast, TransformPreOverrides}
import org.apache.gluten.extension.columnar.rewrite.RewriteSparkPlanRulesManager
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.injector.{RuleInjector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.parser.GlutenClickhouseSqlParser
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.{CHAggregateFunctionRewriteRule, EqualToRewrite}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter}
import org.apache.spark.util.SparkPlanRules

class CHRuleApi extends RuleApi {
  import CHRuleApi._
  override def injectRules(injector: RuleInjector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

private object CHRuleApi {
  def injectSpark(injector: SparkInjector): Unit = {
    // Regular Spark rules.
    injector.injectQueryStagePrepRule(FallbackBroadcastHashJoinPrepQueryStage.apply)
    injector.injectParser(
      (spark, parserInterface) => new GlutenClickhouseSqlParser(spark, parserInterface))
    injector.injectResolutionRule(
      spark => new RewriteToDateExpresstionRule(spark, spark.sessionState.conf))
    injector.injectResolutionRule(
      spark => new RewriteDateTimestampComparisonRule(spark, spark.sessionState.conf))
    injector.injectOptimizerRule(
      spark => new CommonSubexpressionEliminateRule(spark, spark.sessionState.conf))
    injector.injectOptimizerRule(spark => CHAggregateFunctionRewriteRule(spark))
    injector.injectOptimizerRule(_ => CountDistinctWithoutExpand)
    injector.injectOptimizerRule(_ => EqualToRewrite)
  }

  def injectLegacy(injector: LegacyInjector): Unit = {
    // Gluten columnar: Transform rules.
    injector.injectTransform(_ => RemoveTransitions)
    injector.injectTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectTransform(_ => RewriteSubqueryBroadcast())
    injector.injectTransform(c => FallbackBroadcastHashJoin.apply(c.session))
    injector.injectTransform(c => MergeTwoPhasesHashBaseAggregate.apply(c.session))
    injector.injectTransform(_ => RewriteSparkPlanRulesManager())
    injector.injectTransform(_ => AddFallbackTagRule())
    injector.injectTransform(_ => TransformPreOverrides())
    injector.injectTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectTransform(c => RewriteTransformer.apply(c.session))
    injector.injectTransform(_ => EnsureLocalSortRequirements)
    injector.injectTransform(_ => EliminateLocalSort)
    injector.injectTransform(_ => CollapseProjectExecTransformer)
    injector.injectTransform(c => RewriteSortMergeJoinToHashJoinRule.apply(c.session))
    injector.injectTransform(
      c => SparkPlanRules.extendedColumnarRule(c.conf.extendedColumnarTransformRules)(c.session))
    injector.injectTransform(c => InsertTransitions(c.outputsColumnar))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.conf))
    injector.injectTransform(
      c => SparkPlanRules.extendedColumnarRule(c.conf.extendedColumnarPostRules)(c.session))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.conf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  def injectRas(injector: RasInjector): Unit = {
    // CH backend doesn't work with RAS at the moment. Inject a rule that aborts any
    // execution calls.
    injector.inject(
      _ =>
        new SparkPlanRules.AbortRule(
          "Clickhouse backend doesn't yet have RAS support, please try disabling RAS and" +
            " rerunning the application"))
  }
}
