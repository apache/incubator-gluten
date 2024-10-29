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
import org.apache.gluten.parser.{GlutenCacheFilesSqlParser, GlutenClickhouseSqlParser}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.{CHAggregateFunctionRewriteRule, EqualToRewrite}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, CommandResultExec, FileSourceScanExec, GlutenFallbackReporter, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
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
    // Inject the regular Spark rules directly.
    injector.injectQueryStagePrepRule(FallbackBroadcastHashJoinPrepQueryStage.apply)
    injector.injectQueryStagePrepRule(spark => CHAQEPropagateEmptyRelation(spark))
    injector.injectParser(
      (spark, parserInterface) => new GlutenCacheFilesSqlParser(spark, parserInterface))
    injector.injectParser(
      (spark, parserInterface) => new GlutenClickhouseSqlParser(spark, parserInterface))
    injector.injectResolutionRule(spark => new RewriteToDateExpresstionRule(spark))
    injector.injectResolutionRule(spark => new RewriteDateTimestampComparisonRule(spark))
    injector.injectOptimizerRule(spark => new CommonSubexpressionEliminateRule(spark))
    injector.injectOptimizerRule(spark => CHAggregateFunctionRewriteRule(spark))
    injector.injectOptimizerRule(_ => CountDistinctWithoutExpand)
    injector.injectOptimizerRule(_ => EqualToRewrite)
  }

  def injectLegacy(injector: LegacyInjector): Unit = {

    // Gluten columnar: Transform rules.
    injector.injectTransform(_ => RemoveTransitions)
    injector.injectTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectTransform(_ => RewriteSubqueryBroadcast())
    injector.injectTransform(c => FallbackBroadcastHashJoin.apply(c.session))
    injector.injectTransform(c => MergeTwoPhasesHashBaseAggregate.apply(c.session))
    injector.injectTransform(_ => intercept(RewriteSparkPlanRulesManager()))
    injector.injectTransform(_ => intercept(AddFallbackTagRule()))
    injector.injectTransform(_ => intercept(TransformPreOverrides()))
    injector.injectTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectTransform(c => intercept(RewriteTransformer.apply(c.session)))
    injector.injectTransform(_ => PushDownFilterToScan)
    injector.injectTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectTransform(_ => EnsureLocalSortRequirements)
    injector.injectTransform(_ => EliminateLocalSort)
    injector.injectTransform(_ => CollapseProjectExecTransformer)
    injector.injectTransform(c => RewriteSortMergeJoinToHashJoinRule.apply(c.session))
    injector.injectTransform(c => PushdownAggregatePreProjectionAheadExpand.apply(c.session))
    injector.injectTransform(
      c =>
        intercept(
          SparkPlanRules.extendedColumnarRule(c.conf.extendedColumnarTransformRules)(c.session)))
    injector.injectTransform(c => InsertTransitions(c.outputsColumnar))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => intercept(each(c.session))))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.conf))
    injector.injectTransform(
      c =>
        intercept(SparkPlanRules.extendedColumnarRule(c.conf.extendedColumnarPostRules)(c.session)))

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

  /**
   * Since https://github.com/apache/incubator-gluten/pull/883.
   *
   * TODO: Remove this since tricky to maintain.
   */
  private class CHSparkRuleInterceptor(delegate: Rule[SparkPlan])
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper {
    override val ruleName: String = delegate.ruleName

    override def apply(plan: SparkPlan): SparkPlan = {
      if (skipOn(plan)) {
        return plan
      }
      delegate(plan)
    }

    private def skipOn(plan: SparkPlan): Boolean = {
      // TODO: Currently there are some fallback issues on CH backend when SparkPlan is
      // TODO: SerializeFromObjectExec, ObjectHashAggregateExec and V2CommandExec.
      // For example:
      //   val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
      //   import spark.implicits._
      //   val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      //   df.summary().show(100, false)

      def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = {
        scanExec.relation.location.isInstanceOf[DeltaLogFileIndex]
      }

      val includedUnsupportedPlans = collect(plan) {
        // case s: SerializeFromObjectExec => true
        // case d: DeserializeToObjectExec => true
        // case o: ObjectHashAggregateExec => true
        case rddScanExec: RDDScanExec if rddScanExec.nodeName.contains("Delta Table State") => true
        case f: FileSourceScanExec if includedDeltaOperator(f) => true
        case v2CommandExec: V2CommandExec => true
        case commandResultExec: CommandResultExec => true
      }

      includedUnsupportedPlans.contains(true)
    }
  }

  private def intercept(delegate: Rule[SparkPlan]): Rule[SparkPlan] = {
    new CHSparkRuleInterceptor(delegate)
  }
}
