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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.datasource.ArrowConvertorRule
import org.apache.gluten.extension._
import org.apache.gluten.extension.EmptySchemaWorkaround.{FallbackEmptySchemaRelation, PlanOneRowRelation}
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast, TransformPreOverrides}
import org.apache.gluten.extension.columnar.enumerated.EnumeratedTransform
import org.apache.gluten.extension.columnar.rewrite.RewriteSparkPlanRulesManager
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.injector.{RuleInjector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PhysicalPlanSelector

import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter}

class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: RuleInjector): Unit = {
    injector.gluten.skipOn(PhysicalPlanSelector.skipCond)

    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

private object VeloxRuleApi {
  def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectOptimizerRule(CollectRewriteRule.apply)
    injector.injectOptimizerRule(HLLRewriteRule.apply)
    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
  }

  def injectLegacy(injector: LegacyInjector): Unit = {
    // Gluten columnar: Transform rules.
    injector.injectTransform(_ => RemoveTransitions)
    injector.injectTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectTransform(c => PlanOneRowRelation.apply(c.session))
    injector.injectTransform(_ => RewriteSubqueryBroadcast())
    injector.injectTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectTransform(c => ArrowScanReplaceRule.apply(c.session))
    injector.injectTransform(_ => FallbackEmptySchemaRelation())
    injector.injectTransform(_ => RewriteSparkPlanRulesManager())
    injector.injectTransform(_ => AddFallbackTagRule())
    injector.injectTransform(_ => TransformPreOverrides())
    injector.injectTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectTransform(c => RewriteTransformer.apply(c.session))
    injector.injectTransform(_ => PushDownFilterToScan)
    injector.injectTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectTransform(_ => EnsureLocalSortRequirements)
    injector.injectTransform(_ => EliminateLocalSort)
    injector.injectTransform(_ => CollapseProjectExecTransformer)
    injector.injectTransform(c => FlushableHashAggregateRule.apply(c.session))
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

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.conf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  def injectRas(injector: RasInjector): Unit = {
    // Gluten RAS: Pre rules.
    injector.inject(_ => RemoveTransitions)
    injector.inject(_ => PushDownInputFileExpression.PreOffload)
    injector.inject(c => FallbackOnANSIMode.apply(c.session))
    injector.inject(c => PlanOneRowRelation.apply(c.session))
    injector.inject(_ => FallbackEmptySchemaRelation())
    injector.inject(_ => RewriteSubqueryBroadcast())
    injector.inject(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.inject(c => ArrowScanReplaceRule.apply(c.session))

    // Gluten RAS: The RAS rule.
    injector.inject(c => EnumeratedTransform(c.session, c.outputsColumnar))

    // Gluten RAS: Post rules.
    injector.inject(_ => RemoveTransitions)
    injector.inject(_ => RemoveNativeWriteFilesSortAndProject())
    injector.inject(c => RewriteTransformer.apply(c.session))
    injector.inject(_ => PushDownFilterToScan)
    injector.inject(_ => PushDownInputFileExpression.PostOffload)
    injector.inject(_ => EnsureLocalSortRequirements)
    injector.inject(_ => EliminateLocalSort)
    injector.inject(_ => CollapseProjectExecTransformer)
    injector.inject(c => FlushableHashAggregateRule.apply(c.session))
    injector.inject(c => InsertTransitions(c.outputsColumnar))
    injector.inject(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.inject(c => each(c.session)))
    injector.inject(c => ColumnarCollapseTransformStages(c.conf))
    injector.inject(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.inject(c => GlutenFallbackReporter(c.conf, c.session))
    injector.inject(_ => RemoveFallbackTagRule())
  }
}
