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
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.enumerated.{RasOffload, RemoveSort}
import org.apache.gluten.extension.columnar.enumerated.planner.cost.{LegacyCoster, RoughCoster, RoughCoster2}
import org.apache.gluten.extension.columnar.heuristic.ExpandFallbackPolicy
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.extension.columnar.validator.Validators.ValidatorBuilderImplicits
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

object VeloxRuleApi {
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectOptimizerRule(CollectRewriteRule.apply)
    injector.injectOptimizerRule(HLLRewriteRule.apply)
    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
  }

  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Legacy: The Legacy transform rule.
    injector.injectValidator {
      c =>
        Validator
          .builder()
          .fallbackByHint()
          .fallbackIfScanOnlyWithFilterPushed(c.glutenConf.enableScanOnly)
          .fallbackComplexExpressions()
          .fallbackByBackendSettings()
          .fallbackByUserOptions()
          .fallbackByTestInjects()
          .fallbackByNativeValidation()
          .build()
    }
    injector.injectRewriteRule(_ => RewriteIn)
    injector.injectRewriteRule(_ => RewriteMultiChildrenCount)
    injector.injectRewriteRule(_ => RewriteJoin)
    injector.injectRewriteRule(_ => PullOutPreProject)
    injector.injectRewriteRule(_ => PullOutPostProject)
    injector.injectOffloadRule(_ => OffloadOthers())
    injector.injectOffloadRule(_ => OffloadExchange())
    injector.injectOffloadRule(_ => OffloadJoin())

    // Legacy: Post-transform rules.
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(c => RewriteTransformer.apply(c.session))
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions(c.outputsColumnar))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.glutenConf))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  private def injectRas(injector: RasInjector): Unit = {
    // Gluten RAS: Pre rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Gluten RAS: The RAS rule.
    injector.injectCoster(_ => LegacyCoster)
    injector.injectCoster(_ => RoughCoster)
    injector.injectCoster(_ => RoughCoster2)
    injector.injectValidator {
      c =>
        Validator
          .builder()
          .fallbackByHint()
          .fallbackIfScanOnlyWithFilterPushed(c.glutenConf.enableScanOnly)
          .fallbackComplexExpressions()
          .fallbackByBackendSettings()
          .fallbackByUserOptions()
          .fallbackByTestInjects()
          .fallbackByNativeValidation()
          .build()
    }
    injector.injectRewriteRule(_ => RewriteIn)
    injector.injectRewriteRule(_ => RewriteMultiChildrenCount)
    injector.injectRewriteRule(_ => RewriteJoin)
    injector.injectRewriteRule(_ => PullOutPreProject)
    injector.injectRewriteRule(_ => PullOutPostProject)
    injector.injectRasRule(_ => RemoveSort)
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[Exchange](OffloadExchange()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[BaseJoinExec](OffloadJoin()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[FilterExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[ProjectExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from(HiveTableScanExecTransformer.isHiveTableScan)(OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[CoalesceExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from[HashAggregateExec](OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from[SortAggregateExec](OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[UnionExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[ExpandExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(RasOffload.from[WriteFilesExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[SortExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(
          RasOffload.from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec)(OffloadOthers()),
          c.validator,
          c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[LimitExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[GenerateExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c =>
        RasOffload.Rule(RasOffload.from[EvalPythonExec](OffloadOthers()), c.validator, c.rewrites))
    injector.injectRasRule(
      c => RasOffload.Rule(RasOffload.from[SampleExec](OffloadOthers()), c.validator, c.rewrites))

    // Gluten RAS: Post rules.
    injector.injectPostTransform(_ => RemoveTransitions)
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(c => RewriteTransformer.apply(c.session))
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions(c.outputsColumnar))
    injector.injectPostTransform(
      c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPostTransform(c => each(c.session)))
    injector.injectPostTransform(c => ColumnarCollapseTransformStages(c.glutenConf))
    injector.injectPostTransform(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectPostTransform(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectPostTransform(_ => RemoveFallbackTagRule())
  }
}
