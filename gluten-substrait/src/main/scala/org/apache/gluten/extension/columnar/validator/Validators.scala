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
package org.apache.gluten.extension.columnar.validator

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.{BackendsApiManager, BackendSettingsApi}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.{BasicScanExecTransformer, ColumnarCoalesceExec, ColumnarUnionExec, ExpandExecTransformer, HashAggregateExecBaseTransformer, LimitExecTransformer, ProjectExecTransformer, ScanTransformerFactory, SortExecTransformer, TakeOrderedAndProjectExecTransformer, WindowExecTransformer, WindowGroupLimitExecTransformer, WriteFilesExecTransformer}
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadJoin
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

object Validators {
  implicit class ValidatorBuilderImplicits(builder: Validator.Builder) {
    private val conf = GlutenConfig.getConf
    private val settings = BackendsApiManager.getSettings

    /** Fails validation if a plan node was already tagged with TRANSFORM_UNSUPPORTED. */
    def fallbackByHint(): Validator.Builder = {
      builder.add(FallbackByHint)
    }

    /**
     * Fails validation if a plan node includes an expression that is considered too complex to
     * executed by native library. By default, we use a threshold option in config to make the
     * decision.
     */
    def fallbackComplexExpressions(): Validator.Builder = {
      builder.add(new FallbackComplexExpressions(conf.fallbackExpressionsThreshold))
    }

    /** Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. */
    def fallbackIfScanOnly(): Validator.Builder = {
      builder.add(new FallbackIfScanOnly(conf.enableScanOnly))
    }

    /**
     * Fails validation if native-execution of a plan node is not supported by current backend
     * implementation by checking the active BackendSettings.
     */
    def fallbackByBackendSettings(): Validator.Builder = {
      builder.add(new FallbackByBackendSettings(settings))
    }

    /**
     * Fails validation if native-execution of a plan node is disabled by Gluten/Spark
     * configuration.
     */
    def fallbackByUserOptions(): Validator.Builder = {
      builder.add(new FallbackByUserOptions(conf))
    }

    def fallbackByTestInjects(): Validator.Builder = {
      builder.add(new FallbackByTestInjects())
    }

    /**
     * Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. Also, passes
     * validation on filter for the exception that filter + scan is detected. Because filters can be
     * pushed into scan then the filter conditions will be processed only in scan.
     */
    def fallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean): Validator.Builder = {
      builder.add(new FallbackIfScanOnlyWithFilterPushed(scanOnly))
    }

    /**
     * Attempts to offload the input query plan node and check native validation result. Fails when
     * native validation failed.
     */
    def fallbackByNativeValidation(): Validator.Builder = {
      builder.add(new FallbackByNativeValidation())
    }
  }

  private object FallbackByHint extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackTags.nonEmpty(plan)) {
        val tag = FallbackTags.get(plan)
        return fail(tag.reason())
      }
      pass()
    }
  }

  private class FallbackComplexExpressions(threshold: Int) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (ExpressionUtils.hasComplexExpressions(plan, threshold)) {
        return fail(
          s"Disabled because at least one present expression exceeded depth threshold: " +
            s"${plan.nodeName}")
      }
      pass()
    }
  }

  private class FallbackIfScanOnly(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case _: BatchScanExec => pass()
      case _: FileSourceScanExec => pass()
      case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
      case p if scanOnly => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByBackendSettings(settings: BackendSettingsApi) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: ShuffleExchangeExec if !settings.supportColumnarShuffleExec() => fail(p)
      case p: SortMergeJoinExec if !settings.supportSortMergeJoinExec() => fail(p)
      case p: WriteFilesExec if !settings.enableNativeWriteFiles() =>
        fail(p)
      case p: CartesianProductExec if !settings.supportCartesianProductExec() => fail(p)
      case p: TakeOrderedAndProjectExec if !settings.supportColumnarShuffleExec() => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByUserOptions(glutenConf: GlutenConfig) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: SortExec if !glutenConf.enableColumnarSort => fail(p)
      case p: WindowExec if !glutenConf.enableColumnarWindow => fail(p)
      case p: SortMergeJoinExec if !glutenConf.enableColumnarSortMergeJoin => fail(p)
      case p: BatchScanExec if !glutenConf.enableColumnarBatchScan => fail(p)
      case p: FileSourceScanExec if !glutenConf.enableColumnarFileScan => fail(p)
      case p: ProjectExec if !glutenConf.enableColumnarProject => fail(p)
      case p: FilterExec if !glutenConf.enableColumnarFilter => fail(p)
      case p: UnionExec if !glutenConf.enableColumnarUnion => fail(p)
      case p: ExpandExec if !glutenConf.enableColumnarExpand => fail(p)
      case p: SortAggregateExec if !glutenConf.forceToUseHashAgg => fail(p)
      case p: ShuffledHashJoinExec if !glutenConf.enableColumnarShuffledHashJoin => fail(p)
      case p: ShuffleExchangeExec if !glutenConf.enableColumnarShuffle => fail(p)
      case p: BroadcastExchangeExec if !glutenConf.enableColumnarBroadcastExchange => fail(p)
      case p @ (_: LocalLimitExec | _: GlobalLimitExec) if !glutenConf.enableColumnarLimit =>
        fail(p)
      case p: GenerateExec if !glutenConf.enableColumnarGenerate => fail(p)
      case p: CoalesceExec if !glutenConf.enableColumnarCoalesce => fail(p)
      case p: CartesianProductExec if !glutenConf.cartesianProductTransformerEnabled => fail(p)
      case p: TakeOrderedAndProjectExec
          if !(glutenConf.enableTakeOrderedAndProject && glutenConf.enableColumnarSort &&
            glutenConf.enableColumnarShuffle && glutenConf.enableColumnarProject) =>
        fail(p)
      case p: BroadcastHashJoinExec if !glutenConf.enableColumnarBroadcastJoin =>
        fail(p)
      case p: BroadcastNestedLoopJoinExec
          if !(glutenConf.enableColumnarBroadcastJoin &&
            glutenConf.broadcastNestedLoopJoinTransformerTransformerEnabled) =>
        fail(p)
      case p @ (_: HashAggregateExec | _: SortAggregateExec | _: ObjectHashAggregateExec)
          if !glutenConf.enableColumnarHashAgg =>
        fail(p)
      case p
          if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(
            plan) && !glutenConf.enableColumnarWindowGroupLimit =>
        fail(p)
      case p
          if HiveTableScanExecTransformer.isHiveTableScan(
            p) && !glutenConf.enableColumnarHiveTableScan =>
        fail(p)
      case p: SampleExec
          if !(glutenConf.enableColumnarSample && BackendsApiManager.getSettings
            .supportSampleExec()) =>
        fail(p)
      case _ => pass()
    }
  }

  private class FallbackByTestInjects() extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackInjects.shouldFallback(plan)) {
        return fail(plan)
      }
      pass()
    }
  }

  private class FallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (!scanOnly) {
        return pass()
      }
      // Scan-only mode
      plan match {
        case _: BatchScanExec => pass()
        case _: FileSourceScanExec => pass()
        case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
        case filter: FilterExec =>
          val childIsScan = filter.child.isInstanceOf[FileSourceScanExec] ||
            filter.child.isInstanceOf[BatchScanExec]
          if (childIsScan) {
            pass()
          } else {
            fail(filter)
          }
        case other => fail(other)
      }
    }
  }

  private class FallbackByNativeValidation() extends Validator with Logging {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      try {
        validate0(plan)
      } catch {
        case e @ (_: GlutenNotSupportException | _: UnsupportedOperationException) =>
          if (!e.isInstanceOf[GlutenNotSupportException]) {
            logDebug("Just a warning. This exception perhaps needs to be fixed.", e)
          }
          fail(
            s"${e.getMessage}, original Spark plan is " +
              s"${plan.getClass}(${plan.children.toList.map(_.getClass)})")
      }
    }

    private def validate0(plan: SparkPlan): Validator.OutCome = plan match {
      case plan: BatchScanExec =>
        val transformer =
          ScanTransformerFactory
            .createBatchScanTransformer(plan, validation = true)
            .asInstanceOf[BasicScanExecTransformer]
        transformer.doValidate().toValidatorOutcome()
      case plan: FileSourceScanExec =>
        val transformer =
          ScanTransformerFactory.createFileSourceScanTransformer(plan)
        transformer.doValidate().toValidatorOutcome()
      case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
        HiveTableScanExecTransformer.validate(plan).toValidatorOutcome()
      case plan: ProjectExec =>
        val transformer = ProjectExecTransformer(plan.projectList, plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan: FilterExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genFilterExecTransformer(plan.condition, plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan: HashAggregateExec =>
        val transformer = HashAggregateExecBaseTransformer.from(plan)
        transformer.doValidate().toValidatorOutcome()
      case plan: SortAggregateExec =>
        val transformer = HashAggregateExecBaseTransformer.from(plan)
        transformer.doValidate().toValidatorOutcome()
      case plan: ObjectHashAggregateExec =>
        val transformer = HashAggregateExecBaseTransformer.from(plan)
        transformer.doValidate().toValidatorOutcome()
      case plan: UnionExec =>
        val transformer = ColumnarUnionExec(plan.children)
        transformer.doValidate().toValidatorOutcome()
      case plan: ExpandExec =>
        val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan: WriteFilesExec =>
        val transformer = WriteFilesExecTransformer(
          plan.child,
          plan.fileFormat,
          plan.partitionColumns,
          plan.bucketSpec,
          plan.options,
          plan.staticPartitions)
        transformer.doValidate().toValidatorOutcome()
      case plan: SortExec =>
        val transformer =
          SortExecTransformer(plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
        transformer.doValidate().toValidatorOutcome()
      case plan: ShuffleExchangeExec =>
        val transformer = ColumnarShuffleExchangeExec(plan, plan.child, plan.child.output)
        transformer.doValidate().toValidatorOutcome()
      case plan: ShuffledHashJoinExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            OffloadJoin.getShjBuildSide(plan),
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin)
        transformer.doValidate().toValidatorOutcome()
      case plan: BroadcastExchangeExec =>
        val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
        transformer.doValidate().toValidatorOutcome()
      case bhj: BroadcastHashJoinExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastHashJoinExecTransformer(
            bhj.leftKeys,
            bhj.rightKeys,
            bhj.joinType,
            bhj.buildSide,
            bhj.condition,
            bhj.left,
            bhj.right,
            isNullAwareAntiJoin = bhj.isNullAwareAntiJoin)
        transformer.doValidate().toValidatorOutcome()
      case plan: SortMergeJoinExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genSortMergeJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin)
        transformer.doValidate().toValidatorOutcome()
      case plan: CartesianProductExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(plan.left, plan.right, plan.condition)
        transformer.doValidate().toValidatorOutcome()
      case plan: BroadcastNestedLoopJoinExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastNestedLoopJoinExecTransformer(
            plan.left,
            plan.right,
            plan.buildSide,
            plan.joinType,
            plan.condition)
        transformer.doValidate().toValidatorOutcome()
      case plan: WindowExec =>
        val transformer = WindowExecTransformer(
          plan.windowExpression,
          plan.partitionSpec,
          plan.orderSpec,
          plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
        val windowGroupLimitPlan = SparkShimLoader.getSparkShims
          .getWindowGroupLimitExecShim(plan)
          .asInstanceOf[WindowGroupLimitExecShim]
        val transformer = WindowGroupLimitExecTransformer(
          windowGroupLimitPlan.partitionSpec,
          windowGroupLimitPlan.orderSpec,
          windowGroupLimitPlan.rankLikeFunction,
          windowGroupLimitPlan.limit,
          windowGroupLimitPlan.mode,
          windowGroupLimitPlan.child
        )
        transformer.doValidate().toValidatorOutcome()
      case plan: CoalesceExec =>
        ColumnarCoalesceExec(plan.numPartitions, plan.child)
          .doValidate()
          .toValidatorOutcome()
      case plan: GlobalLimitExec =>
        val (limit, offset) =
          SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
        val transformer = LimitExecTransformer(plan.child, offset, limit)
        transformer.doValidate().toValidatorOutcome()
      case plan: LocalLimitExec =>
        val transformer = LimitExecTransformer(plan.child, 0L, plan.limit)
        transformer.doValidate().toValidatorOutcome()
      case plan: GenerateExec =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
          plan.generator,
          plan.requiredChildOutput,
          plan.outer,
          plan.generatorOutput,
          plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan: BatchEvalPythonExec =>
        val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
        transformer.doValidate().toValidatorOutcome()
      case plan: ArrowEvalPythonExec =>
        // When backend doesn't support ColumnarArrow or colunmnar arrow configuration not
        // enabled, we will try offloading through EvalPythonExecTransformer
        if (
          !BackendsApiManager.getSettings.supportColumnarArrowUdf() ||
          !GlutenConfig.getConf.enableColumnarArrowUDF
        ) {
          // Both CH and Velox will try using backend's built-in functions for calculate
          val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
          transformer.doValidate().toValidatorOutcome()
        }
        pass()
      case plan: TakeOrderedAndProjectExec =>
        val (limit, offset) =
          SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
        val transformer = TakeOrderedAndProjectExecTransformer(
          limit,
          plan.sortOrder,
          plan.projectList,
          plan.child,
          offset)
        transformer.doValidate().toValidatorOutcome()
      case plan: SampleExec =>
        val transformer =
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            plan.lowerBound,
            plan.upperBound,
            plan.withReplacement,
            plan.seed,
            plan.child)
        transformer.doValidate().toValidatorOutcome()
      case _ =>
        // Currently we assume a plan to be offload-able by default.
        pass()
    }
  }
}
