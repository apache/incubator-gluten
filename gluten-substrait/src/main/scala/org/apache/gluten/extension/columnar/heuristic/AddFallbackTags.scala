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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.{BasicScanExecTransformer, ColumnarCoalesceExec, ColumnarUnionExec, ExpandExecTransformer, HashAggregateExecBaseTransformer, LimitExecTransformer, ProjectExecTransformer, ScanTransformerFactory, SortExecTransformer, TakeOrderedAndProjectExecTransformer, WindowExecTransformer, WindowGroupLimitExecTransformer, WriteFilesExecTransformer}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CoalesceExec, ColumnarBroadcastExchangeExec, ColumnarShuffleExchangeExec, ExpandExec, FileSourceScanExec, FilterExec, GenerateExec, GlobalLimitExec, LocalLimitExec, ProjectExec, SampleExec, SortExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class AddFallbackTags() extends Rule[SparkPlan] {
  import AddFallbackTags._
  private val glutenConf: GlutenConfig = GlutenConfig.getConf
  private val validator = Validators
    .builder()
    .fallbackByHint()
    .fallbackIfScanOnlyWithFilterPushed(glutenConf.enableScanOnly)
    .fallbackComplexExpressions()
    .fallbackByBackendSettings()
    .fallbackByUserOptions()
    .fallbackByTestInjects()
    .build()

  def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp { case p => addFallbackTag(p) }
    plan
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    val outcome = validator.validate(plan)
    outcome match {
      case Validator.Failed(reason) =>
        FallbackTags.add(plan, reason)
        return
      case Validator.Passed =>
    }

    try {
      plan match {
        case plan: BatchScanExec =>
          val transformer =
            ScanTransformerFactory
              .createBatchScanTransformer(plan, validation = true)
              .asInstanceOf[BasicScanExecTransformer]
          transformer.doValidate().tagOnFallback(plan)
        case plan: FileSourceScanExec =>
          val transformer =
            ScanTransformerFactory.createFileSourceScanTransformer(plan)
          transformer.doValidate().tagOnFallback(plan)
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          HiveTableScanExecTransformer.validate(plan).tagOnFallback(plan)
        case plan: ProjectExec =>
          val transformer = ProjectExecTransformer(plan.projectList, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: FilterExec =>
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance
            .genFilterExecTransformer(plan.condition, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: HashAggregateExec =>
          val transformer = HashAggregateExecBaseTransformer.from(plan)
          transformer.doValidate().tagOnFallback(plan)
        case plan: SortAggregateExec =>
          val transformer = HashAggregateExecBaseTransformer.from(plan)
          transformer.doValidate().tagOnFallback(plan)
        case plan: ObjectHashAggregateExec =>
          val transformer = HashAggregateExecBaseTransformer.from(plan)
          transformer.doValidate().tagOnFallback(plan)
        case plan: UnionExec =>
          val transformer = ColumnarUnionExec(plan.children)
          transformer.doValidate().tagOnFallback(plan)
        case plan: ExpandExec =>
          val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: WriteFilesExec =>
          val transformer = WriteFilesExecTransformer(
            plan.child,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
          transformer.doValidate().tagOnFallback(plan)
        case plan: SortExec =>
          val transformer =
            SortExecTransformer(plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
          transformer.doValidate().tagOnFallback(plan)
        case plan: ShuffleExchangeExec =>
          val transformer = ColumnarShuffleExchangeExec(plan, plan.child, plan.child.output)
          transformer.doValidate().tagOnFallback(plan)
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
          transformer.doValidate().tagOnFallback(plan)
        case plan: BroadcastExchangeExec =>
          val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
          transformer.doValidate().tagOnFallback(plan)
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
          transformer.doValidate().tagOnFallback(plan)
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
          transformer.doValidate().tagOnFallback(plan)
        case plan: CartesianProductExec =>
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance
            .genCartesianProductExecTransformer(plan.left, plan.right, plan.condition)
          transformer.doValidate().tagOnFallback(plan)
        case plan: BroadcastNestedLoopJoinExec =>
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance
            .genBroadcastNestedLoopJoinExecTransformer(
              plan.left,
              plan.right,
              plan.buildSide,
              plan.joinType,
              plan.condition)
          transformer.doValidate().tagOnFallback(plan)
        case plan: WindowExec =>
          val transformer = WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
          transformer.doValidate().tagOnFallback(plan)
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
          transformer.doValidate().tagOnFallback(plan)
        case plan: CoalesceExec =>
          ColumnarCoalesceExec(plan.numPartitions, plan.child)
            .doValidate()
            .tagOnFallback(plan)
        case plan: GlobalLimitExec =>
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          val transformer = LimitExecTransformer(plan.child, offset, limit)
          transformer.doValidate().tagOnFallback(plan)
        case plan: LocalLimitExec =>
          val transformer = LimitExecTransformer(plan.child, 0L, plan.limit)
          transformer.doValidate().tagOnFallback(plan)
        case plan: GenerateExec =>
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            plan.generator,
            plan.requiredChildOutput,
            plan.outer,
            plan.generatorOutput,
            plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: BatchEvalPythonExec =>
          val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: ArrowEvalPythonExec =>
          // When backend doesn't support ColumnarArrow or colunmnar arrow configuration not
          // enabled, we will try offloading through EvalPythonExecTransformer
          if (
            !BackendsApiManager.getSettings.supportColumnarArrowUdf() ||
            !GlutenConfig.getConf.enableColumnarArrowUDF
          ) {
            // Both CH and Velox will try using backend's built-in functions for calculate
            val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: TakeOrderedAndProjectExec =>
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          val transformer = TakeOrderedAndProjectExecTransformer(
            limit,
            plan.sortOrder,
            plan.projectList,
            plan.child,
            offset)
          transformer.doValidate().tagOnFallback(plan)
        case plan: SampleExec =>
          val transformer =
            BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
              plan.lowerBound,
              plan.upperBound,
              plan.withReplacement,
              plan.seed,
              plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case _ =>
        // Currently we assume a plan to be offload-able by default.
      }
    } catch {
      case e @ (_: GlutenNotSupportException | _: UnsupportedOperationException) =>
        FallbackTags.add(
          plan,
          s"${e.getMessage}, original Spark plan is " +
            s"${plan.getClass}(${plan.children.toList.map(_.getClass)})")
        if (!e.isInstanceOf[GlutenNotSupportException]) {
          logDebug("Just a warning. This exception perhaps needs to be fixed.", e)
        }
    }
  }
}

object AddFallbackTags {
  implicit private class ValidatorBuilderImplicits(builder: Validators.Builder) {

    /**
     * Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. Also, passes
     * validation on filter for the exception that filter + scan is detected. Because filters can be
     * pushed into scan then the filter conditions will be processed only in scan.
     */
    def fallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean): Validators.Builder = {
      builder.add(new FallbackIfScanOnlyWithFilterPushed(scanOnly))
      builder
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
}
