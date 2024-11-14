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
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Validators {
  def builder(): Builder = Builder()

  class Builder private {
    private val conf = GlutenConfig.getConf
    private val settings = BackendsApiManager.getSettings
    private val buffer: ListBuffer[Validator] = mutable.ListBuffer()

    /** Fails validation if a plan node was already tagged with TRANSFORM_UNSUPPORTED. */
    def fallbackByHint(): Builder = {
      buffer += FallbackByHint
      this
    }

    /**
     * Fails validation if a plan node includes an expression that is considered too complex to
     * executed by native library. By default, we use a threshold option in config to make the
     * decision.
     */
    def fallbackComplexExpressions(): Builder = {
      buffer += new FallbackComplexExpressions(conf.fallbackExpressionsThreshold)
      this
    }

    /** Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. */
    def fallbackIfScanOnly(): Builder = {
      buffer += new FallbackIfScanOnly(conf.enableScanOnly)
      this
    }

    /**
     * Fails validation if native-execution of a plan node is not supported by current backend
     * implementation by checking the active BackendSettings.
     */
    def fallbackByBackendSettings(): Builder = {
      buffer += new FallbackByBackendSettings(settings)
      this
    }

    /**
     * Fails validation if native-execution of a plan node is disabled by Gluten/Spark
     * configuration.
     */
    def fallbackByUserOptions(): Builder = {
      buffer += new FallbackByUserOptions(conf)
      this
    }

    def fallbackByTestInjects(): Builder = {
      buffer += new FallbackByTestInjects()
      this
    }

    /** Add a custom validator to pipeline. */
    def add(validator: Validator): Builder = {
      buffer += validator
      this
    }

    def build(): Validator = {
      if (buffer.isEmpty) {
        NoopValidator
      } else {
        new ValidatorPipeline(buffer.toSeq)
      }
    }
  }

  private object Builder {
    def apply(): Builder = new Builder()
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

  private class ValidatorPipeline(validators: Seq[Validator]) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      val init: Validator.OutCome = pass()
      val finalOut = validators.foldLeft(init) {
        case (out, validator) =>
          out match {
            case Validator.Passed => validator.validate(plan)
            case Validator.Failed(_) => out
          }
      }
      finalOut
    }
  }

  private object NoopValidator extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = pass()
  }
}
