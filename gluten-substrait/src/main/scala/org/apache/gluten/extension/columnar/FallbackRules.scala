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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.extension.columnar.FallbackTags.EncodeFallbackTagImplicits
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

import org.apache.commons.lang3.exception.ExceptionUtils

sealed trait FallbackTag {
  val stacktrace: Option[String] =
    if (FallbackTags.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None

  def reason(): String
}

object FallbackTag {

  /** A tag that stores one reason text of fall back. */
  case class Appendable(override val reason: String) extends FallbackTag

  /**
   * A tag that stores reason text of fall back. Other reasons will be discarded when this tag is
   * added to plan.
   */
  case class Exclusive(override val reason: String) extends FallbackTag

  trait Converter[T] {
    def from(obj: T): Option[FallbackTag]
  }

  object Converter {
    implicit def asIs[T <: FallbackTag]: Converter[T] = (tag: T) => Some(tag)

    implicit object FromString extends Converter[String] {
      override def from(reason: String): Option[FallbackTag] = Some(Appendable(reason))
    }

    implicit object FromValidationResult extends Converter[ValidationResult] {
      override def from(result: ValidationResult): Option[FallbackTag] = {
        if (result.ok()) {
          return None
        }
        Some(Appendable(result.reason()))
      }
    }
  }
}

object FallbackTags {
  val TAG: TreeNodeTag[FallbackTag] =
    TreeNodeTag[FallbackTag]("org.apache.gluten.FallbackTag")

  val DEBUG = false

  /**
   * If true, the plan node will be guaranteed fallback to Vanilla plan node while being
   * implemented.
   *
   * If false, the plan still has chance to be turned into "non-transformable" in any another
   * validation rule. So user should not consider the plan "transformable" unless all validation
   * rules are passed.
   */
  def nonEmpty(plan: SparkPlan): Boolean = {
    getOption(plan).nonEmpty
  }

  /**
   * If true, it implies the plan maybe transformable during validation phase but not guaranteed,
   * since another validation rule could turn it to "non-transformable" before implementing the plan
   * within Gluten transformers. If false, the plan node will be guaranteed fallback to Vanilla plan
   * node while being implemented.
   */
  def maybeOffloadable(plan: SparkPlan): Boolean = !nonEmpty(plan)

  def add[T](plan: TreeNode[_], t: T)(implicit converter: FallbackTag.Converter[T]): Unit = {
    val tagOption = getOption(plan)
    val newTagOption = converter.from(t)

    val mergedTagOption: Option[FallbackTag] =
      (tagOption ++ newTagOption).reduceOption[FallbackTag] {
        // New tag comes while the plan was already tagged, merge.
        case (_, exclusive: FallbackTag.Exclusive) =>
          exclusive
        case (exclusive: FallbackTag.Exclusive, _) =>
          exclusive
        case (l: FallbackTag.Appendable, r: FallbackTag.Appendable) =>
          FallbackTag.Appendable(s"${l.reason}; ${r.reason}")
      }
    mergedTagOption
      .foreach(mergedTag => plan.setTagValue(TAG, mergedTag))
  }

  def addRecursively[T](plan: TreeNode[_], t: T)(implicit
      converter: FallbackTag.Converter[T]): Unit = {
    plan.foreach {
      case _: GlutenPlan => // ignore
      case other: TreeNode[_] => add(other, t)
    }
  }

  def untag(plan: TreeNode[_]): Unit = {
    plan.unsetTagValue(TAG)
  }

  def get(plan: TreeNode[_]): FallbackTag = {
    getOption(plan).getOrElse(
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString()))
  }

  def getOption(plan: TreeNode[_]): Option[FallbackTag] = {
    plan.getTagValue(TAG)
  }

  implicit class EncodeFallbackTagImplicits(result: ValidationResult) {
    def tagOnFallback(plan: TreeNode[_]): Unit = {
      if (result.ok()) {
        return
      }
      add(plan, result)
    }
  }
}

case class FallbackOnANSIMode(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (GlutenConfig.getConf.enableAnsiMode) {
      plan.foreach(FallbackTags.add(_, "does not support ansi mode"))
    }
    plan
  }
}

case class FallbackMultiCodegens(session: SparkSession) extends Rule[SparkPlan] {
  lazy val columnarConf: GlutenConfig = GlutenConfig.getConf
  lazy val physicalJoinOptimize = columnarConf.enablePhysicalJoinOptimize
  lazy val optimizeLevel: Integer = columnarConf.physicalJoinOptimizationThrottle

  def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: SortMergeJoinExec if GlutenConfig.getConf.forceShuffledHashJoin =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case _ => false
    }

  def addFallbackTag(plan: SparkPlan): SparkPlan = {
    FallbackTags.add(plan, "fallback multi codegens")
    plan
  }

  def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  def isAQEShuffleReadExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: AQEShuffleReadExec => true
      case _ => false
    }
  }

  def addFallbackTagRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        addFallbackTag(p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        addFallbackTag(p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        addFallbackTag(p.withNewChildren(p.children.map(addFallbackTagRecursive)))
      case p if !supportCodegen(p) =>
        p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(tagOnFallbackForMultiCodegens))
      case p: QueryStageExec => p
      case p => addFallbackTag(p.withNewChildren(p.children.map(addFallbackTagRecursive)))
    }
  }

  def tagOnFallbackForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if existsMultiCodegens(plan) =>
        addFallbackTagRecursive(plan)
      case other =>
        other.withNewChildren(other.children.map(tagOnFallbackForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (physicalJoinOptimize) {
      tagOnFallbackForMultiCodegens(plan)
    } else plan
  }
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class AddFallbackTagRule() extends Rule[SparkPlan] {
  import AddFallbackTagRule._
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
          // If filter expressions aren't empty, we need to transform the inner operators.
          if (plan.runtimeFilters.isEmpty) {
            val transformer =
              ScanTransformerFactory
                .createBatchScanTransformer(plan, validation = true)
                .asInstanceOf[BasicScanExecTransformer]
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: FileSourceScanExec =>
          // If filter expressions aren't empty, we need to transform the inner operators.
          if (plan.partitionFilters.isEmpty) {
            val transformer =
              ScanTransformerFactory.createFileSourceScanTransformer(plan)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          HiveTableScanExecTransformer.validate(plan).tagOnFallback(plan)
        case plan: ProjectExec =>
          val transformer = ProjectExecTransformer(plan.projectList, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case plan: FilterExec =>
          val transformer = FilterTransformerFactory.createFilterTransformer(plan)
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
          val transformer = LimitTransformer(plan.child, offset, limit)
          transformer.doValidate().tagOnFallback(plan)
        case plan: LocalLimitExec =>
          val transformer = LimitTransformer(plan.child, 0L, plan.limit)
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

object AddFallbackTagRule {
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

case class RemoveFallbackTagRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(FallbackTags.untag)
    plan
  }
}
