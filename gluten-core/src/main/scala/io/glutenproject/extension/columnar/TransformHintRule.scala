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
package io.glutenproject.extension.columnar

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution._
import io.glutenproject.extension.{GlutenPlan, ValidationResult}
import io.glutenproject.utils.PhysicalPlanSelector

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.StringType

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.control.Breaks.{break, breakable}

trait TransformHint {
  val stacktrace: Option[String] =
    if (TransformHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class TRANSFORM_SUPPORTED() extends TransformHint
case class TRANSFORM_UNSUPPORTED(reason: Option[String]) extends TransformHint

object TransformHints {
  val TAG: TreeNodeTag[TransformHint] =
    TreeNodeTag[TransformHint]("io.glutenproject.transformhint")

  val DEBUG = false

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isTransformable(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_SUPPORTED]
  }

  def isNotTransformable(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_UNSUPPORTED]
  }

  def tag(plan: SparkPlan, hint: TransformHint): Unit = {
    val mergedHint = getHintOption(plan)
      .map {
        case originalHint @ TRANSFORM_UNSUPPORTED(Some(originalReason)) =>
          hint match {
            case TRANSFORM_UNSUPPORTED(Some(newReason)) =>
              TRANSFORM_UNSUPPORTED(Some(originalReason + "; " + newReason))
            case TRANSFORM_UNSUPPORTED(None) =>
              originalHint
            case _ =>
              throw new UnsupportedOperationException(
                "Plan was already tagged as non-transformable, " +
                  s"cannot mark it as transformable after that:\n${plan.toString()}")
          }
        case _ =>
          hint
      }
      .getOrElse(hint)
    plan.setTagValue(TAG, mergedHint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_SUPPORTED())
  }

  def tagNotTransformable(plan: SparkPlan, validationResult: ValidationResult): Unit = {
    if (!validationResult.isValid) {
      tag(plan, TRANSFORM_UNSUPPORTED(validationResult.reason))
    }
  }

  def tagNotTransformable(plan: SparkPlan, reason: String): Unit = {
    tag(plan, TRANSFORM_UNSUPPORTED(Some(reason)))
  }

  def tagAllNotTransformable(plan: SparkPlan, reason: String): Unit = {
    plan.foreach {
      case _: GlutenPlan => // ignore
      case other => tagNotTransformable(other, reason)
    }
  }

  def getHint(plan: SparkPlan): TransformHint = {
    if (!isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString())
    }
    plan.getTagValue(TAG).getOrElse(throw new IllegalStateException())
  }

  def getHintOption(plan: SparkPlan): Option[TransformHint] = {
    plan.getTagValue(TAG)
  }
}

// Holds rules which have higher privilege to tag (not) transformable before AddTransformHintRule.
object TagBeforeTransformHits {
  val ruleBuilders: List[SparkSession => Rule[SparkPlan]] = {
    List(FallbackOnANSIMode, FallbackMultiCodegens)
  }
}

case class FallbackOnANSIMode(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    if (GlutenConfig.getConf.enableAnsiMode) {
      plan.foreach(TransformHints.tagNotTransformable(_, "does not support ansi mode"))
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
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case other => false
    }

  def tagNotTransformable(plan: SparkPlan): SparkPlan = {
    TransformHints.tagNotTransformable(plan, "fallback multi codegens")
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

  def tagNotTransformableRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
      case p if !supportCodegen(p) =>
        // insert row guard them recursively
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p: QueryStageExec => p
      case p => tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
    }
  }

  def tagNotTransformableForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if existsMultiCodegens(plan) =>
        tagNotTransformableRecursive(plan)
      case other =>
        other.withNewChildren(other.children.map(tagNotTransformableForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    if (physicalJoinOptimize) {
      tagNotTransformableForMultiCodegens(plan)
    } else plan
  }
}

/**
 * This rule plans [[RDDScanExec]] with a fake schema to make gluten work, because gluten does not
 * support empty output relation, see [[FallbackEmptySchemaRelation]].
 */
case class PlanOneRowRelation(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.getConf.enableOneRowRelationColumnar) {
      return plan
    }

    plan.transform {
      // We should make sure the output does not change, e.g.
      // Window
      //   OneRowRelation
      case u: UnaryExecNode
          if u.child.isInstanceOf[RDDScanExec] &&
            u.child.asInstanceOf[RDDScanExec].name == "OneRowRelation" &&
            u.outputSet != u.child.outputSet =>
        val rdd = spark.sparkContext.parallelize(InternalRow(null) :: Nil, 1)
        val attr = AttributeReference("fake_column", StringType)()
        u.withNewChildren(RDDScanExec(attr :: Nil, rdd, "OneRowRelation") :: Nil)
    }
  }
}

case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p =>
      if (BackendsApiManager.getSettings.fallbackOnEmptySchema(p)) {
        if (p.children.exists(_.output.isEmpty)) {
          // Some backends are not eligible to offload plan with zero-column input.
          // If any child have empty output, mark the plan and that child as UNSUPPORTED.
          TransformHints.tagNotTransformable(p, "at least one of its children has empty output")
          p.children.foreach {
            child =>
              if (child.output.isEmpty) {
                TransformHints.tagNotTransformable(
                  child,
                  "at least one of its children has empty output")
              }
          }
        }
      }
      p
  }
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class AddTransformHintRule() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val optimizeLevel: Integer = columnarConf.physicalJoinOptimizationThrottle
  val scanOnly: Boolean = columnarConf.enableScanOnly
  val enableColumnarShuffle: Boolean =
    !scanOnly && BackendsApiManager.getSettings.supportColumnarShuffleExec()
  val enableColumnarSort: Boolean = !scanOnly && columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = !scanOnly && columnarConf.enableColumnarWindow
  val enableColumnarSortMergeJoin: Boolean = !scanOnly &&
    BackendsApiManager.getSettings.supportSortMergeJoinExec()
  val enableColumnarBatchScan: Boolean = columnarConf.enableColumnarBatchScan
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarHiveTableScan: Boolean = columnarConf.enableColumnarHiveTableScan
  val enableColumnarProject: Boolean = !scanOnly && columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarHashAgg: Boolean = !scanOnly && columnarConf.enableColumnarHashAgg
  val enableColumnarUnion: Boolean = !scanOnly && columnarConf.enableColumnarUnion
  val enableColumnarExpand: Boolean = !scanOnly && columnarConf.enableColumnarExpand
  val enableColumnarShuffledHashJoin: Boolean =
    !scanOnly && columnarConf.enableColumnarShuffledHashJoin
  val enableColumnarBroadcastExchange: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastJoin && columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastJoin && columnarConf.enableColumnarBroadcastExchange
  val enableColumnarArrowUDF: Boolean = !scanOnly && columnarConf.enableColumnarArrowUDF
  val enableColumnarLimit: Boolean = !scanOnly && columnarConf.enableColumnarLimit
  val enableColumnarGenerate: Boolean = !scanOnly && columnarConf.enableColumnarGenerate
  val enableColumnarCoalesce: Boolean = !scanOnly && columnarConf.enableColumnarCoalesce
  val enableTakeOrderedAndProject: Boolean =
    !scanOnly && columnarConf.enableTakeOrderedAndProject &&
      enableColumnarSort && enableColumnarLimit && enableColumnarShuffle && enableColumnarProject

  def apply(plan: SparkPlan): SparkPlan = {
    addTransformableTags(plan)
  }

  /** Inserts a transformable tag on top of those that are not supported. */
  private def addTransformableTags(plan: SparkPlan): SparkPlan = {
    addTransformableTag(plan)
    plan.withNewChildren(plan.children.map(addTransformableTags))
  }

  private def addTransformableTag(plan: SparkPlan): Unit = {
    if (TransformHints.isAlreadyTagged(plan)) {
      logDebug(
        s"Skip adding transformable tag, since plan already tagged as " +
          s"${TransformHints.getHint(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) {
            TransformHints.tagNotTransformable(plan, "columnar BatchScan is disabled")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.runtimeFilters.nonEmpty) {
              TransformHints.tagTransformable(plan)
            } else {
              val transformer =
                new BatchScanExecTransformer(plan.output, plan.scan, plan.runtimeFilters)
              TransformHints.tag(plan, transformer.doValidate().toTransformHint)
            }
          }
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar FileScan is not enabled in FileSourceScanExec")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.partitionFilters.nonEmpty) {
              TransformHints.tagTransformable(plan)
            } else {
              val transformer = new FileSourceScanExecTransformer(
                plan.relation,
                plan.output,
                plan.requiredSchema,
                plan.partitionFilters,
                plan.optionalBucketSet,
                plan.optionalNumCoalescedBuckets,
                plan.dataFilters,
                plan.tableIdentifier,
                plan.disableBucketedScan
              )
              TransformHints.tag(plan, transformer.doValidate().toTransformHint)
            }
          }
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          if (!enableColumnarHiveTableScan) {
            TransformHints.tagNotTransformable(plan, "columnar hive table scan is disabled")
          } else {
            TransformHints.tag(plan, HiveTableScanExecTransformer.validate(plan).toTransformHint)
          }
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            TransformHints.tagNotTransformable(plan, "columnar project is disabled")
          } else {
            val transformer = ProjectExecTransformer(plan.projectList, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: FilterExec =>
          val childIsScan = plan.child.isInstanceOf[FileSourceScanExec] ||
            plan.child.isInstanceOf[BatchScanExec]
          // When scanOnly is enabled, filter after scan will be offloaded.
          if ((!scanOnly && !enableColumnarFilter) || (scanOnly && !childIsScan)) {
            TransformHints.tagNotTransformable(
              plan,
              "ScanOnly enabled and plan child is not Scan in FilterExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(plan.condition, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAggregate is not enabled in HashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: SortAggregateExec =>
          if (!BackendsApiManager.getSettings.replaceSortAggWithHashAgg) {
            TransformHints.tagNotTransformable(plan, "replaceSortAggWithHashAgg is not enabled")
          }
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAgg is not enabled in SortAggregateExec")
          }
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance
            .genHashAggregateExecTransformer(
              plan.requiredChildDistributionExpressions,
              plan.groupingExpressions,
              plan.aggregateExpressions,
              plan.aggregateAttributes,
              plan.initialInputBufferOffset,
              plan.resultExpressions,
              plan.child
            )
          TransformHints.tag(plan, transformer.doValidate().toTransformHint)
        case plan: ObjectHashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAgg is not enabled in ObjectHashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            TransformHints.tagNotTransformable(plan, "columnar Union is not enabled in UnionExec")
          } else {
            val transformer = UnionExecTransformer(plan.children)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ExpandExec =>
          if (!enableColumnarExpand) {
            TransformHints.tagNotTransformable(plan, "columnar Expand is not enabled in ExpandExec")
          } else {
            val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: SortExec =>
          if (!enableColumnarSort) {
            TransformHints.tagNotTransformable(plan, "columnar Sort is not enabled in SortExec")
          } else {
            val transformer =
              SortExecTransformer(plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar Shuffle is not enabled in ShuffleExchangeExec")
          } else {
            val transformer = ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              plan.child,
              plan.shuffleOrigin,
              plan.child.output)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar shufflehashjoin is not enabled in ShuffledHashJoinExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genShuffledHashJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.buildSide,
                plan.condition,
                plan.left,
                plan.right,
                plan.isSkewJoin)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: BroadcastExchangeExec =>
          // columnar broadcast is enabled only when columnar bhj is enabled.
          if (!enableColumnarBroadcastExchange) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar BroadcastExchange is not enabled in BroadcastExchangeExec")
          } else {
            val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case bhj: BroadcastHashJoinExec =>
          // FIXME Hongze: In following codes we perform a lot of if-else conditions to
          //  make sure the broadcast exchange and broadcast hash-join are of same type,
          //  either vanilla or columnar. In order to simplify the codes we have to do
          //  some tricks around C2R and R2C to make them adapt to columnar broadcast.
          //  Currently their doBroadcast() methods just propagate child's broadcast
          //  payloads which is not right in speaking of columnar.
          if (!enableColumnarBroadcastJoin) {
            TransformHints.tagNotTransformable(
              bhj,
              "columnar BroadcastJoin is not enabled in BroadcastHashJoinExec")
          } else {
            val isBhjTransformable: ValidationResult = {
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
              transformer.doValidate()
            }
            val buildSidePlan = bhj.buildSide match {
              case BuildLeft => bhj.left
              case BuildRight => bhj.right
            }

            val maybeExchange = buildSidePlan
              .find {
                case BroadcastExchangeExec(_, _) => true
                case _ => false
              }
              .map(_.asInstanceOf[BroadcastExchangeExec])

            maybeExchange match {
              case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
                TransformHints.tag(bhj, isBhjTransformable.toTransformHint)
                TransformHints.tagNotTransformable(exchange, isBhjTransformable)
              case None =>
                // we are in AQE, find the hidden exchange
                // FIXME did we consider the case that AQE: OFF && Reuse: ON ?
                var maybeHiddenExchange: Option[BroadcastExchangeLike] = None
                breakable {
                  buildSidePlan.foreach {
                    case e: BroadcastExchangeLike =>
                      maybeHiddenExchange = Some(e)
                      break
                    case t: BroadcastQueryStageExec =>
                      t.plan.foreach {
                        case e2: BroadcastExchangeLike =>
                          maybeHiddenExchange = Some(e2)
                          break
                        case r: ReusedExchangeExec =>
                          r.child match {
                            case e2: BroadcastExchangeLike =>
                              maybeHiddenExchange = Some(e2)
                              break
                            case _ =>
                          }
                        case _ =>
                      }
                    case _ =>
                  }
                }
                // restriction to force the hidden exchange to be found
                val exchange = maybeHiddenExchange.get
                // to conform to the underlying exchange's type, columnar or vanilla
                exchange match {
                  case BroadcastExchangeExec(mode, child) =>
                    TransformHints.tagNotTransformable(
                      bhj,
                      "it's a materialized broadcast exchange or reused broadcast exchange")
                  case ColumnarBroadcastExchangeExec(mode, child) =>
                    if (!isBhjTransformable.isValid) {
                      throw new IllegalStateException(
                        s"BroadcastExchange has already been" +
                          s" transformed to columnar version but BHJ is determined as" +
                          s" non-transformable: ${bhj.toString()}")
                    }
                    TransformHints.tagTransformable(bhj)
                }
            }
          }
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar sort merge join is not enabled or join type is FullOuter")
          } else {
            val transformer = SortMergeJoinExecTransformer(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: WindowExec =>
          if (!enableColumnarWindow) {
            TransformHints.tagNotTransformable(plan, "columnar window is not enabled in WindowExec")
          } else {
            val transformer = WindowExecTransformer(
              plan.windowExpression,
              plan.partitionSpec,
              plan.orderSpec,
              plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: CoalesceExec =>
          if (!enableColumnarCoalesce) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar coalesce is not enabled in CoalesceExec")
          } else {
            val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: GlobalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: LocalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: GenerateExec =>
          if (!enableColumnarGenerate) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar generate is not enabled in GenerateExec")
          } else {
            val transformer = GenerateExecTransformer(
              plan.generator,
              plan.requiredChildOutput,
              plan.outer,
              plan.generatorOutput,
              plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: EvalPythonExec =>
          val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
          TransformHints.tag(plan, transformer.doValidate().toTransformHint)
        case _: AQEShuffleReadExec =>
          TransformHints.tagTransformable(plan)
        case plan: TakeOrderedAndProjectExec =>
          if (!enableTakeOrderedAndProject) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar topK is not enabled in TakeOrderedAndProjectExec")
          } else {
            var tagged: ValidationResult = null
            val limitPlan = LimitTransformer(plan.child, 0, plan.limit)
            tagged = limitPlan.doValidate()
            if (tagged.isValid) {
              val sortPlan = SortExecTransformer(plan.sortOrder, false, plan.child)
              tagged = sortPlan.doValidate()
            }
            if (tagged.isValid) {
              val projectPlan = ProjectExecTransformer(plan.projectList, plan.child)
              tagged = projectPlan.doValidate()
            }
            TransformHints.tag(plan, tagged.toTransformHint)
          }
        case _ =>
          // currently we assume a plan to be transformable by default
          TransformHints.tagTransformable(plan)
      }
    } catch {
      case e: UnsupportedOperationException =>
        TransformHints.tagNotTransformable(
          plan,
          s"${e.getMessage}, original sparkplan is " +
            s"${plan.getClass}(${plan.children.toList.map(_.getClass)})")
    }
  }

  implicit class EncodeTransformableTagImplicits(validationResult: ValidationResult) {
    def toTransformHint: TransformHint = {
      if (validationResult.isValid) {
        TRANSFORM_SUPPORTED()
      } else {
        TRANSFORM_UNSUPPORTED(validationResult.reason)
      }
    }
  }
}

case class RemoveTransformHintRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(TransformHints.untag)
    plan
  }
}
