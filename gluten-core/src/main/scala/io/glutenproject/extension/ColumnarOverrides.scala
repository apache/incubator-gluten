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
package io.glutenproject.extension

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution._
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.extension.columnar._
import io.glutenproject.metrics.GlutenTimeMetric
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.utils.{LogLevelUtil, PhysicalPlanSelector, PlanUtil}

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.util.SparkRuleUtil

import scala.collection.mutable.ListBuffer

// This rule will conduct the conversion from Spark plan to the plan transformer.
case class TransformPreOverrides() extends Rule[SparkPlan] with LogLevelUtil {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  /**
   * Generate a plan for hash aggregation.
   * @param plan:
   *   the original Spark plan.
   * @return
   *   the actually used plan for execution.
   */
  private def genHashAggregateExec(plan: HashAggregateExec): SparkPlan = {
    val newChild = replaceWithTransformerPlan(plan.child)
    def transformHashAggregate(): GlutenPlan = {
      BackendsApiManager.getSparkPlanExecApiInstance
        .genHashAggregateExecTransformer(
          plan.requiredChildDistributionExpressions,
          plan.groupingExpressions,
          plan.aggregateExpressions,
          plan.aggregateAttributes,
          plan.initialInputBufferOffset,
          plan.resultExpressions,
          newChild
        )
    }

    // If child's output is empty, fallback or offload both the child and aggregation.
    if (plan.child.output.isEmpty && BackendsApiManager.getSettings.fallbackAggregateWithChild()) {
      newChild match {
        case _: TransformSupport =>
          // If the child is transformable, transform aggregation as well.
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          transformHashAggregate()
        case p: SparkPlan if PlanUtil.isGlutenTableCache(p) =>
          transformHashAggregate()
        case _ =>
          // If the child is not transformable, transform the grandchildren only.
          TransformHints.tagNotTransformable(plan, "child output schema is empty")
          val grandChildren = plan.child.children.map(child => replaceWithTransformerPlan(child))
          plan.withNewChildren(Seq(plan.child.withNewChildren(grandChildren)))
      }
    } else {
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      transformHashAggregate()
    }
  }

  /**
   * Generate a plan for filter.
   * @param plan:
   *   the original Spark plan.
   * @return
   *   the actually used plan for execution.
   */
  private def genFilterExec(plan: FilterExec): SparkPlan = {
    // FIXME: Filter push-down should be better done by Vanilla Spark's planner or by
    //  a individual rule.
    val scan = plan.child
    // Push down the left conditions in Filter into FileSourceScan.
    val newChild: SparkPlan = scan match {
      case _: FileSourceScanExec | _: BatchScanExec =>
        TransformHints.getHint(scan) match {
          case TRANSFORM_SUPPORTED() =>
            val newScan = FilterHandler.applyFilterPushdownToScan(plan)
            newScan match {
              case ts: TransformSupport if ts.doValidate().isValid => ts
              case _ => replaceWithTransformerPlan(scan)
            }
          case _ => replaceWithTransformerPlan(scan)
        }
      case _ => replaceWithTransformerPlan(plan.child)
    }
    logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
    BackendsApiManager.getSparkPlanExecApiInstance
      .genFilterExecTransformer(plan.condition, newChild)
  }

  def applyScanNotTransformable(plan: SparkPlan): SparkPlan = plan match {
    case plan: FileSourceScanExec =>
      val newPartitionFilters =
        ExpressionConverter.transformDynamicPruningExpr(plan.partitionFilters)
      val newSource = plan.copy(partitionFilters = newPartitionFilters)
      if (plan.logicalLink.nonEmpty) {
        newSource.setLogicalLink(plan.logicalLink.get)
      }
      TransformHints.tag(newSource, TransformHints.getHint(plan))
      newSource
    case plan: BatchScanExec =>
      val newPartitionFilters: Seq[Expression] = plan.scan match {
        case scan: FileScan =>
          ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters)
        case _ =>
          ExpressionConverter.transformDynamicPruningExpr(plan.runtimeFilters)
      }
      val newSource = plan.copy(runtimeFilters = newPartitionFilters)
      if (plan.logicalLink.nonEmpty) {
        newSource.setLogicalLink(plan.logicalLink.get)
      }
      TransformHints.tag(newSource, TransformHints.getHint(plan))
      newSource
    case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
      val newPartitionFilters: Seq[Expression] = ExpressionConverter.transformDynamicPruningExpr(
        HiveTableScanExecTransformer.getPartitionFilters(plan))
      val newSource = HiveTableScanExecTransformer.copyWith(plan, newPartitionFilters)
      if (plan.logicalLink.nonEmpty) {
        newSource.setLogicalLink(plan.logicalLink.get)
      }
      TransformHints.tag(newSource, TransformHints.getHint(plan))
      newSource
    case other =>
      throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
  }

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = {
    TransformHints.getHint(plan) match {
      case _: TRANSFORM_SUPPORTED =>
      // supported, break
      case _: TRANSFORM_UNSUPPORTED =>
        logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
        plan match {
          case shj: ShuffledHashJoinExec =>
            if (BackendsApiManager.getSettings.recreateJoinExecOnFallback()) {
              // Because we manually removed the build side limitation for LeftOuter, LeftSemi and
              // RightOuter, need to change the build side back if this join fallback into vanilla
              // Spark for execution.
              return ShuffledHashJoinExec(
                shj.leftKeys,
                shj.rightKeys,
                shj.joinType,
                getSparkSupportedBuildSide(shj),
                shj.condition,
                replaceWithTransformerPlan(shj.left),
                replaceWithTransformerPlan(shj.right),
                shj.isSkewJoin
              )
            } else {
              return shj.withNewChildren(shj.children.map(replaceWithTransformerPlan))
            }
          case plan: BatchScanExec =>
            return applyScanNotTransformable(plan)
          case plan: FileSourceScanExec =>
            return applyScanNotTransformable(plan)
          case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
            return applyScanNotTransformable(plan)
          case p =>
            return p.withNewChildren(p.children.map(replaceWithTransformerPlan))
        }
    }
    plan match {
      case plan: BatchScanExec =>
        applyScanTransformer(plan)
      case plan: FileSourceScanExec =>
        applyScanTransformer(plan)
      case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
        applyScanTransformer(plan)
      case plan: CoalesceExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        CoalesceExecTransformer(plan.numPartitions, replaceWithTransformerPlan(plan.child))
      case plan: ProjectExec =>
        val columnarChild = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ProjectExecTransformer(plan.projectList, columnarChild)
      case plan: FilterExec =>
        genFilterExec(plan)
      case plan: HashAggregateExec =>
        genHashAggregateExec(plan)
      case plan: SortAggregateExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genHashAggregateExecTransformer(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child match {
              case sort: SortExecTransformer if !sort.global =>
                replaceWithTransformerPlan(sort.child)
              case sort: SortExec if !sort.global =>
                replaceWithTransformerPlan(sort.child)
              case _ => replaceWithTransformerPlan(plan.child)
            }
          )
      case plan: ObjectHashAggregateExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genHashAggregateExecTransformer(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            child
          )
      case plan: UnionExec =>
        val children = plan.children.map(replaceWithTransformerPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarUnionExec(children)
      case plan: ExpandExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ExpandExecTransformer(plan.projections, plan.output, child)
      case plan: WriteFilesExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val writeTransformer = WriteFilesExecTransformer(
          child,
          plan.fileFormat,
          plan.partitionColumns,
          plan.bucketSpec,
          plan.options,
          plan.staticPartitions)
        BackendsApiManager.getSparkPlanExecApiInstance.createColumnarWriteFilesExec(
          writeTransformer,
          plan.fileFormat,
          plan.partitionColumns,
          plan.bucketSpec,
          plan.options,
          plan.staticPartitions
        )
      case plan: SortExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      case plan: TakeOrderedAndProjectExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
        TakeOrderedAndProjectExecTransformer(limit, plan.sortOrder, plan.projectList, child, offset)
      case plan: ShuffleExchangeExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        if (
          (child.supportsColumnar || columnarConf.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec()
        ) {
          BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(plan, child)
        } else {
          plan.withNewChildren(Seq(child))
        }
      case plan: ShuffledHashJoinExec =>
        val left = replaceWithTransformerPlan(plan.left)
        val right = replaceWithTransformerPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: SortMergeJoinExec =>
        val left = replaceWithTransformerPlan(plan.left)
        val right = replaceWithTransformerPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        SortMergeJoinExecTransformer(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      case plan: BroadcastExchangeExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarBroadcastExchangeExec(plan.mode, child)
      case plan: BroadcastHashJoinExec =>
        val left = replaceWithTransformerPlan(plan.left)
        val right = replaceWithTransformerPlan(plan.right)
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right,
            isNullAwareAntiJoin = plan.isNullAwareAntiJoin)
      case plan: CartesianProductExec =>
        val left = replaceWithTransformerPlan(plan.left)
        val right = replaceWithTransformerPlan(plan.right)
        BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(left, right, plan.condition)
      case plan: WindowExec =>
        WindowExecTransformer(
          plan.windowExpression,
          plan.partitionSpec,
          plan.orderSpec,
          replaceWithTransformerPlan(plan.child))
      case plan: GlobalLimitExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
        LimitTransformer(child, offset, limit)
      case plan: LocalLimitExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        LimitTransformer(child, 0L, plan.limit)
      case plan: GenerateExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        GenerateExecTransformer(
          plan.generator,
          plan.requiredChildOutput,
          plan.outer,
          plan.generatorOutput,
          child)
      case plan: EvalPythonExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
      case p =>
        logDebug(s"Transformation for ${p.getClass} is currently not supported.")
        val children = plan.children.map(replaceWithTransformerPlan)
        p.withNewChildren(children)
    }
  }

  /**
   * Get the build side supported by the execution of vanilla Spark.
   *
   * @param plan:
   *   shuffled hash join plan
   * @return
   *   the supported build side
   */
  private def getSparkSupportedBuildSide(plan: ShuffledHashJoinExec): BuildSide = {
    plan.joinType match {
      case LeftOuter | LeftSemi => BuildRight
      case RightOuter => BuildLeft
      case _ => plan.buildSide
    }
  }

  /**
   * Apply scan transformer for file source and batch source,
   *   1. create new filter and scan transformer, 2. validate, tag new scan as unsupported if
   *      failed, 3. return new source.
   */
  def applyScanTransformer(plan: SparkPlan): SparkPlan = plan match {
    case plan: FileSourceScanExec =>
      val transformer = ScanTransformerFactory.createFileSourceScanTransformer(plan)
      val validationResult = transformer.doValidate()
      if (validationResult.isValid) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        val newSource = plan.copy(partitionFilters = transformer.partitionFilters)
        TransformHints.tagNotTransformable(newSource, validationResult.reason.get)
        newSource
      }
    case plan: BatchScanExec =>
      ScanTransformerFactory.createBatchScanTransformer(plan)

    case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
      // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
      val newPartitionFilters: Seq[Expression] = ExpressionConverter.transformDynamicPruningExpr(
        HiveTableScanExecTransformer.getPartitionFilters(plan))
      val hiveTableScanExecTransformer =
        BackendsApiManager.getSparkPlanExecApiInstance.genHiveTableScanExecTransformer(plan)
      val validateResult = hiveTableScanExecTransformer.doValidate()
      if (validateResult.isValid) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        return hiveTableScanExecTransformer
      }
      logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
      val newSource = HiveTableScanExecTransformer.copyWith(plan, newPartitionFilters)
      TransformHints.tagNotTransformable(newSource, validateResult.reason.get)
      newSource
    case other =>
      throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = replaceWithTransformerPlan(plan)

    planChangeLogger.logRule(ruleName, plan, newPlan)
    newPlan
  }
}

// This rule will try to convert the row-to-columnar and columnar-to-row
// into native implementations.
case class TransformPostOverrides(isAdaptiveContext: Boolean) extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  def transformColumnarToRowExec(plan: ColumnarToRowExec): SparkPlan = {
    if (columnarConf.enableNativeColumnarToRow) {
      val child = replaceWithTransformerPlan(plan.child)

      if (!PlanUtil.outputNativeColumnarData(child)) {
        TransformHints.tagNotTransformable(plan, "child is not gluten plan")
        plan.withNewChildren(plan.children.map(replaceWithTransformerPlan))
      } else {
        logDebug(s"ColumnarPostOverrides ColumnarToRowExecBase(${child.nodeName})")
        val nativeConversion =
          BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(child)
        val validationResult = nativeConversion.doValidate()
        if (validationResult.isValid) {
          nativeConversion
        } else {
          TransformHints.tagNotTransformable(plan, validationResult)
          plan.withNewChildren(plan.children.map(replaceWithTransformerPlan))
        }
      }
    } else {
      plan.withNewChildren(plan.children.map(replaceWithTransformerPlan))
    }
  }

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"ColumnarPostOverrides RowToColumnarExec(${child.getClass})")
      BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(child)
    // The ColumnarShuffleExchangeExec node may be the top node, so we cannot remove it.
    // e.g. select /* REPARTITION */ from testData, and the AQE create shuffle stage will check
    // if the transformed is instance of ShuffleExchangeLike, so we need to remove it in AQE
    // mode have tested gluten-it TPCH when AQE OFF
    case ColumnarToRowExec(child: ColumnarShuffleExchangeExec) if isAdaptiveContext =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeExec) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: BroadcastQueryStageExec) =>
      replaceWithTransformerPlan(child)
    // `InMemoryTableScanExec` internally supports ColumnarToRow
    case ColumnarToRowExec(child: SparkPlan) if PlanUtil.isGlutenTableCache(child) =>
      child
    case plan: ColumnarToRowExec =>
      transformColumnarToRowExec(plan)
    case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar &&
          r.children.exists(_.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan
      r.withNewChildren(r.children.map {
        // `InMemoryTableScanExec` internally supports ColumnarToRow
        case c: ColumnarToRowExec if !PlanUtil.isGlutenTableCache(c.child) =>
          transformColumnarToRowExec(c)
        case other =>
          replaceWithTransformerPlan(other)
      })
    case p =>
      p.withNewChildren(p.children.map(replaceWithTransformerPlan))
  }

  // apply for the physical not final plan
  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = replaceWithTransformerPlan(plan)
    planChangeLogger.logRule(ruleName, plan, newPlan)
    newPlan
  }
}

// This rule will try to add RowToColumnarExecBase and ColumnarToRowExec
// to support vanilla columnar operators.
case class VanillaColumnarPlanOverrides(session: SparkSession) extends Rule[SparkPlan] {
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  private def replaceWithVanillaColumnarToRow(plan: SparkPlan): SparkPlan = plan match {
    case _ if PlanUtil.isGlutenColumnarOp(plan) =>
      plan.withNewChildren(plan.children.map {
        c =>
          val child = replaceWithVanillaColumnarToRow(c)
          if (PlanUtil.isVanillaColumnarOp(child)) {
            BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(
              ColumnarToRowExec(child))
          } else {
            child
          }
      })
    case _ =>
      plan.withNewChildren(plan.children.map(replaceWithVanillaColumnarToRow))
  }

  private def replaceWithVanillaRowToColumnar(plan: SparkPlan): SparkPlan = plan match {
    case _ if PlanUtil.isVanillaColumnarOp(plan) =>
      plan.withNewChildren(plan.children.map {
        c =>
          val child = replaceWithVanillaRowToColumnar(c)
          if (PlanUtil.isGlutenColumnarOp(child)) {
            RowToColumnarExec(
              BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(child))
          } else {
            child
          }
      })
    case _ =>
      plan.withNewChildren(plan.children.map(replaceWithVanillaRowToColumnar))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = replaceWithVanillaRowToColumnar(replaceWithVanillaColumnarToRow(plan))
    planChangeLogger.logRule(ruleName, plan, newPlan)
    newPlan
  }
}

object ColumnarOverrideRules {
  val GLUTEN_IS_ADAPTIVE_CONTEXT = "gluten.isAdaptiveContext"

  def rewriteSparkPlanRule(): Rule[SparkPlan] = {
    val rewriteRules = Seq(RewriteMultiChildrenCount, PullOutPreProject, PullOutPostProject)
    new RewriteSparkPlanRulesManager(rewriteRules)
  }
}

case class ColumnarOverrideRules(session: SparkSession)
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  import ColumnarOverrideRules._

  private lazy val transformPlanLogLevel = GlutenConfig.getConf.transformPlanLogLevel
  @transient private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 14

  // Holds the original plan for possible entire fallback.
  private val localOriginalPlans: ThreadLocal[ListBuffer[SparkPlan]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[SparkPlan])
  private val localIsAdaptiveContextFlags: ThreadLocal[ListBuffer[Boolean]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[Boolean])

  // Do not create rules in class initialization as we should access SQLConf
  // while creating the rules. At this time SQLConf may not be there yet.

  // Just for test use.
  def enableAdaptiveContext(): Unit = {
    session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, "true")
  }

  def isAdaptiveContext: Boolean =
    Option(session.sparkContext.getLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT))
      .getOrElse("false")
      .toBoolean ||
      localIsAdaptiveContextFlags.get().head

  private def setAdaptiveContext(): Unit = {
    val traceElements = Thread.currentThread.getStackTrace
    assert(
      traceElements.length > aqeStackTraceIndex,
      s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
    // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
    // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
    // columnar rule will be applied in adaptive execution context. This part of code
    // needs to be carefully checked when supporting higher versions of spark to make
    // sure the calling stack has not been changed.
    localIsAdaptiveContextFlags
      .get()
      .prepend(
        traceElements(aqeStackTraceIndex).getClassName
          .equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def resetAdaptiveContext(): Unit =
    localIsAdaptiveContextFlags.get().remove(0)

  private def setOriginalPlan(plan: SparkPlan): Unit = {
    localOriginalPlans.get.prepend(plan)
  }

  private def originalPlan: SparkPlan = {
    val plan = localOriginalPlans.get.head
    assert(plan != null)
    plan
  }

  private def resetOriginalPlan(): Unit = localOriginalPlans.get.remove(0)

  private def preOverrides(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (spark: SparkSession) => FallbackOnANSIMode(spark),
      (spark: SparkSession) => FallbackMultiCodegens(spark),
      (spark: SparkSession) => PlanOneRowRelation(spark),
      (_: SparkSession) => FallbackEmptySchemaRelation()
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarValidationRules() :::
      List(
        (_: SparkSession) => rewriteSparkPlanRule(),
        (_: SparkSession) => AddTransformHintRule(),
        (_: SparkSession) => FallbackBloomFilterAggIfNeeded(),
        (_: SparkSession) => TransformPreOverrides(),
        (_: SparkSession) => RemoveNativeWriteFilesSortAndProject(),
        (spark: SparkSession) => RewriteTransformer(spark),
        (_: SparkSession) => EnsureLocalSortRequirements,
        (_: SparkSession) => CollapseProjectExecTransformer
      ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPreRules() :::
      SparkRuleUtil.extendedColumnarRules(session, GlutenConfig.getConf.extendedColumnarPreRules)
  }

  private def fallbackPolicy(): List[SparkSession => Rule[SparkPlan]] = {
    List((_: SparkSession) => ExpandFallbackPolicy(isAdaptiveContext, originalPlan))
  }

  private def postOverrides(): List[SparkSession => Rule[SparkPlan]] =
    List(
      (_: SparkSession) => TransformPostOverrides(isAdaptiveContext),
      (s: SparkSession) => VanillaColumnarPlanOverrides(s)
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPostRules() :::
      List((_: SparkSession) => ColumnarCollapseTransformStages(GlutenConfig.getConf)) :::
      SparkRuleUtil.extendedColumnarRules(session, GlutenConfig.getConf.extendedColumnarPostRules)

  private def finallyRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (s: SparkSession) => GlutenFallbackReporter(GlutenConfig.getConf, s),
      (_: SparkSession) => RemoveTransformHintRule()
    )
  }

  override def preColumnarTransitions: Rule[SparkPlan] = plan =>
    PhysicalPlanSelector.maybe(session, plan) {
      setAdaptiveContext()
      setOriginalPlan(plan)
      transformPlan(preOverrides(), plan, "pre")
    }

  override def postColumnarTransitions: Rule[SparkPlan] = plan =>
    PhysicalPlanSelector.maybe(session, plan) {
      val planWithFallbackPolicy = transformPlan(fallbackPolicy(), plan, "fallback")
      val finalPlan = planWithFallbackPolicy match {
        case FallbackNode(fallbackPlan) =>
          // we should use vanilla c2r rather than native c2r,
          // and there should be no `GlutenPlan` any more,
          // so skip the `postOverrides()`.
          fallbackPlan
        case plan =>
          transformPlan(postOverrides(), plan, "post")
      }
      resetOriginalPlan()
      resetAdaptiveContext()
      transformPlan(finallyRules(), finalPlan, "final")
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
}

object ColumnarOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(spark => ColumnarOverrideRules(spark))
  }
}
