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
import io.glutenproject.sql.shims.SparkShimLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Murmur3Hash}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import io.glutenproject.extension.columnar.{AddTransformHintRule, FallbackMultiCodegens, RemoveTransformHintRule, StoreExpandGroupExpression, TransformHint, TransformHints}
import io.glutenproject.utils.LogLevelUtil

// This rule will conduct the conversion from Spark plan to the plan transformer.
case class TransformPreOverrides() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  /**
   * Insert a Project as the new child of Shuffle to calculate the hash expressions.
   * @param exprs hash expressions in Shuffle HashPartitioning.
   * @param child the original child of Shuffle.
   * @return a new Spark plan with Project inserted.
   */
  private def getProjectWithHash(exprs: Seq[Expression], child: SparkPlan)
  : SparkPlan = {
    val hashExpression = new Murmur3Hash(exprs)
    hashExpression.withNewChildren(exprs)
    // If the child of shuffle is also a Project, we do not merge them together here.
    // Suppose the plan is like below, in which Project2 is inserted for hash calculation.
    // Because the hash expressions are based on Project1, Project1 cannot be merged with Project2.
    // ... => Child_of_Project1(a, b)
    //     => Project1(a as c, b as d)
    //     => Project2(hash(c), c, d)
    //     => Shuffle => ...
    val project = ProjectExec(
      Seq(Alias(hashExpression, "hash_partition_key")()) ++ child.output, child)
    AddTransformHintRule().apply(project)
    replaceWithTransformerPlan(project)
  }

  /**
   * Generate a columnar plan for shuffle exchange.
   *
   * @param plan the spark plan of shuffle exchange.
   * @param child the child of shuffle exchange.
   * @param removeHashColumn whether the hash column should be removed.
   * @return a columnar shuffle exchange.
   */
  def genColumnarShuffleExchange(plan: ShuffleExchangeExec,
                                 child: SparkPlan,
                                 removeHashColumn: Boolean = false): SparkPlan = {
    if (SparkShimLoader.getSparkShims.supportAdaptiveWithExchangeConsidered(plan)) {
      ColumnarShuffleExchangeAdaptor(
        plan.outputPartitioning, child, removeHashColumn = removeHashColumn)
    } else {
      CoalesceBatchesExec(ColumnarShuffleExchangeExec(
        plan.outputPartitioning, child, removeHashColumn = removeHashColumn))
    }
  }

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = {
    TransformHints.getHint(plan) match {
      case TransformHint.TRANSFORM_SUPPORTED =>
      // supported, break
      case TransformHint.TRANSFORM_UNSUPPORTED =>
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
                shj.isSkewJoin)
            } else {
              return shj.withNewChildren(shj.children.map(replaceWithTransformerPlan))
            }
          case p =>
            return p.withNewChildren(p.children.map(replaceWithTransformerPlan))
        }
    }
    plan match {
      case plan: BatchScanExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val newPartitionFilters =
          ExpressionConverter.transformDynamicPruningExpr(plan.runtimeFilters)
        new BatchScanExecTransformer(plan.output, plan.scan, newPartitionFilters)
      case plan: FileSourceScanExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new FileSourceScanExecTransformer(
          plan.relation,
          plan.output,
          plan.requiredSchema,
          ExpressionConverter.transformDynamicPruningExpr(plan.partitionFilters),
          plan.optionalBucketSet,
          plan.optionalNumCoalescedBuckets,
          plan.dataFilters,
          plan.tableIdentifier,
          plan.disableBucketedScan)
      case plan: CoalesceExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        CoalesceExecTransformer(
          plan.numPartitions, replaceWithTransformerPlan(plan.child))
      case plan: InMemoryTableScanExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarInMemoryTableScanExec(plan.attributes, plan.predicates, plan.relation)
      case plan: ProjectExec =>
        val columnarChild = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ProjectExecTransformer(plan.projectList, columnarChild)
      case plan: FilterExec =>
        // Push down the left conditions in Filter into Scan.
        val newChild =
          if (plan.child.isInstanceOf[FileSourceScanExec] ||
            plan.child.isInstanceOf[BatchScanExec]) {
            TransformHints.getHint(plan.child) match {
              case TransformHint.TRANSFORM_SUPPORTED =>
                FilterHandler.applyFilterPushdownToScan(plan)
              case _ =>
                replaceWithTransformerPlan(plan.child)
            }
          } else {
            replaceWithTransformerPlan(plan.child)
          }
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genFilterExecTransformer(plan.condition, newChild)
      case plan: HashAggregateExec =>
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
            child)
      case plan: UnionExec =>
        val children = plan.children.map(replaceWithTransformerPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        UnionExecTransformer(children)
      case plan: CustomExpandExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ExpandExecTransformer(plan.projections, plan.groupExpression, plan.output, child)
      case plan: SortExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      case plan: TakeOrderedAndProjectExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        TakeOrderedAndProjectExecTransformer(plan.limit, plan.sortOrder, plan.projectList, child)
      case plan: ShuffleExchangeExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        if ((child.supportsColumnar || columnarConf.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec()) {
          if (BackendsApiManager.getSettings.removeHashColumnFromColumnarShuffleExchangeExec()) {
            plan.outputPartitioning match {
              case HashPartitioning(exprs, _) =>
                val projectChild = getProjectWithHash(exprs, child)
                if (projectChild.supportsColumnar) {
                  genColumnarShuffleExchange(plan, projectChild, removeHashColumn = true)
                } else {
                  plan.withNewChildren(Seq(child))
                }
              case _ =>
                genColumnarShuffleExchange(plan, child)
            }
          } else {
            genColumnarShuffleExchange(plan, child)
          }
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
            right)
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
      case plan: AQEShuffleReadExec if
          BackendsApiManager.getSettings.supportColumnarShuffleExec() =>
        plan.child match {
          case _: ColumnarShuffleExchangeAdaptor =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            CoalesceBatchesExec(ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs))
          case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeAdaptor, _) =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            CoalesceBatchesExec(ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs))
          case ShuffleQueryStageExec(_, reused: ReusedExchangeExec, _) =>
            reused match {
              case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
                logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
                CoalesceBatchesExec(
                  ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs))
              case _ =>
                plan
            }
          case _ =>
            plan
        }
      case plan: WindowExec =>
        WindowExecTransformer(
          plan.windowExpression,
          plan.partitionSpec,
          plan.orderSpec,
          replaceWithTransformerPlan(plan.child))
      case plan: GlobalLimitExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        LimitTransformer(child, 0L, plan.limit)
      case plan: LocalLimitExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val child = replaceWithTransformerPlan(plan.child)
        LimitTransformer(child, 0L, plan.limit)
      case plan: GenerateExec =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        var child = replaceWithTransformerPlan(plan.child)
        GenerateExecTransformer(plan.generator, plan.requiredChildOutput,
          plan.outer, plan.generatorOutput, child)
      case p =>
        logDebug(s"Transformation for ${p.getClass} is currently not supported.")
        val children = plan.children.map(replaceWithTransformerPlan)
        p.withNewChildren(children)
    }
  }

  /**
   * Get the build side supported by the execution of vanilla Spark.
   *
   * @param plan: shuffled hash join plan
   * @return the supported build side
   */
  private def getSparkSupportedBuildSide(plan: ShuffledHashJoinExec): BuildSide = {
    plan.joinType match {
      case LeftOuter | LeftSemi => BuildRight
      case RightOuter => BuildLeft
      case _ => plan.buildSide
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    var checkOneRowRelation = false
    if (plan.find(_.isInstanceOf[RDDScanExec]).isDefined) {
      val rddScan = plan.find(_.isInstanceOf[RDDScanExec]).get
      checkOneRowRelation = rddScan.asInstanceOf[RDDScanExec].name.equals("OneRowRelation")
    }
    if (checkOneRowRelation) {
      plan
    } else {
      val newPlan = replaceWithTransformerPlan(plan)
      planChangeLogger.logRule(ruleName, plan, newPlan)
      newPlan
    }
  }
}

// This rule will try to convert the row-to-columnar and columnar-to-row
// into columnar implementations.
case class TransformPostOverrides() extends Rule[SparkPlan] {
  val columnarConf = GlutenConfig.getSessionConf
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"ColumnarPostOverrides RowToArrowColumnarExec(${child.getClass})")
      BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeExec) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: BroadcastQueryStageExec) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithTransformerPlan(child.child)))
    case plan: ColumnarToRowExec =>
      if (columnarConf.enableNativeColumnarToRow) {
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"ColumnarPostOverrides GlutenColumnarToRowExecBase(${child.getClass})")
        val nativeConversion =
          BackendsApiManager.getSparkPlanExecApiInstance.genNativeColumnarToRowExec(child)
        if (nativeConversion.doValidate()) {
          nativeConversion
        } else {
          logDebug("NativeColumnarToRow : Falling back to ColumnarToRow...")
          plan.withNewChildren(plan.children.map(replaceWithTransformerPlan))
        }
      } else {
        val children = plan.children.map(replaceWithTransformerPlan)
        plan.withNewChildren(children)
      }
    case r: SparkPlan
      if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
        c.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          if (columnarConf.enableNativeColumnarToRow) {
            val child = replaceWithTransformerPlan(c.child)
            val nativeConversion =
              BackendsApiManager.getSparkPlanExecApiInstance.genNativeColumnarToRowExec(child)
            if (nativeConversion.doValidate()) {
              nativeConversion
            } else {
              logInfo("NativeColumnarToRow : Falling back to ColumnarToRow...")
              c.withNewChildren(c.children.map(replaceWithTransformerPlan))
            }
          } else {
            c.withNewChildren(c.children.map(replaceWithTransformerPlan))
          }
        case other =>
          replaceWithTransformerPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithTransformerPlan)
      p.withNewChildren(children)
  }

  // apply for the physical not final plan
  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = replaceWithTransformerPlan(plan)
    planChangeLogger.logRule(ruleName, plan, newPlan)
    newPlan
  }
}

case class ColumnarOverrideRules(session: SparkSession)
  extends ColumnarRule with Logging with LogLevelUtil {

  lazy val transformPlanLogLevel = GlutenConfig.getSessionConf.transformPlanLogLevel
  @transient private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()
  // Do not create rules in class initialization as we should access SQLConf
  // while creating the rules. At this time SQLConf may not be there yet.

  def preOverrides: List[SparkSession => Rule[SparkPlan]] =
    List((_: SparkSession) => StoreExpandGroupExpression(),
      (_: SparkSession) => FallbackMultiCodegens(),
      (_: SparkSession) => AddTransformHintRule(),
      (_: SparkSession) => TransformPreOverrides(),
      (_: SparkSession) => RemoveTransformHintRule()) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPreRules()

  def postOverrides: List[SparkSession => Rule[SparkPlan]] =
    List((_: SparkSession) => TransformPostOverrides()) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPostRules() :::
      List((_: SparkSession) => ColumnarCollapseCodegenStages(GlutenConfig.getSessionConf))

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    val supportedGluten = BackendsApiManager.getSparkPlanExecApiInstance.supportedGluten(
      nativeEngineEnabled,
      plan)

    if (supportedGluten) {
      var overridden: SparkPlan = plan
      val startTime = System.nanoTime()
      logOnLevel(
        transformPlanLogLevel,
        s"preColumnarTransitions preOverriden plan:\n${plan.toString}")
      preOverrides.foreach { r =>
        overridden = r(session)(overridden)
        planChangeLogger.logRule(r(session).ruleName, plan, overridden)
      }
      logOnLevel(
        transformPlanLogLevel,
        s"preColumnarTransitions afterOverriden plan:\n${overridden.toString}")
      logOnLevel(
        transformPlanLogLevel,
        s"preTransform SparkPlan took: ${(System.nanoTime() - startTime) / 1000000.0} ms.")
      overridden
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    val supportedGluten = BackendsApiManager.getSparkPlanExecApiInstance.supportedGluten(
      nativeEngineEnabled,
      plan)

    logOnLevel(
      transformPlanLogLevel,
      s"postColumnarTransitions preOverriden plan:\n${plan.toString}")
    if (supportedGluten) {
      var overridden: SparkPlan = plan
      val startTime = System.nanoTime()
      postOverrides.foreach { r =>
        overridden = r(session)(overridden)
        planChangeLogger.logRule(r(session).ruleName, plan, overridden)
      }
      logOnLevel(
        transformPlanLogLevel,
        s"postColumnarTransitions afterOverriden plan:\n${overridden.toString}")
      logOnLevel(
        transformPlanLogLevel,
        s"postTransform SparkPlan took: ${(System.nanoTime() - startTime) / 1000000.0} ms.")
      overridden
    } else {
      plan
    }
  }

  def nativeEngineEnabled: Boolean = GlutenConfig.getSessionConf.enableNativeEngine

}

object ColumnarOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(ColumnarOverrideRules)
  }
}
