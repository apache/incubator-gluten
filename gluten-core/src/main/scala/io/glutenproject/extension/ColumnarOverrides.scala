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
import io.glutenproject.utils.{ColumnarShuffleUtil, LogLevelUtil, PhysicalPlanSelector}

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BindReferences, BoundReference, Expression, Murmur3Hash, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.util.SparkRuleUtil

// This rule will conduct the conversion from Spark plan to the plan transformer.
case class TransformPreOverrides(isAdaptiveContext: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  val reuseSubquery: Boolean = isAdaptiveContext && conf.subqueryReuseEnabled

  /**
   * Insert a Project as the new child of Shuffle to calculate the hash expressions.
   * @param exprs
   *   hash expressions in Shuffle HashPartitioning.
   * @param child
   *   the original child of Shuffle.
   * @return
   *   a new Spark plan with Project inserted.
   */
  private def getProjectWithHash(exprs: Seq[Expression], child: SparkPlan): SparkPlan = {
    val hashExpression = new Murmur3Hash(exprs)
    hashExpression.withNewChildren(exprs)
    // If the child of shuffle is also a Project, we do not merge them together here.
    // Suppose the plan is like below, in which Project2 is inserted for hash calculation.
    // Because the hash expressions are based on Project1, Project1 cannot be merged with Project2.
    // ... => Child_of_Project1(a, b)
    //     => Project1(a as c, b as d)
    //     => Project2(hash(c), c, d)
    //     => Shuffle => ...
    val project =
      ProjectExec(Seq(Alias(hashExpression, "hash_partition_key")()) ++ child.output, child)
    AddTransformHintRule().apply(project)
    TransformHints.getHint(project) match {
      case _: TRANSFORM_SUPPORTED => replaceWithTransformerPlan(project)
      case _: TRANSFORM_UNSUPPORTED => project
    }
  }

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
        case i: InMemoryTableScanExec if InMemoryTableScanHelper.isGlutenTableCache(i) =>
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
    // Push down the left conditions in Filter into Scan.
    val newChild: SparkPlan =
      if (
        plan.child.isInstanceOf[FileSourceScanExec] ||
        plan.child.isInstanceOf[BatchScanExec]
      ) {
        TransformHints.getHint(plan.child) match {
          case TRANSFORM_SUPPORTED() =>
            val newScan = FilterHandler.applyFilterPushdownToScan(plan, reuseSubquery)
            newScan match {
              case ts: TransformSupport =>
                if (ts.doValidate().isValid) {
                  ts
                } else {
                  replaceWithTransformerPlan(plan.child)
                }
              case p: SparkPlan => p
            }
          case _ =>
            replaceWithTransformerPlan(plan.child)
        }
      } else {
        replaceWithTransformerPlan(plan.child)
      }
    logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
    BackendsApiManager.getSparkPlanExecApiInstance
      .genFilterExecTransformer(plan.condition, newChild)
  }

  /**
   * If there are expressions (not field reference) in the partitioning's children, add a projection
   * before shuffle exchange and make a new partitioning with the old expressions replaced by the
   * result columns from the projection.
   */
  private def addProjectionForShuffleExchange(
      plan: ShuffleExchangeExec): (Int, Partitioning, SparkPlan) = {
    def selectExpressions(
        exprs: Seq[Expression],
        attributes: Seq[Attribute]): (Seq[NamedExpression], Seq[Int]) = {
      var expressionPos = Seq[Int]()
      var projectExpressions = Seq[NamedExpression]()

      exprs.foreach(
        expr => {
          if (!expr.isInstanceOf[AttributeReference]) {
            val n = projectExpressions.size
            val namedExpression = Alias(expr, s"projected_partitioning_value_$n")()
            projectExpressions = projectExpressions :+ namedExpression
            expressionPos = expressionPos :+ (attributes.size + n)
          } else {
            // the new projected columns are appended at the end
            expressionPos = expressionPos :+ BindReferences
              .bindReference(expr, attributes)
              .asInstanceOf[BoundReference]
              .ordinal
          }
        })
      (projectExpressions, expressionPos)
    }
    plan.outputPartitioning match {
      case HashPartitioning(exprs, numPartitions) =>
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        val project = replaceWithTransformerPlan(
          AddTransformHintRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newExprs = Seq[Expression]()
        for (i <- exprs.indices) {
          val pos = newExpressionsPosition(i)
          newExprs = newExprs :+ project.output(pos)
        }
        (projectExpressions.size, HashPartitioning(newExprs, numPartitions), project)
      case RangePartitioning(orderings, numPartitions) =>
        val exprs = orderings.map(ordering => ordering.child)
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        val project = replaceWithTransformerPlan(
          AddTransformHintRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newOrderings = Seq[SortOrder]()
        for (i <- orderings.indices) {
          val oldOrdering = orderings(i)
          val pos = newExpressionsPosition(i)
          val ordering = SortOrder(
            project.output(pos),
            oldOrdering.direction,
            oldOrdering.nullOrdering,
            oldOrdering.sameOrderExpressions)
          newOrderings = newOrderings :+ ordering
        }
        (projectExpressions.size, RangePartitioning(newOrderings, numPartitions), project)
      case _ =>
        // no change for other cases
        (0, plan.outputPartitioning, plan.child)
    }
  }

  def applyScanNotTransformable(plan: SparkPlan): SparkPlan = plan match {
    case plan: FileSourceScanExec =>
      val newPartitionFilters =
        ExpressionConverter.transformDynamicPruningExpr(plan.partitionFilters, reuseSubquery)
      val newSource = plan.copy(partitionFilters = newPartitionFilters)
      if (plan.logicalLink.nonEmpty) {
        newSource.setLogicalLink(plan.logicalLink.get)
      }
      TransformHints.tag(newSource, TransformHints.getHint(plan))
      newSource
    case plan: BatchScanExec =>
      val newPartitionFilters: Seq[Expression] = plan.scan match {
        case scan: FileScan =>
          ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters, reuseSubquery)
        case _ =>
          ExpressionConverter.transformDynamicPruningExpr(plan.runtimeFilters, reuseSubquery)
      }
      val newSource = plan.copy(runtimeFilters = newPartitionFilters)
      if (plan.logicalLink.nonEmpty) {
        newSource.setLogicalLink(plan.logicalLink.get)
      }
      TransformHints.tag(newSource, TransformHints.getHint(plan))
      newSource
    case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
      val newPartitionFilters: Seq[Expression] = ExpressionConverter.transformDynamicPruningExpr(
        HiveTableScanExecTransformer.getPartitionFilters(plan),
        reuseSubquery)
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
        UnionExecTransformer(children)
      case plan: ExpandExec =>
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ExpandExecTransformer(plan.projections, plan.output, child)
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
        if (
          (child.supportsColumnar || columnarConf.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec()
        ) {
          if (BackendsApiManager.getSettings.removeHashColumnFromColumnarShuffleExchangeExec()) {
            plan.outputPartitioning match {
              case HashPartitioning(exprs, _) =>
                val projectChild = getProjectWithHash(exprs, child)
                if (projectChild.supportsColumnar) {
                  ColumnarShuffleUtil.genColumnarShuffleExchange(
                    plan,
                    projectChild,
                    projectChild.output.drop(1))
                } else {
                  plan.withNewChildren(Seq(child))
                }
              case _ =>
                ColumnarShuffleUtil.genColumnarShuffleExchange(plan, child, null)
            }
          } else if (
            BackendsApiManager.getSettings.supportShuffleWithProject(
              plan.outputPartitioning,
              plan.child)
          ) {
            val (projectColumnNumber, newPartitioning, newChild) =
              addProjectionForShuffleExchange(plan)

            if (projectColumnNumber != 0) {
              if (newChild.supportsColumnar) {
                val newPlan = ShuffleExchangeExec(newPartitioning, newChild, plan.shuffleOrigin)
                // the new projections columns are appended at the end.
                ColumnarShuffleUtil.genColumnarShuffleExchange(
                  newPlan,
                  newChild,
                  newChild.output.dropRight(projectColumnNumber))
              } else {
                // It's the case that partitioning expressions could be offloaded into native.
                plan.withNewChildren(Seq(child))
              }
            } else {
              ColumnarShuffleUtil.genColumnarShuffleExchange(plan, child, null)
            }
          } else {
            ColumnarShuffleUtil.genColumnarShuffleExchange(plan, child, null)
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
      case plan: AQEShuffleReadExec
          if BackendsApiManager.getSettings.supportColumnarShuffleExec() =>
        plan.child match {
          case ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _) =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs)
          case ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: ColumnarShuffleExchangeExec), _) =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs)
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
      case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance.genHiveTableScanExecTransformer(plan)
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
      val newPartitionFilters =
        ExpressionConverter.transformDynamicPruningExpr(plan.partitionFilters, reuseSubquery)
      val transformer = new FileSourceScanExecTransformer(
        plan.relation,
        plan.output,
        plan.requiredSchema,
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        plan.dataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      val validationResult = transformer.doValidate()
      if (validationResult.isValid) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        val newSource = plan.copy(partitionFilters = newPartitionFilters)
        TransformHints.tagNotTransformable(newSource, validationResult.reason.get)
        newSource
      }
    case plan: BatchScanExec =>
      val newPartitionFilters: Seq[Expression] = plan.scan match {
        case scan: FileScan =>
          ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters, reuseSubquery)
        case _ =>
          ExpressionConverter.transformDynamicPruningExpr(plan.runtimeFilters, reuseSubquery)
      }
      val transformer = new BatchScanExecTransformer(plan.output, plan.scan, newPartitionFilters)
      val validationResult = transformer.doValidate()
      if (validationResult.isValid) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        val newSource = plan.copy(runtimeFilters = newPartitionFilters)
        TransformHints.tagNotTransformable(newSource, validationResult.reason.get)
        newSource
      }
    case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
      // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
      val newPartitionFilters: Seq[Expression] = ExpressionConverter.transformDynamicPruningExpr(
        HiveTableScanExecTransformer.getPartitionFilters(plan),
        reuseSubquery)
      val hiveTableScanExecTransformer =
        BackendsApiManager.getSparkPlanExecApiInstance.genHiveTableScanExecTransformer(plan)
      val validateResult = hiveTableScanExecTransformer.doValidate()
      if (validateResult.isValid) {
        return hiveTableScanExecTransformer
      }
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
    case plan: ColumnarToRowExec =>
      transformColumnarToRowExec(plan)
    case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar &&
          r.children.exists(_.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan
      r.withNewChildren(r.children.map {
        case c: ColumnarToRowExec =>
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

object InMemoryTableScanHelper {
  def isGlutenTableCache(i: InMemoryTableScanExec): Boolean = {
    // `ColumnarCachedBatchSerializer` is at velox module, so use class name here
    i.relation.cacheBuilder.serializer.getClass.getSimpleName == "ColumnarCachedBatchSerializer" &&
    i.supportsColumnar
  }
}

// This rule will try to add RowToColumnarExecBase and ColumnarToRowExec
// to support vanilla columnar scan.
case class VanillaColumnarPlanOverrides(session: SparkSession) extends Rule[SparkPlan] {
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  private def replaceWithVanillaColumnarToRow(plan: SparkPlan): SparkPlan = plan match {
    case c2r: ColumnarToRowExecBase if isVanillaColumnarReader(c2r.child) =>
      ColumnarToRowExec(c2r.child)
    case c2r: ColumnarToRowExec if isVanillaColumnarReader(c2r.child) =>
      c2r
    case _ if isVanillaColumnarReader(plan) =>
      BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(ColumnarToRowExec(plan))
    case _ =>
      plan.withNewChildren(plan.children.map(replaceWithVanillaColumnarToRow))
  }

  private def isVanillaColumnarReader(plan: SparkPlan): Boolean = plan match {
    case _: BatchScanExec | _: DataSourceScanExec =>
      !plan.isInstanceOf[GlutenPlan] && plan.supportsColumnar
    case i: InMemoryTableScanExec =>
      if (InMemoryTableScanHelper.isGlutenTableCache(i)) {
        // `InMemoryTableScanExec` do not need extra RowToColumnar or ColumnarToRow
        false
      } else {
        !plan.isInstanceOf[GlutenPlan] && plan.supportsColumnar
      }
    case _ => false
  }

  def apply(plan: SparkPlan): SparkPlan =
    if (GlutenConfig.getConf.enableVanillaVectorizedReaders) {
      val newPlan = replaceWithVanillaColumnarToRow(plan)
      planChangeLogger.logRule(ruleName, plan, newPlan)
      newPlan
    } else {
      plan
    }
}

object ColumnarOverrideRules {
  val GLUTEN_IS_ADAPTIVE_CONTEXT = "gluten.isAdaptiveContext"
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
  private val localOriginalPlan = new ThreadLocal[SparkPlan]

  // Do not create rules in class initialization as we should access SQLConf
  // while creating the rules. At this time SQLConf may not be there yet.

  // Just for test use.
  def enableAdaptiveContext(): Unit = {
    session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, "true")
  }

  def isAdaptiveContext: Boolean =
    session.sparkContext.getLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT).toBoolean

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
    session.sparkContext.setLocalProperty(
      GLUTEN_IS_ADAPTIVE_CONTEXT,
      traceElements(aqeStackTraceIndex).getClassName
        .equals(AdaptiveSparkPlanExec.getClass.getName)
        .toString)
  }

  private def resetAdaptiveContext(): Unit =
    session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, null)

  private def setOriginalPlan(plan: SparkPlan): Unit = localOriginalPlan.set(plan)

  private def originalPlan: SparkPlan = {
    val plan = localOriginalPlan.get()
    assert(plan != null)
    plan
  }

  private def resetOriginalPlan(): Unit = localOriginalPlan.remove()

  private def preOverrides(): List[SparkSession => Rule[SparkPlan]] = {
    val tagBeforeTransformHitsRules =
      if (isAdaptiveContext) {
        // When AQE is supported, rules are applied in ColumnarQueryStagePrepOverrides
        List.empty
      } else {
        TagBeforeTransformHits.ruleBuilders
      }
    tagBeforeTransformHitsRules :::
      List(
        (spark: SparkSession) => PlanOneRowRelation(spark),
        (_: SparkSession) => FallbackEmptySchemaRelation(),
        (_: SparkSession) => AddTransformHintRule(),
        (_: SparkSession) => TransformPreOverrides(isAdaptiveContext),
        (_: SparkSession) => EnsureLocalSortRequirements
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
