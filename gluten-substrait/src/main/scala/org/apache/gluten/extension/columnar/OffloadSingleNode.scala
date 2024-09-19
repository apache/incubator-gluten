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
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

/**
 * Converts a vanilla Spark plan node into Gluten plan node. Gluten plan is supposed to be executed
 * in native, and the internals of execution is subject by backend's implementation.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 */
sealed trait OffloadSingleNode extends Logging {
  def offload(plan: SparkPlan): SparkPlan
}

// Aggregation transformation.
case class OffloadAggregate() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case plan if FallbackTags.nonEmpty(plan) =>
      plan
    case agg: HashAggregateExec =>
      genHashAggregateExec(agg)
    case other => other
  }

  /**
   * Generate a plan for hash aggregation.
   *
   * @param plan
   *   : the original Spark plan.
   * @return
   *   the actually used plan for execution.
   */
  private def genHashAggregateExec(plan: HashAggregateExec): SparkPlan = {
    if (FallbackTags.nonEmpty(plan)) {
      return plan
    }

    val aggChild = plan.child

    // If child's output is empty, fallback or offload both the child and aggregation.
    if (
      aggChild.output.isEmpty && BackendsApiManager.getSettings
        .fallbackAggregateWithEmptyOutputChild()
    ) {
      aggChild match {
        case _: TransformSupport =>
          // If the child is transformable, transform aggregation as well.
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case p: SparkPlan if PlanUtil.isGlutenTableCache(p) =>
          HashAggregateExecBaseTransformer.from(plan)
        case _ =>
          // If the child is not transformable, do not transform the agg.
          FallbackTags.add(plan, "child output schema is empty")
          plan
      }
    } else {
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      HashAggregateExecBaseTransformer.from(plan)
    }
  }
}

// Exchange transformation.
case class OffloadExchange() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case s: ShuffleExchangeExec
        if (s.child.supportsColumnar || GlutenConfig.getConf.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec() =>
      logDebug(s"Columnar Processing for ${s.getClass} is currently supported.")
      BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(s)
    case b: BroadcastExchangeExec =>
      val child = b.child
      logDebug(s"Columnar Processing for ${b.getClass} is currently supported.")
      ColumnarBroadcastExchangeExec(b.mode, child)
    case other => other
  }
}

// Join transformation.
case class OffloadJoin() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (FallbackTags.nonEmpty(plan)) {
      logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
      return plan
    }
    plan match {
      case plan: ShuffledHashJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            OffloadJoin.getShjBuildSide(plan),
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: SortMergeJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genSortMergeJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: BroadcastHashJoinExec =>
        val left = plan.left
        val right = plan.right
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
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(left, right, plan.condition)
      case plan: BroadcastNestedLoopJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastNestedLoopJoinExecTransformer(
            left,
            right,
            plan.buildSide,
            plan.joinType,
            plan.condition)
      case other => other
    }
  }
}

object OffloadJoin {
  def getShjBuildSide(shj: ShuffledHashJoinExec): BuildSide = {
    val leftBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(shj.joinType)
    val rightBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(shj.joinType)

    assert(leftBuildable || rightBuildable)

    if (!leftBuildable) {
      return BuildRight
    }
    if (!rightBuildable) {
      return BuildLeft
    }

    // Both left and right are buildable. Find out the better one.
    if (!GlutenConfig.getConf.shuffledHashJoinOptimizeBuildSide) {
      // User disabled build side re-optimization. Return original build side from vanilla Spark.
      return shj.buildSide
    }
    shj.logicalLink
      .flatMap {
        case join: Join => Some(getOptimalBuildSide(join))
        case _ => None
      }
      .getOrElse {
        // Some shj operators generated in certain Spark tests such as OuterJoinSuite,
        // could possibly have no logical link set.
        shj.buildSide
      }
  }

  def getOptimalBuildSide(join: Join): BuildSide = {
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val leftRowCount = join.left.stats.rowCount
    val rightRowCount = join.right.stats.rowCount
    if (leftSize == rightSize && rightRowCount.isDefined && leftRowCount.isDefined) {
      if (rightRowCount.get <= leftRowCount.get) {
        return BuildRight
      }
      return BuildLeft
    }
    if (rightSize <= leftSize) {
      return BuildRight
    }
    BuildLeft
  }
}

// Other transformations.
case class OffloadOthers() extends OffloadSingleNode with LogLevelUtil {
  import OffloadOthers._
  private val replace = new ReplaceSingleNode()

  override def offload(plan: SparkPlan): SparkPlan = replace.doReplace(plan)
}

object OffloadOthers {
  // Utility to replace single node within transformed Gluten node.
  // Children will be preserved as they are as children of the output node.
  //
  // Do not look-up on children on the input node in this rule. Otherwise
  // it may break RAS which would group all the possible input nodes to
  // search for validate candidates.
  class ReplaceSingleNode() extends LogLevelUtil with Logging {

    def doReplace(p: SparkPlan): SparkPlan = {
      val plan = p
      if (FallbackTags.nonEmpty(plan)) {
        return plan
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
          ColumnarCoalesceExec(plan.numPartitions, plan.child)
        case plan: FilterExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance
            .genFilterExecTransformer(plan.condition, plan.child)
        case plan: ProjectExec =>
          val columnarChild = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ProjectExecTransformer(plan.projectList, columnarChild)
        case plan: SortAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: ObjectHashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: UnionExec =>
          val children = plan.children
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarUnionExec(children)
        case plan: ExpandExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ExpandExecTransformer(plan.projections, plan.output, child)
        case plan: WriteFilesExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val writeTransformer = WriteFilesExecTransformer(
            child,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)

          ColumnarWriteFilesExec(
            writeTransformer,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
        case plan: SortExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
        case plan: TakeOrderedAndProjectExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          TakeOrderedAndProjectExecTransformer(
            limit,
            plan.sortOrder,
            plan.projectList,
            child,
            offset)
        case plan: WindowExec =>
          WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
        case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
          val windowGroupLimitPlan = SparkShimLoader.getSparkShims
            .getWindowGroupLimitExecShim(plan)
            .asInstanceOf[WindowGroupLimitExecShim]
          BackendsApiManager.getSparkPlanExecApiInstance.genWindowGroupLimitTransformer(
            windowGroupLimitPlan.partitionSpec,
            windowGroupLimitPlan.orderSpec,
            windowGroupLimitPlan.rankLikeFunction,
            windowGroupLimitPlan.limit,
            windowGroupLimitPlan.mode,
            windowGroupLimitPlan.child
          )
        case plan: GlobalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          LimitTransformer(child, offset, limit)
        case plan: LocalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          LimitTransformer(child, 0L, plan.limit)
        case plan: GenerateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            plan.generator,
            plan.requiredChildOutput,
            plan.outer,
            plan.generatorOutput,
            child)
        case plan: BatchEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
        case plan: ArrowEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          // For ArrowEvalPythonExec, CH supports it through EvalPythonExecTransformer while
          // Velox backend uses ColumnarArrowEvalPythonExec.
          if (!BackendsApiManager.getSettings.supportColumnarArrowUdf()) {
            EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.createColumnarArrowEvalPythonExec(
              plan.udfs,
              plan.resultAttrs,
              child,
              plan.evalType)
          }
        case plan: SampleExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            plan.lowerBound,
            plan.upperBound,
            plan.withReplacement,
            plan.seed,
            child)
        case p if !p.isInstanceOf[GlutenPlan] =>
          logDebug(s"Transformation for ${p.getClass} is currently not supported.")
          val children = plan.children
          p.withNewChildren(children)
        case other => other
      }
    }

    /**
     * Apply scan transformer for file source and batch source,
     *   1. create new filter and scan transformer, 2. validate, tag new scan as unsupported if
     *      failed, 3. return new source.
     */
    private def applyScanTransformer(plan: SparkPlan): SparkPlan = plan match {
      case plan: FileSourceScanExec =>
        val transformer = ScanTransformerFactory.createFileSourceScanTransformer(plan)
        val validationResult = transformer.doValidate()
        if (validationResult.ok()) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          transformer
        } else {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
          FallbackTags.add(plan, validationResult.reason())
          plan
        }
      case plan: BatchScanExec =>
        ScanTransformerFactory.createBatchScanTransformer(plan)
      case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
        // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
        val hiveTableScanExecTransformer =
          BackendsApiManager.getSparkPlanExecApiInstance.genHiveTableScanExecTransformer(plan)
        val validateResult = hiveTableScanExecTransformer.doValidate()
        if (validateResult.ok()) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          return hiveTableScanExecTransformer
        }
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        FallbackTags.add(plan, validateResult.reason())
        plan
      case other =>
        throw new GlutenNotSupportException(s"${other.getClass.toString} is not supported.")
    }
  }
}
