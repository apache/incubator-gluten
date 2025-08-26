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
package org.apache.gluten.extension.columnar.offload
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.RDDScanTransformer
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec, EvalPythonExecTransformer}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

// Exchange transformation.
case class OffloadExchange() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case s: ShuffleExchangeExec =>
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
    if (!GlutenConfig.get.shuffledHashJoinOptimizeBuildSide) {
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
  private val replace = new ReplaceSingleNode

  override def offload(plan: SparkPlan): SparkPlan = replace.doReplace(plan)
}

object OffloadOthers {
  // Utility to replace single node within transformed Gluten node.
  // Children will be preserved as they are as children of the output node.
  //
  // Do not look up on children on the input node in this rule. Otherwise,
  // it may break RAS which would group all the possible input nodes to
  // search for validate candidates.
  private class ReplaceSingleNode extends LogLevelUtil with Logging {

    def doReplace(p: SparkPlan): SparkPlan = {
      if (FallbackTags.nonEmpty(p)) {
        return p
      }
      p match {
        case plan: BatchScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createBatchScanTransformer(plan)
        case plan: FileSourceScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createFileSourceScanTransformer(plan)
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HiveTableScanExecTransformer(plan)
        case CoalesceExec(numPartitions, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          ColumnarCoalesceExec(numPartitions, child)
        case FilterExec(condition, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance
            .genFilterExecTransformer(condition, child)
        case ProjectExec(projectList, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          ProjectExecTransformer(projectList, child)
        case plan: HashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: SortAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: ObjectHashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case UnionExec(children) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          ColumnarUnionExec(children)
        case ExpandExec(projections, output, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          ExpandExecTransformer(projections, output, child)
        case WriteFilesExec(
              child,
              fileFormat,
              partitionColumns,
              bucketSpec,
              options,
              staticPartitions) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          val writeTransformer = WriteFilesExecTransformer(
            child,
            fileFormat,
            partitionColumns,
            bucketSpec,
            options,
            staticPartitions)
          ColumnarWriteFilesExec(
            writeTransformer,
            fileFormat,
            partitionColumns,
            bucketSpec,
            options,
            staticPartitions)
        case SortExec(sortOrder, global, child, testSpillFrequency) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          SortExecTransformer(sortOrder, global, child, testSpillFrequency)
        case plan @ TakeOrderedAndProjectExec(_, sortOrder, projectList, child) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          TakeOrderedAndProjectExecTransformer(limit, sortOrder, projectList, child, offset)
        case WindowExec(windowExpression, partitionSpec, orderSpec, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          WindowExecTransformer(windowExpression, partitionSpec, orderSpec, child)
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
        case plan @ GlobalLimitExec(_, child) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          LimitExecTransformer(child, offset, limit)
        case LocalLimitExec(limit, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          LimitExecTransformer(child, 0L, limit)
        case GenerateExec(generator, requiredChildOutput, outer, generatorOutput, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            generator,
            requiredChildOutput,
            outer,
            generatorOutput,
            child)
        case BatchEvalPythonExec(udfs, resultAttrs, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          EvalPythonExecTransformer(udfs, resultAttrs, child)
        case ArrowEvalPythonExec(udfs, resultAttrs, child, evalType) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          // For ArrowEvalPythonExec, CH supports it through EvalPythonExecTransformer while
          // Velox backend uses ColumnarArrowEvalPythonExec.
          if (
            !BackendsApiManager.getSettings.supportColumnarArrowUdf() ||
            !GlutenConfig.get.enableColumnarArrowUDF
          ) {
            EvalPythonExecTransformer(udfs, resultAttrs, child)
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.createColumnarArrowEvalPythonExec(
              udfs,
              resultAttrs,
              child,
              evalType)
          }
        case plan: RangeExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance.genColumnarRangeExec(
            plan.start,
            plan.end,
            plan.step,
            plan.numSlices,
            plan.numElements,
            plan.output,
            plan.children
          )
        case SampleExec(lowerBound, upperBound, withReplacement, seed, child) =>
          logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            lowerBound,
            upperBound,
            withReplacement,
            seed,
            child)
        case plan: RDDScanExec if RDDScanTransformer.isSupportRDDScanExec(plan) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          RDDScanTransformer.getRDDScanTransform(plan)
        case p if !p.isInstanceOf[GlutenPlan] =>
          logDebug(s"Transformation for ${p.getClass} is currently not supported.")
          p
        case other => other
      }
    }
  }
}
