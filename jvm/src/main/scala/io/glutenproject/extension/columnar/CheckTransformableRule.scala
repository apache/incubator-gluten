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

import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec

object Transformable {
  val TAG: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("io.glutenproject.istransformable")

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def tag(plan: SparkPlan, isTransformable: Boolean): Unit = {
    if (isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transformable tag already set in plan: " + plan.toString())
    }
    plan.setTagValue(TAG, isTransformable)
  }

  def tagTransformable(plan: SparkPlan): Unit = {
    tag(plan, true)
  }

  def tagNotTransformable(plan: SparkPlan): Unit = {
    tag(plan, false)
  }

  def isTransformable(plan: SparkPlan): Boolean = {
    if (!isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transformable tag not set in plan: " + plan.toString())
    }
    plan.getTagValue(TAG).getOrElse(throw new IllegalStateException())
  }
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class CheckTransformableRule() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val optimizeLevel: Integer = columnarConf.joinOptimizationThrottle
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableColumnarBatchScan: Boolean = columnarConf.enableColumnarBatchScan
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableColumnarExpand: Boolean = columnarConf.enableColumnarExpand
  val enableColumnarShuffledHashJoin: Boolean = columnarConf.enableColumnarShuffledHashJoin
  val enableColumnarBroadcastExchange: Boolean =
    columnarConf.enableColumnarBroadcastJoin && columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean =
    columnarConf.enableColumnarBroadcastJoin && columnarConf.enableColumnarBroadcastExchange
  val enableColumnarArrowUDF: Boolean = columnarConf.enableColumnarArrowUDF

  def apply(plan: SparkPlan): SparkPlan = {
    addTransformableTags(plan)
  }

  /**
   * Inserts a transformable tag on top of those that are not supported.
   */
  private def addTransformableTags(plan: SparkPlan): SparkPlan = {
    addTransformableTag(plan)
    plan.withNewChildren(plan.children.map(addTransformableTags))
  }

  private def addTransformableTag(plan: SparkPlan): Unit = {
    if (Transformable.isAlreadyTagged(plan)) {
      logDebug(
        s"Skipping executing" +
          s"io.glutenproject.extension.columnar.CheckTransformableRule.addTransformableTag " +
          s"since plan already tagged as " +
          s"${Transformable.isTransformable(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        /* case plan: ArrowEvalPythonExec =>
          if (!enableColumnarArrowUDF) return false
          val transformer = ArrowEvalPythonExecTransformer(
            plan.udfs, plan.resultAttrs, plan.child, plan.evalType)
          if (!transformer.doValidate()) return false
          transformer */
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = new BatchScanExecTransformer(plan.output, plan.scan,
              plan.runtimeFilters)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = new FileSourceScanExecTransformer(plan.relation,
              plan.output,
              plan.requiredSchema,
              plan.partitionFilters,
              plan.optionalBucketSet,
              plan.optionalNumCoalescedBuckets,
              plan.dataFilters,
              plan.tableIdentifier,
              plan.disableBucketedScan)
            Transformable.tag(plan, transformer.doValidate())
          }
        case _: InMemoryTableScanExec =>
          false
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = ProjectExecTransformer(plan.projectList, plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: FilterExec =>
          if (!enableColumnarFilter) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(plan.condition, plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = UnionExecTransformer(plan.children)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: ExpandExec =>
          if (!enableColumnarExpand) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: SortExec =>
          if (!enableColumnarSort) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = SortExecTransformer(
              plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = new ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genShuffledHashJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.buildSide,
                plan.condition,
                plan.left,
                plan.right)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: BroadcastExchangeExec =>
          // columnar broadcast is enabled only when columnar bhj is enabled.
          if (!enableColumnarBroadcastExchange) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: BroadcastHashJoinExec =>
          if (!enableColumnarBroadcastJoin) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genBroadcastHashJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.buildSide,
                plan.condition,
                plan.left,
                plan.right,
                isNullAwareAntiJoin = plan.isNullAwareAntiJoin)
            val isTransformable = transformer.doValidate()
            Transformable.tag(plan, isTransformable)
            if (!isTransformable) {
              plan.children.foreach {
                case exchange: BroadcastExchangeExec =>
                  Transformable.tagNotTransformable(exchange)
                case _ =>
              }
            }
          }
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = SortMergeJoinExecTransformer(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: WindowExec =>
          if (!enableColumnarWindow) {
            Transformable.tagNotTransformable(plan)
          } else {
            val transformer = WindowExecTransformer(
              plan.windowExpression,
              plan.partitionSpec,
              plan.orderSpec,
              plan.child)
            Transformable.tag(plan, transformer.doValidate())
          }
        case plan: CoalesceExec =>
          val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
          Transformable.tag(plan, transformer.doValidate())
        case _: AQEShuffleReadExec =>
          Transformable.tagNotTransformable(plan)
        case _ =>
          // currently we assume a plan to be transformable by default
          Transformable.tagTransformable(plan)
      }
    } catch {
      case e: UnsupportedOperationException =>
        logError(
          s"Fall back to use row-based operators, error is ${e.getMessage}," +
            s"original sparkplan is ${plan.getClass}(${plan.children.toList.map(_.getClass)})")
        false
    }
  }
}
