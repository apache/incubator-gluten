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

import scala.util.control.Breaks.{break, breakable}

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution._
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}


trait TransformHint {
  val stacktrace: Option[String] =
    if (TransformHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class TRANSFORM_SUPPORTED() extends TransformHint
case class TRANSFORM_UNSUPPORTED() extends TransformHint

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
    if (isAlreadyTagged(plan)) {
      if (isNotTransformable(plan) && hint.isInstanceOf[TRANSFORM_SUPPORTED]) {
        throw new UnsupportedOperationException(
          s"Plan was already tagged as non-transformable, " +
            s"cannot mark it as transformable after that: ${plan.toString()}")
      }
      untag(plan)
    }
    plan.setTagValue(TAG, hint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_SUPPORTED())
  }

  def tagNotTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_UNSUPPORTED())
  }

  def getHint(plan: SparkPlan): TransformHint = {
    if (!isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString())
    }
    plan.getTagValue(TAG).getOrElse(throw new IllegalStateException())
  }
}

// Holds rules which have higher privilege to tag (not) transformable before AddTransformHintRule.
object TagBeforeTransformHits {
  val ruleBuilders: List[SparkSession => Rule[SparkPlan]] = {
    List((_: SparkSession) => FallbackOneRowRelation(),
      (_: SparkSession) => FallbackMultiCodegens())
  }
}

case class StoreExpandGroupExpression() extends  Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case agg @ HashAggregateExec(_, _, _, _, _, _, _, _, child: ExpandExec) =>
      agg.copy(child = CustomExpandExec(
        child.projections, agg.groupingExpressions,
        child.output, child.child))
  }
}

case class FallbackMultiCodegens() extends Rule[SparkPlan] {
  lazy val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
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
    TransformHints.tagNotTransformable(plan)
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

  def insertRowGuardRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: BroadcastExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: ShuffledHashJoinExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
      case p if !supportCodegen(p) =>
        // insert row guard them recursively
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p: BroadcastQueryStageExec =>
        p
      case p => tagNotTransformable(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
    }
  }

  def insertRowGuardOrNot(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      case plan if existsMultiCodegens(plan) =>
        insertRowGuardRecursive(plan)
      case p: BroadcastQueryStageExec =>
        p
      case other =>
        other.withNewChildren(other.children.map(insertRowGuardOrNot))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (physicalJoinOptimize) {
      insertRowGuardOrNot(plan)
    } else plan
  }
}

// This rule will fall back the whole plan if it contains OneRowRelation scan.
// This should only affect some light-weight cases in some basic UTs.
case class FallbackOneRowRelation() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val hasOneRowRelation =
      plan.find(_.isInstanceOf[RDDScanExec]) match {
        case Some(scan: RDDScanExec) => scan.name.equals("OneRowRelation")
        case _ => false
      }
    if (hasOneRowRelation) {
      plan.foreach(TransformHints.tagNotTransformable)
    }
    plan
  }
}

case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p =>
      if (BackendsApiManager.getSettings.fallbackOnEmptySchema()) {
        if (p.output.isEmpty) {
          // Some backends are not eligible to offload zero-column plan so far
          TransformHints.tagNotTransformable(p)
        }
        if (p.children.exists(_.output.isEmpty)) {
          // Some backends are also not eligible to offload plan within zero-column input so far
          TransformHints.tagNotTransformable(p)
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
  val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val optimizeLevel: Integer = columnarConf.physicalJoinOptimizationThrottle
  val enableColumnarShuffle: Boolean = BackendsApiManager.getSettings.supportColumnarShuffleExec()
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
  val enableColumnarLimit: Boolean = columnarConf.enableColumnarLimit
  val enableColumnarGenerate: Boolean = columnarConf.enableColumnarGenerate
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
    if (TransformHints.isAlreadyTagged(plan)) {
      logDebug(
        s"Skipping executing" +
          s"io.glutenproject.extension.columnar.CheckTransformableRule.addTransformableTag " +
          s"since plan already tagged as " +
          s"${TransformHints.getHint(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) {
            TransformHints.tagNotTransformable(plan)
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.runtimeFilters.nonEmpty) {
              TransformHints.tagTransformable(plan)
            } else {
              val transformer = new BatchScanExecTransformer(plan.output, plan.scan,
                plan.runtimeFilters)
              TransformHints.tag(plan, transformer.doValidate().toTransformHint)
            }
          }
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) {
            TransformHints.tagNotTransformable(plan)
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.partitionFilters.nonEmpty) {
              TransformHints.tagTransformable(plan)
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
              TransformHints.tag(plan, transformer.doValidate().toTransformHint)
            }
          }
        case plan: InMemoryTableScanExec =>
          // ColumnarInMemoryTableScanExec.scala appears to be out-of-date
          //   and need some tests before being enabled.
          TransformHints.tagNotTransformable(plan)
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = ProjectExecTransformer(plan.projectList, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: FilterExec =>
          if (!enableColumnarFilter) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(plan.condition, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(plan)
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
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ObjectHashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(plan)
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
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = UnionExecTransformer(plan.children)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: CustomExpandExec =>
          if (!enableColumnarExpand) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = ExpandExecTransformer(plan.projections,
              plan.groupExpression, plan.output, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: SortExec =>
          if (!enableColumnarSort) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = SortExecTransformer(
              plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = new ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) {
            TransformHints.tagNotTransformable(plan)
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
            TransformHints.tagNotTransformable(plan)
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
            TransformHints.tagNotTransformable(bhj)
          } else {
            val isBhjTransformable: Boolean = {
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

            val maybeExchange = buildSidePlan.find {
              case BroadcastExchangeExec(_, _) => true
              case _ => false
            }

            maybeExchange match {
              case Some(exchange@BroadcastExchangeExec(mode, child)) =>
                TransformHints.tag(bhj, isBhjTransformable.toTransformHint)
                if (!isBhjTransformable) {
                  TransformHints.tagNotTransformable(exchange)
                }
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
                    TransformHints.tagNotTransformable(bhj)
                  case ColumnarBroadcastExchangeExec(mode, child) =>
                    if (!isBhjTransformable) {
                      throw new IllegalStateException(s"BroadcastExchange has already been" +
                        s" transformed to columnar version but BHJ is determined as" +
                        s" non-transformable: ${bhj.toString()}")
                    }
                    TransformHints.tagTransformable(bhj)
                }
            }
          }
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) {
            TransformHints.tagNotTransformable(plan)
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
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = WindowExecTransformer(
              plan.windowExpression,
              plan.partitionSpec,
              plan.orderSpec,
              plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: CoalesceExec =>
          val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
          TransformHints.tag(plan, transformer.doValidate().toTransformHint)
        case plan: GlobalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: LocalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }
        case plan: GenerateExec =>
          if (!enableColumnarGenerate) {
            TransformHints.tagNotTransformable(plan)
          } else {
            val transformer = GenerateExecTransformer(plan.generator, plan.requiredChildOutput,
              plan.outer, plan.generatorOutput, plan.child)
            TransformHints.tag(plan, transformer.doValidate().toTransformHint)
          }

        case _: AQEShuffleReadExec =>
          TransformHints.tagTransformable(plan)
        case plan: TakeOrderedAndProjectExec =>
          if (!enableColumnarSort || !enableColumnarLimit || !enableColumnarShuffle ||
            !enableColumnarProject) {
            TransformHints.tagNotTransformable(plan)
          } else {
            var tagged = false
            val limitPlan = LimitTransformer(plan.child, 0, plan.limit)
            tagged = limitPlan.doValidate()

            if (tagged) {
              val sortPlan = SortExecTransformer(plan.sortOrder, false, plan.child)
              tagged = sortPlan.doValidate()
            }

            if (tagged) {
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
        logWarning(
          s"Fall back to use row-based operators, error is ${e.getMessage}," +
            s"original sparkplan is ${plan.getClass}(${plan.children.toList.map(_.getClass)})")
        TransformHints.tagNotTransformable(plan)
    }
  }

  implicit class EncodeTransformableTagImplicits(transformable: Boolean) {
    def toTransformHint: TransformHint = {
      if (transformable) {
        TRANSFORM_SUPPORTED()
      } else {
        TRANSFORM_UNSUPPORTED()
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
