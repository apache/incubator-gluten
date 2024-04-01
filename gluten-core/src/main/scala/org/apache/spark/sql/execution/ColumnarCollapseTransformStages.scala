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
package org.apache.spark.sql.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution._
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

/**
 * A bridge to connect [[SparkPlan]] and [[TransformSupport]] and provide the substrait plan
 * `ReadRel` for the child columnar iterator, so that the [[TransformSupport]] always has input. It
 * would be transformed to `ValueStreamNode` at native side.
 */
case class InputIteratorTransformer(child: SparkPlan) extends UnaryTransformSupport {
  assert(child.isInstanceOf[ColumnarInputAdapter])

  @transient
  override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genInputIteratorTransformerMetrics(sparkContext)

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genInputIteratorTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val operatorId = context.nextOperatorId(nodeName)
    val readRel = RelBuilder.makeReadRelForInputIterator(child.output.asJava, context, operatorId)
    TransformContext(output, output, readRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * Implemented by referring to spark's CollapseCodegenStages.
 *
 * Find the chained plans that support transform, collapse them together as WholeStageTransformer.
 *
 * The `transformStageCounter` generates ID for transform stages within a query plan. It does not
 * affect equality, nor does it participate in destructuring pattern matching of
 * WholeStageTransformer.
 *
 * This ID is used to help differentiate between transform stages. It is included as a part of the
 * explain output for physical plans, e.g.
 *
 * ==Physical Plan==
 * *(5) SortMergeJoin [x#3L], [y#9L], Inner :- *(2) Sort [x#3L ASC NULLS FIRST], false, 0 : +-
 * Exchange hashpartitioning(x#3L, 200) : +- *(1) Project [(id#0L % 2) AS x#3L] : +- *(1) Filter
 * isnotnull((id#0L % 2)) : +- *(1) Range (0, 5, step=1, splits=8) +- *(4) Sort [y#9L ASC NULLS
 * FIRST], false, 0 +- Exchange hashpartitioning(y#9L, 200) +- *(3) Project [(id#6L % 2) AS y#9L] +-
 * *(3) Filter isnotnull((id#6L % 2)) +- *(3) Range (0, 5, step=1, splits=8)
 *
 * where the ID makes it obvious that not all adjacent transformed plan operators are of the same
 * transform stage.
 *
 * The transform stage ID is also optionally included in the name of the generated classes as a
 * suffix, so that it's easier to associate a generated class back to the physical operator. This is
 * controlled by SQLConf: spark.sql.codegen.useIdInClassName
 *
 * The ID is also included in various log messages.
 *
 * Within a query, a transform stage in a plan starts counting from 1, in "insertion order".
 * WholeStageTransformer operators are inserted into a plan in depth-first post-order. See
 * CollapseTransformStages.insertWholeStageTransform for the definition of insertion order.
 *
 * 0 is reserved as a special ID value to indicate a temporary WholeStageTransformer object is
 * created, e.g. for special fallback handling when an existing WholeStageTransformer failed to
 * generate/compile code.
 */
case class ColumnarCollapseTransformStages(
    glutenConfig: GlutenConfig,
    transformStageCounter: AtomicInteger = ColumnarCollapseTransformStages.transformStageCounter)
  extends Rule[SparkPlan] {

  def separateScanRDD: Boolean =
    BackendsApiManager.getSettings.excludeScanExecFromCollapsedStage()

  def apply(plan: SparkPlan): SparkPlan = {
    insertWholeStageTransformer(plan)
  }

  /**
   * When it's the ClickHouse backend, BasicScanExecTransformer will not be included in
   * WholeStageTransformer.
   */
  private def isSeparateBaseScanExecTransformer(plan: SparkPlan): Boolean = plan match {
    case _: BasicScanExecTransformer if separateScanRDD => true
    case _ => false
  }

  private def supportTransform(plan: SparkPlan): Boolean = plan match {
    case plan: TransformSupport if !isSeparateBaseScanExecTransformer(plan) => true
    case _ => false
  }

  /** Inserts an InputIteratorTransformer on top of those that do not support transform. */
  private def insertInputIteratorTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      case p if !supportTransform(p) =>
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(insertWholeStageTransformer(p))
      case p =>
        p.withNewChildren(p.children.map(insertInputIteratorTransformer))
    }
  }

  private def insertWholeStageTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      case t if supportTransform(t) =>
        WholeStageTransformer(t.withNewChildren(t.children.map(insertInputIteratorTransformer)))(
          transformStageCounter.incrementAndGet())
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageTransformer))
    }
  }
}

case class ColumnarInputAdapter(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def supportsColumnar: Boolean = child.supportsColumnar
  override protected def doExecute(): RDD[InternalRow] =
    child.execute()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = child.executeColumnar()
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def vectorTypes: Option[Seq[String]] = child.vectorTypes
  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = child.executeBroadcast()
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
  // Node name's required to be "InputAdapter" to correctly draw UI graph.
  // See buildSparkPlanGraphNode in SparkPlanGraph.scala of Spark.
  override def nodeName: String = s"InputAdapter"
}

object ColumnarCollapseTransformStages {
  val transformStageCounter = new AtomicInteger(0)

  def wrapInputIteratorTransformer(plan: SparkPlan): TransformSupport = {
    InputIteratorTransformer(ColumnarInputAdapter(plan))
  }
}
