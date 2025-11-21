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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, _}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

abstract class FilterExecTransformerBase(val cond: Expression, val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  val enablePushDownFilter = true

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

  def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, originalInputAttributes)
      .doTransform(context)
    RelBuilder.makeFilterRel(
      context,
      condExprNode,
      originalInputAttributes.asJava,
      operatorId,
      input,
      validation
    )
  }

  protected def flattenedCond: Seq[Expression] = splitConjunctivePredicates(cond)

  override def output: Seq[Attribute] =
    FilterExecTransformerBase.buildNewOutput(child.output, flattenedCond)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = child.output

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode =
      getRelNode(substraitContext, cond, child.output, operatorId, null, validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, cond, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(output, currRel)
  }
}

object FilterExecTransformerBase extends PredicateHelper {

  def buildNewOutput(output: Seq[Attribute], conds: Seq[Expression]): Seq[Attribute] = {
    // Split out all the IsNotNulls from condition.
    val (notNullPreds, _) = conds.partition {
      case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(AttributeSet(output))
      case _ => false
    }

    // The columns that will filter out by `IsNotNull` could be considered as not nullable.
    val notNullAttributes: Seq[ExprId] = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

    outputWithNullability(output, notNullAttributes)
  }
}

abstract class ProjectExecTransformerBase(val list: Seq[NamedExpression], val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    failValidationWithException {
      val relNode =
        getRelNode(substraitContext, list, child.output, operatorId, null, validation = true)
      // Then, validate the generated plan in native engine.
      doNativeValidation(substraitContext, relNode)
    }()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, list, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Project Rel should be valid")
    TransformContext(output, currRel)
  }

  override def output: Seq[Attribute] = list.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = list

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(projectList, originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(context)).asJava
    RelBuilder.makeProjectRel(
      originalInputAttributes.asJava,
      input,
      projExprNodeList,
      context,
      operatorId,
      validation)
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", list)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }
}

// An alternative for UnionExec.
case class ColumnarUnionExec(children: Seq[SparkPlan]) extends ValidatablePlan {
  children.foreach {
    case w: WholeStageTransformer =>
      // FIXME: Avoid such practice for plan immutability.
      w.setOutputSchemaForPlan(output)
    case _ =>
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map {
      attrs =>
        val firstAttr = attrs.head
        val nullable = attrs.exists(_.nullable)
        val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.unionLikeMerge)
        if (firstAttr.dataType == newDt) {
          firstAttr.withNullability(nullable)
        } else {
          AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
            firstAttr.exprId,
            firstAttr.qualifier)
        }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): ColumnarUnionExec =
    copy(children = newChildren)

  def columnarInputRDD: RDD[ColumnarBatch] = {
    if (children.isEmpty) {
      throw new IllegalArgumentException(s"Empty children")
    }
    sparkContext.union(children.map(c => c.executeColumnar()))
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = columnarInputRDD
}

/**
 * Contains functions for the comparison and separation of the filter conditions in Scan and Filter.
 * Contains the function to manually push down the conditions into Scan.
 */
object FilterHandler extends PredicateHelper {

  /**
   * Compare the semantics of the filter conditions pushed down to Scan and in the Filter.
   *
   * @param scanFilters
   *   : the conditions pushed down into Scan
   * @param filters
   *   : the conditions in the Filter after the Scan
   * @return
   *   the filter conditions not pushed down into Scan.
   */
  def getRemainingFilters(scanFilters: Seq[Expression], filters: Seq[Expression]): Seq[Expression] =
    (filters.toSet -- scanFilters.toSet).toSeq
}
