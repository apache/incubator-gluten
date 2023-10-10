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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionNode, WindowFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.Any
import io.substrait.proto.SortField

import java.util

case class WindowExecTransformer(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase
  with TransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetricsUpdater(metrics)

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning(
        "No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (BackendsApiManager.isVeloxBackend) {
      // We still need to do sort for columnar window, see `FLAGS_SkipRowSortInWindowOp`
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    } else {
      Seq(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    throw new UnsupportedOperationException(s"This operator doesn't support getBuildPlans.")
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  def getRelNode(
      context: SubstraitContext,
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      sortOrder: Seq[SortOrder],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // WindowFunction Expressions
    val windowExpressions = new util.ArrayList[WindowFunctionNode]()
    BackendsApiManager.getSparkPlanExecApiInstance.genWindowFunctionsNode(
      windowExpression,
      windowExpressions,
      originalInputAttributes,
      args
    )

    // Partition By Expressions
    val partitionsExpressions = new util.ArrayList[ExpressionNode]()
    partitionSpec.map {
      partitionExpr =>
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(partitionExpr, attributeSeq = child.output)
          .doTransform(args)
        partitionsExpressions.add(exprNode)
    }

    // Sort By Expressions
    val sortFieldList = new util.ArrayList[SortField]()
    sortOrder.map(
      order => {
        val builder = SortField.newBuilder()
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
          .doTransform(args)
        builder.setExpr(exprNode.toProtobuf)

        (order.direction.sql, order.nullOrdering.sql) match {
          case ("ASC", "NULLS FIRST") =>
            builder.setDirectionValue(1);
          case ("ASC", "NULLS LAST") =>
            builder.setDirectionValue(2);
          case ("DESC", "NULLS FIRST") =>
            builder.setDirectionValue(3);
          case ("DESC", "NULLS LAST") =>
            builder.setDirectionValue(4);
          case _ =>
            builder.setDirectionValue(0);
        }
        sortFieldList.add(builder.build())
      })
    if (!validation) {
      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportWindowExec(windowExpression)) {
      return ValidationResult
        .notOk(s"Found unsupported window expression: ${windowExpression.mkString(", ")}")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = getRelNode(
      substraitContext,
      windowExpression,
      partitionSpec,
      orderSpec,
      child.output,
      operatorId,
      null,
      validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    if (windowExpression == null || windowExpression.isEmpty) {
      // The computing for this project is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (
        getRelNode(
          context,
          windowExpression,
          partitionSpec,
          orderSpec,
          child.output,
          operatorId,
          childCtx.root,
          validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (
        getRelNode(
          context,
          windowExpression,
          partitionSpec,
          orderSpec,
          child.output,
          operatorId,
          readRel,
          validation = false),
        child.output)
    }
    assert(currRel != null, "Window Rel should be valid")
    TransformContext(inputAttributes, output, currRel)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowExecTransformer =
    copy(child = newChild)
}

object WindowExecTransformer {

  /** Gets lower/upper bound represented in string. */
  def getFrameBound(bound: Expression): String = {
    // The lower/upper can be either a foldable Expression or a SpecialFrameBoundary.
    if (bound.foldable) {
      bound.eval().toString
    } else {
      bound.sql
    }
  }
}
