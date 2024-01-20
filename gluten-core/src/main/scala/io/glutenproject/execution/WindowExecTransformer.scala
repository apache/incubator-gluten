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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExecBase

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.SortField

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class WindowExecTransformer(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetricsUpdater(metrics)

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
    if (
      BackendsApiManager.getSettings.requiredChildOrderingForWindow()
      && GlutenConfig.getConf.veloxColumnarWindowType.equals("streaming")
    ) {
      // Velox StreamingWindow need to require child order.
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    } else {
      Seq(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  def genWindowParameters(): Any = {
    // Start with "WindowParameters:"
    val windowParametersStr = new StringBuffer("WindowParameters:")
    // isStreaming: 1 for streaming, 0 for sort
    val isStreaming: Int =
      if (GlutenConfig.getConf.veloxColumnarWindowType.equals("streaming")) 1 else 0

    windowParametersStr
      .append("isStreaming=")
      .append(isStreaming)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(windowParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  private def needsProject: Boolean = (partitionSpec ++ orderSpec.map(_.child)).exists {
    case _: Attribute => false
    case _ => true
  }

  def getWindowRelWithoutProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // WindowFunction Expressions
    val windowExpressions = new JArrayList[WindowFunctionNode]()
    BackendsApiManager.getSparkPlanExecApiInstance.genWindowFunctionsNode(
      windowExpression,
      windowExpressions,
      originalInputAttributes,
      args
    )

    // Partition By Expressions
    val partitionsExpressions = partitionSpec
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, attributeSeq = child.output)
          .doTransform(args))
      .asJava

    // Sort By Expressions
    val sortFieldList =
      orderSpec.map {
        order =>
          val builder = SortField.newBuilder()
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
            .doTransform(args)
          builder.setExpr(exprNode.toProtobuf)
          builder.setDirectionValue(SortExecTransformer.transformSortDirection(order))
          builder.build()
      }.asJava
    if (!validation) {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(genWindowParameters(), null)
      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

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

  private def getWindowRelWithProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction

    val preExpressions = new ArrayBuffer[Expression]()
    val selections = new ArrayBuffer[Int]()

    // Window requires all output attributes from child.
    preExpressions ++= originalInputAttributes
    selections ++= originalInputAttributes.indices

    def appendIfNotFound(expression: Expression): Unit = {
      val foundExpr = preExpressions.find(e => e.semanticEquals(expression))
      if (foundExpr.isDefined) {
        // If found, no need to add it to preExpressions again.
        // The selecting index will be found.
        selections += preExpressions.indexOf(foundExpr.get)
      } else {
        // If not found, add this expression into preExpressions.
        // A new selecting index will be created.
        preExpressions += expression.clone()
        selections += (preExpressions.size - 1)
      }
    }

    partitionSpec.foreach(appendIfNotFound)
    orderSpec.foreach(order => appendIfNotFound(order.child))

    // Create the expression nodes needed by Project node.
    val preExprNodes = preExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, originalInputAttributes)
          .doTransform(args))
      .asJava
    val emitStartIndex = originalInputAttributes.size
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, preExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        preExprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
    createWindowRelAfterProjection(
      context,
      originalInputAttributes,
      preExprNodes,
      selections,
      inputRel,
      operatorId,
      validation)
  }

  private def createWindowRelAfterProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      preExprNodes: JList[ExpressionNode],
      selections: Seq[Int],
      inputRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction

    // WindowFunction Expressions
    val windowExpressions = new JArrayList[WindowFunctionNode]()
    BackendsApiManager.getSparkPlanExecApiInstance.genWindowFunctionsNode(
      windowExpression,
      windowExpressions,
      originalInputAttributes,
      args
    )

    var colIdx = originalInputAttributes.size
    val partitionList = new JArrayList[ExpressionNode]()
    val partitionAttributes = new ArrayBuffer[AttributeReference]()
    partitionSpec.foreach {
      partition =>
        val partitionExpr = ExpressionBuilder.makeSelection(selections(colIdx))
        partitionList.add(partitionExpr)
        partitionAttributes += AttributeReference(s"col_$colIdx", partition.dataType)()
        colIdx += 1
    }

    val sortAttributes = new ArrayBuffer[AttributeReference]()
    val sortFieldList = orderSpec.map {
      order =>
        sortAttributes += AttributeReference(s"col_$colIdx", order.child.dataType)()
        val builder = SortField.newBuilder()
        val exprNode = ExpressionBuilder.makeSelection(selections(colIdx))
        colIdx += 1
        builder.setExpr(exprNode.toProtobuf)
        builder.setDirectionValue(SortExecTransformer.transformSortDirection(order))
        builder.build()
    }.asJava

    val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
    originalInputAttributes.foreach(
      attr => inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable)))
    partitionAttributes.foreach(
      attr => inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable)))
    sortAttributes.foreach(
      attr => inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable)))

    val windowRel = if (!validation) {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(genWindowParameters(), null)
      RelBuilder.makeWindowRel(
        inputRel,
        windowExpressions,
        partitionList,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeWindowRel(
        inputRel,
        windowExpressions,
        partitionList,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    }
    createPostProjection(
      context,
      originalInputAttributes,
      preExprNodes,
      inputTypeNodeList,
      windowRel,
      operatorId,
      validation)
  }

  private def createPostProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      preExprNodes: JList[ExpressionNode],
      inputTypeNodeList: JList[TypeNode],
      inputRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val postExprNodes = new JArrayList[ExpressionNode]()
    postExprNodes.addAll(preExprNodes.subList(0, originalInputAttributes.size))
    var windowStartIdx = preExprNodes.size()
    windowExpression.foreach {
      _ =>
        postExprNodes.add(ExpressionBuilder.makeSelection(windowStartIdx))
        windowStartIdx += 1
    }
    if (!validation) {
      RelBuilder.makeProjectRel(
        inputRel,
        postExprNodes,
        context,
        operatorId,
        preExprNodes.size() + windowExpression.size)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        inputRel,
        postExprNodes,
        extensionNode,
        context,
        operatorId,
        preExprNodes.size() + windowExpression.size)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportWindowExec(windowExpression)) {
      return ValidationResult
        .notOk(s"Found unsupported window expression: ${windowExpression.mkString(", ")}")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = if (needsProject) {
      getWindowRelWithProjection(
        substraitContext,
        child.output,
        operatorId,
        null,
        validation = true)
    } else {
      getWindowRelWithoutProjection(
        substraitContext,
        child.output,
        operatorId,
        null,
        validation = true)
    }

    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if (windowExpression == null || windowExpression.isEmpty) {
      // The computing for this project is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel = if (needsProject) {
      getWindowRelWithProjection(
        context,
        child.output,
        operatorId,
        childCtx.root,
        validation = false)
    } else {
      getWindowRelWithoutProjection(
        context,
        child.output,
        operatorId,
        childCtx.root,
        validation = false)
    }
    assert(currRel != null, "Window Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
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
