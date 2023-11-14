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
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, LiteralTransformer}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.Any

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class ExpandExecTransformer(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetrics(sparkContext)

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(projections.flatten.map(_.references))
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetricsUpdater(metrics)

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  def getRelNode(
      context: SubstraitContext,
      projections: Seq[Seq[Expression]],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    def needsPreProjection(projections: Seq[Seq[Expression]]): Boolean = {
      projections
        .exists(set => set.exists(p => !p.isInstanceOf[Attribute] && !p.isInstanceOf[Literal]))
    }
    if (needsPreProjection(projections)) {
      // if there is not literal and attribute expression in project sets, add a project op
      // to calculate them before expand op.
      val preExprs = ArrayBuffer.empty[Expression]
      val selectionMaps = ArrayBuffer.empty[Seq[Int]]
      var preExprIndex = 0
      for (i <- projections.indices) {
        val selections = ArrayBuffer.empty[Int]
        for (j <- projections(i).indices) {
          val proj = projections(i)(j)
          if (!proj.isInstanceOf[Literal]) {
            val exprIdx = preExprs.indexWhere(expr => expr.semanticEquals(proj))
            if (exprIdx != -1) {
              selections += exprIdx
            } else {
              preExprs += proj
              selections += preExprIndex
              preExprIndex = preExprIndex + 1
            }
          } else {
            selections += -1
          }
        }
        selectionMaps += selections
      }
      // make project
      val preExprNodes = preExprs
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
        val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
        for (attr <- originalInputAttributes) {
          inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        }
        val extensionNode = ExtensionBuilder.makeAdvancedExtension(
          Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
        RelBuilder.makeProjectRel(
          input,
          preExprNodes,
          extensionNode,
          context,
          operatorId,
          emitStartIndex)
      }

      // make expand
      val projectSetExprNodes = new JArrayList[JList[ExpressionNode]]()
      for (i <- projections.indices) {
        val projectExprNodes = new JArrayList[ExpressionNode]()
        for (j <- projections(i).indices) {
          val projectExprNode = projections(i)(j) match {
            case l: Literal =>
              LiteralTransformer(l).doTransform(args)
            case _ =>
              ExpressionBuilder.makeSelection(selectionMaps(i)(j))
          }

          projectExprNodes.add(projectExprNode)
        }
        projectSetExprNodes.add(projectExprNodes)
      }
      RelBuilder.makeExpandRel(inputRel, projectSetExprNodes, context, operatorId)
    } else {
      val projectSetExprNodes = new JArrayList[JList[ExpressionNode]]()
      projections.foreach {
        projectSet =>
          val projectExprNodes = new JArrayList[ExpressionNode]()
          projectSet.foreach {
            project =>
              val projectExprNode = ExpressionConverter
                .replaceWithExpressionTransformer(project, originalInputAttributes)
                .doTransform(args)
              projectExprNodes.add(projectExprNode)
          }
          projectSetExprNodes.add(projectExprNodes)
      }

      if (!validation) {
        RelBuilder.makeExpandRel(input, projectSetExprNodes, context, operatorId)
      } else {
        // Use a extension node to send the input types through Substrait plan for a validation.
        val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
        for (attr <- originalInputAttributes) {
          inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        }

        val extensionNode = ExtensionBuilder.makeAdvancedExtension(
          Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
        RelBuilder.makeExpandRel(input, projectSetExprNodes, extensionNode, context, operatorId)
      }
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportExpandExec()) {
      return ValidationResult.notOk("Current backend does not support expand")
    }
    if (projections.isEmpty) {
      return ValidationResult.notOk("Current backend does not support empty projections in expand")
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getRelNode(substraitContext, projections, child.output, operatorId, null, validation = true)

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
    if (projections == null || projections.isEmpty) {
      // The computing for this Expand is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (
        getRelNode(
          context,
          projections,
          child.output,
          operatorId,
          childCtx.root,
          validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val readRel = RelBuilder.makeReadRel(child.output.asJava, context, operatorId)
      (
        getRelNode(context, projections, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Expand Rel should be valid")
    TransformContext(inputAttributes, output, currRel)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExecTransformer =
    copy(child = newChild)
}
