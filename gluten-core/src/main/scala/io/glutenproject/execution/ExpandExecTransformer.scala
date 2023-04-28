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

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, LiteralTransformer}
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.BindReferencesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import scala.collection.mutable.ArrayBuffer

case class ExpandExecTransformer(projections: Seq[Seq[Expression]],
                                 output: Seq[Attribute],
                                 child: SparkPlan)
  extends UnaryExecNode with TransformSupport with GlutenPlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetrics(sparkContext)

  val originalInputAttributes: Seq[Attribute] = child.output

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def supportsColumnar: Boolean = true

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

  def getRelNode(context: SubstraitContext,
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
      for (i <- 0 until projections.size) {
        val selections = ArrayBuffer.empty[Int]
        for (j <- 0 until projections(i).size) {
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
      val preExprNodes = new util.ArrayList[ExpressionNode]()
      preExprs.foreach { expr =>
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(
            expr,
            originalInputAttributes).doTransform(args)
        preExprNodes.add(exprNode)
      }

      val emitStartIndex = originalInputAttributes.size
      val inputRel = if (!validation) {
        RelBuilder.makeProjectRel(
          input,
          preExprNodes,
          context,
          operatorId,
          emitStartIndex)
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
      val projectSetExprNodes = new util.ArrayList[util.ArrayList[ExpressionNode]]()
      for (i <- 0 until projections.size) {
        val porjectExprNodes = new util.ArrayList[ExpressionNode]()
        for (j <- 0 until projections(i).size) {
          val projectExprNode = projections(i)(j) match {
            case l: Literal =>
              new LiteralTransformer(l).doTransform(args)
            case _ =>
              ExpressionBuilder.makeSelection(selectionMaps(i)(j))
          }
          
          porjectExprNodes.add(projectExprNode)
        }
        projectSetExprNodes.add(porjectExprNodes)
      }
      RelBuilder.makeExpandRel(
        inputRel,
        projectSetExprNodes,
        context,
        operatorId)
    } else {
      val projectSetExprNodes = new util.ArrayList[util.ArrayList[ExpressionNode]]()
      projections.foreach { projectSet =>
        val porjectExprNodes = new util.ArrayList[ExpressionNode]()
        projectSet.foreach { project =>
          var projectExprNode = ExpressionConverter
            .replaceWithExpressionTransformer(
              project,
              originalInputAttributes).doTransform(args)
          porjectExprNodes.add(projectExprNode)
        }
        projectSetExprNodes.add(porjectExprNodes)
      }

      if (!validation) {
        RelBuilder.makeExpandRel(
          input,
          projectSetExprNodes,
          context, operatorId)
      } else {
        // Use a extension node to send the input types through Substrait plan for a validation.
        val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
        for (attr <- originalInputAttributes) {
          inputTypeNodeList.add(
            ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        }

        val extensionNode = ExtensionBuilder.makeAdvancedExtension(
          Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
        RelBuilder.makeExpandRel(input,
          projectSetExprNodes,
          extensionNode, context, operatorId)
      }
    }
  }

  override def doValidateInternal(): Boolean = {
    if (!BackendsApiManager.getSettings.supportExpandExec()) {
      return false
    }
    if (projections.isEmpty) {
      return false
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = try {
      getRelNode(
        substraitContext,
        projections,
        child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logValidateFailure(
          s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
        return false
    }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
    } else {
      true
    }
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
      (getRelNode(
        context,
        projections,
        child.output, operatorId, childCtx.root, validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (getRelNode(
        context,
        projections,
        child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Expand Rel should be valid")
    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("doExecute is not supported in ColumnarExpandExec.")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExecTransformer =
    copy(child = newChild)
}
