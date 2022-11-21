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

import java.util

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.BindReferencesUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ExpandExecTransformer(
                                  projections: Seq[Seq[Expression]],
                                  groupExpression: Seq[NamedExpression],
                                  output: Seq[Attribute],
                                  child: SparkPlan)
  extends UnaryExecNode with TransformSupport {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_condproject"))
  val originalInputAttributes: Seq[Attribute] = child.output

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

  override def getStreamedLeafPlan: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getStreamedLeafPlan.")
  }

  override def getChild: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getChild.")
  }

  def getRelNode(context: SubstraitContext,
                 projections: Seq[Seq[Expression]],
                 groupExpression: Seq[NamedExpression],
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val groupSize = groupExpression.size
    val aggSize = projections(0).size - groupSize

    val groupsetExprNodes = new util.ArrayList[util.ArrayList[ExpressionNode]]()
    val aggExprNodes = new util.ArrayList[ExpressionNode]()
    for (i <- 0 until aggSize) {
      val expr = ExpressionConverter
        .replaceWithExpressionTransformer(projections(0)(i),
          originalInputAttributes)
      val aggExprNode = expr.asInstanceOf[ExpressionTransformer].doTransform(args)
      aggExprNodes.add(aggExprNode)
    }

    projections.foreach { projection =>
      val groupExprNodes = new util.ArrayList[ExpressionNode]()
      for (i <- aggSize until (projection.size - 1)) {
        if (!(projection(i).isInstanceOf[Literal] &&
          projection(i).asInstanceOf[Literal].value == null)) {
          val expr = ExpressionConverter
            .replaceWithExpressionTransformer(projection(i),
              originalInputAttributes)
          val groupExprNode = expr.asInstanceOf[ExpressionTransformer].doTransform(args)
          groupExprNodes.add(groupExprNode)
        }
      }
      groupsetExprNodes.add(groupExprNodes)
    }

    val expandRel = if (!validation) {
      RelBuilder.makeExpandRel(
        input,
        "spark_grouping_id", groupsetExprNodes, aggExprNodes,
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
        "spark_grouping_id", groupsetExprNodes, aggExprNodes,
        extensionNode, context, operatorId)
    }

    // After ExpandRel in velox, the output is
    // grouping cols + agg cols + groupingID col,
    // here we need to add ProjectRel to
    // convert the ouput to agg cols + grouping cols + groupingID col order.
    val selectNodes = new java.util.ArrayList[ExpressionNode]()
    // Add agg cols index
    for (i <- (groupSize -1) until (aggSize + groupSize - 1) ) {
      selectNodes.add(ExpressionBuilder.makeSelection(i))
    }
    // Add grouping cols index
    for (i <- 0 until (groupSize - 1)) {
      selectNodes.add(ExpressionBuilder.makeSelection(i))
    }

    // Add groupID col index
    selectNodes.add(ExpressionBuilder.makeSelection(projections(0).size - 1))

    // Pass the reordered index agg + groupingsets + GID
    if (!validation) {
      RelBuilder.makeProjectRel(expandRel, selectNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- output) {
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(expandRel, selectNodes, extensionNode, context, operatorId)
    }
  }

  override def doValidate(): Boolean = {
    if (!BackendsApiManager.getSettings.supportExpandExec()) {
      return false
    }
    if (projections.isEmpty) {
      return false
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId

    val relNode = try {
      getRelNode(
        substraitContext,
        projections, groupExpression,
        child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      val validator = new ExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
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

    val operatorId = context.nextOperatorId
    if (projections == null || projections.isEmpty) {
      // The computing for this Expand is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context,
        projections, groupExpression,
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
        projections, groupExpression,
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
