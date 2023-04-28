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
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter}
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.utils.BindReferencesUtil
import io.substrait.proto.SortField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

case class SortExecTransformer(sortOrder: Seq[SortOrder],
                               global: Boolean,
                               child: SparkPlan,
                               testSpillFrequency: Int = 0)
  extends UnaryExecNode with TransformSupport with GlutenPlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genSortTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genSortTransformerMetricsUpdater(metrics)

  val sparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  def getRelWithProject(context: SubstraitContext,
                        sortOrder: Seq[SortOrder],
                        originalInputAttributes: Seq[Attribute],
                        operatorId: Long,
                        input: RelNode,
                        validation: Boolean): RelNode = {
    val args = context.registeredFunction

    val sortFieldList = new util.ArrayList[SortField]()
    val projectExpressions = new util.ArrayList[ExpressionNode]()
    val sortExprArttributes = new util.ArrayList[AttributeReference]()

    val selectOrigins =
      originalInputAttributes.indices.map(ExpressionBuilder.makeSelection(_))
    projectExpressions.addAll(selectOrigins.asJava)

    var colIdx = originalInputAttributes.size
    sortOrder.foreach(order => {
      val builder = SortField.newBuilder();
      val projectExprNode = ExpressionConverter
        .replaceWithExpressionTransformer(order.child, originalInputAttributes).doTransform(args)
      projectExpressions.add(projectExprNode)

      val exprNode = ExpressionBuilder.makeSelection(colIdx)
      sortExprArttributes.add(
        AttributeReference(s"col_${colIdx}", order.child.dataType)())
      colIdx += 1
      builder.setExpr(exprNode.toProtobuf)

      builder.setDirectionValue(SortExecTransformer.transformSortDirection(order.direction.sql,
        order.nullOrdering.sql))
      sortFieldList.add(builder.build())
    })

    // Add a Project Rel both original columns and the sorting columns
    val emitStartIndex = originalInputAttributes.size
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, projectExpressions, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      sortExprArttributes.forEach { attr =>
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input, projectExpressions, extensionNode, context, operatorId, emitStartIndex)
    }

    val sortRel = if (!validation) {
      RelBuilder.makeSortRel(
        inputRel, sortFieldList, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      sortExprArttributes.forEach { attr =>
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))

      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeSortRel(
        inputRel, sortFieldList, extensionNode, context, operatorId)
    }

    // Add a Project Rel to remove the sorting columns
    if (!validation) {
      RelBuilder.makeProjectRel(
        sortRel,
        new java.util.ArrayList[ExpressionNode](selectOrigins.asJava),
        context, operatorId, originalInputAttributes.size + sortFieldList.size)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        sortRel, new java.util.ArrayList[ExpressionNode](selectOrigins.asJava),
        extensionNode, context, operatorId, originalInputAttributes.size + sortFieldList.size)
    }
  }

  def getRelWithoutProject(context: SubstraitContext,
                           sortOrder: Seq[SortOrder],
                           originalInputAttributes: Seq[Attribute],
                           operatorId: Long,
                           input: RelNode,
                           validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val sortFieldList = new util.ArrayList[SortField]()
    sortOrder.foreach(order => {
      val builder = SortField.newBuilder();
      val exprNode = ExpressionConverter
        .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
        .doTransform(args)
      builder.setExpr(exprNode.toProtobuf)

      builder.setDirectionValue(SortExecTransformer.transformSortDirection(order.direction.sql,
        order.nullOrdering.sql))
      sortFieldList.add(builder.build())
    })
    if (!validation) {
      RelBuilder.makeSortRel(
        input, sortFieldList, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeSortRel(
        input, sortFieldList, extensionNode, context, operatorId)
    }
  }

  def getRelNode(context: SubstraitContext,
                 sortOrder: Seq[SortOrder],
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val needsProjection = SortExecTransformer.needProjection(sortOrder: Seq[SortOrder])

    if (needsProjection) {
      getRelWithProject(context, sortOrder,
        originalInputAttributes, operatorId, input, validation)
    } else {
      getRelWithoutProject(
        context, sortOrder, originalInputAttributes, operatorId, input, validation)
    }
  }

  override def doValidateInternal(): Boolean = {
    if (!BackendsApiManager.getSettings.supportSortExec()) {
      return false
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = try {
      getRelNode(
        substraitContext, sortOrder, child.output, operatorId, null, validation = true)
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
    if (sortOrder == null || sortOrder.isEmpty) {
      // The computing for this project is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context, sortOrder, child.output, operatorId, childCtx.root, validation = false),
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
        context, sortOrder, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Sort Rel should be valid")
    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarSortExec doesn't support doExecute")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SortExecTransformer =
    copy(child = newChild)
}

object SortExecTransformer {
  def transformSortDirection(direction: String, nullOrdering: String): Int = {
    (direction, nullOrdering) match {
      case ("ASC", "NULLS FIRST") => 1
      case ("ASC", "NULLS LAST") => 2
      case ("DESC", "NULLS FIRST") => 3
      case ("DESC", "NULLS LAST") => 4
      case _ => 0
    }
  }

  def needProjection(sortOrders: Seq[SortOrder]): Boolean = {
    var needsProjection = false
    breakable {
      for (sortOrder <- sortOrders) {
        if (!sortOrder.child.isInstanceOf[Attribute]) {
          needsProjection = true
          break
        }
      }
    }
    needsProjection
  }
}
