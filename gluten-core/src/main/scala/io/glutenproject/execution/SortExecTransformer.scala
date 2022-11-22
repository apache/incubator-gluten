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

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
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
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

case class SortExecTransformer(
                                sortOrder: Seq[SortOrder],
                                global: Boolean,
                                child: SparkPlan,
                                testSpillFrequency: Int = 0)
  extends UnaryExecNode
    with TransformSupport {

  override lazy val metrics = Map(
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_sort"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in cache all data"),
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in sort process"),
    "shuffleTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in shuffle process"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"))
  val sparkConf = sparkContext.getConf
  val elapse = longMetric("processTime")
  val sortTime = longMetric("sortTime")
  val shuffleTime = longMetric("shuffleTime")
  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def getChild: SparkPlan = child

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

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    numOutputBatches += outNumBatches
    numOutputRows += outNumRows
  }

  protected def needsPreProjection(
                                    sortOrders: Seq[SortOrder]): Boolean = {
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
    sortOrder.map(order => {
      val builder = SortField.newBuilder();
      val expr = ExpressionConverter
        .replaceWithExpressionTransformer(order.child, originalInputAttributes)
      val projectExprNode = expr.asInstanceOf[ExpressionTransformer].doTransform(args)
      projectExpressions.add(projectExprNode)

      val exprNode = ExpressionBuilder.makeSelection(colIdx)
      sortExprArttributes.add(
        AttributeReference(s"col_${colIdx}", order.child.dataType)())
      colIdx += 1
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

    // Add a Project Rel both original columns and the sorting columns
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, projectExpressions, context, operatorId)
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
      RelBuilder.makeProjectRel(input, projectExpressions, extensionNode, context, operatorId)
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
      RelBuilder.makeProjectRel(sortRel, new java.util.ArrayList[ExpressionNode](
        selectOrigins.asJava), context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(sortRel, new java.util.ArrayList[ExpressionNode](
        selectOrigins.asJava), extensionNode, context, operatorId)
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
    sortOrder.map(order => {
      val builder = SortField.newBuilder();
      val expr = ExpressionConverter
        .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
      val exprNode = expr.asInstanceOf[ExpressionTransformer].doTransform(args)
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
    val needsProjection = needsPreProjection(sortOrder: Seq[SortOrder])

    if (needsProjection) {
      getRelWithProject(context, sortOrder, originalInputAttributes, operatorId, input, validation)
    } else {
      getRelWithoutProject(
        context, sortOrder, originalInputAttributes, operatorId, input, validation)
    }
  }

  override def doValidate(): Boolean = {
    if (!BackendsApiManager.getSettings.supportSortExec()) {
      return false
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId

    val relNode = try {
      getRelNode(
        substraitContext, sortOrder, child.output, operatorId, null, validation = true)
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
