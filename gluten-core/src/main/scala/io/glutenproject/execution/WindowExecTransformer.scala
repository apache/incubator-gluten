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
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{AggregateFunctionsBuilder, ConverterUtils, ExpressionConverter, ExpressionTransformer, WindowFunctionsBuilder}
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.BindReferencesUtil
import io.substrait.proto.SortField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, NamedExpression, Rank, RowNumber, SortOrder, SpecifiedWindowFrame, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util

case class WindowExecTransformer(windowExpression: Seq[NamedExpression],
                                 partitionSpec: Seq[Expression],
                                 orderSpec: Seq[SortOrder],
                                 child: SparkPlan) extends WindowExecBase with TransformSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "totalTime" -> SQLMetrics
      .createTimingMetric(sparkContext, "totaltime_window"))
  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val totalTime = longMetric("totalTime")
  val sparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  // We no longer require for sorted input for columnar window
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  /* private object NoneType {
    val NONE_TYPE = new NoneType
  }

  private class NoneType extends ArrowType {
    override def getTypeID: ArrowType.ArrowTypeID = {
      return ArrowTypeID.NONE
    }

    override def getType(builder: FlatBufferBuilder): Int = {
      throw new UnsupportedOperationException()
    }

    override def toString: String = {
      return "NONE"
    }

    override def accept[T](visitor: ArrowType.ArrowTypeVisitor[T]): T = {
      throw new UnsupportedOperationException()
    }

    override def isComplex: Boolean = false
  } */

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
    windowExpression.map { windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = aliasExpr.name
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case rowNumber: RowNumber =>
            val frame = rowNumber.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, rowNumber).toInt,
              new util.ArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(rowNumber.dataType, rowNumber.nullable),
              frame.upper.sql,
              frame.lower.sql,
              frame.frameType.sql)
            windowExpressions.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.
                      frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val aggregateFunc = aggExpression.aggregateFunction

            var outputMap: Map[Expression, Int] = Map()
            output.zipWithIndex.foreach { case (attr, index) =>
              outputMap += (attr.canonicalized -> index)
            }

            val childrenNodeList = new util.ArrayList[ExpressionNode]()
            val childrenNodes = aggregateFunc.children.toList.map(expr => {
              ExpressionBuilder.makeSelection(outputMap.get(expr.canonicalized).get)
            })
            for (node <- childrenNodes) {
              childrenNodeList.add(node)
            }

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(args, aggExpression.aggregateFunction).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              frame.upper.sql,
              frame.lower.sql,
              frame.frameType.sql)
            windowExpressions.add(windowFunctionNode)
          case rank: Rank =>
            val frame = rank.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, rank).toInt,
              new util.ArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(rank.dataType, rank.nullable),
              frame.upper.sql,
              frame.lower.sql,
              frame.frameType.sql)
            windowExpressions.add(windowFunctionNode)
          case _ =>
            throw new UnsupportedOperationException("unsupported window function type: " +
              wExpression.windowFunction)
        }
      }

    // Partition By Expressions
    val partitionsExpressions = new util.ArrayList[ExpressionNode]()
    partitionSpec.map { partitionExpr =>
      val expr = ExpressionConverter
        .replaceWithExpressionTransformer(partitionExpr, attributeSeq = child.output)
      val exprNode = expr.asInstanceOf[ExpressionTransformer].doTransform(args)
      partitionsExpressions.add(exprNode)
    }

    // Sort By Expressions
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

  override def doValidate(): Boolean = {
    if (!BackendsApiManager.getSettings.supportWindowExec()) {
      return false
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId

    val relNode = try {
      getRelNode(
        substraitContext,
        windowExpression, partitionSpec,
        orderSpec, child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext,
        Lists.newArrayList(relNode))
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

    val operatorId = context.nextOperatorId
    if (windowExpression == null || windowExpression.isEmpty) {
      // The computing for this project is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context, windowExpression,
        partitionSpec, orderSpec, child.output,
        operatorId, childCtx.root, validation = false),
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
        context, windowExpression,
        partitionSpec, orderSpec,
        child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Window Rel should be valid")
    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
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
