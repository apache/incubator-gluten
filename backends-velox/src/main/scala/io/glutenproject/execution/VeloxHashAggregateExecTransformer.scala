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
import java.util.ArrayList

import scala.collection.JavaConverters._
import com.google.protobuf.Any
import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import java.util

import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{DoubleType, LongType}

case class VeloxHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  /**
   * Returns whether extracting subfield from struct is needed.
   * True when the intermediate type of Velox aggregation is a compound type.
   * @return extracting needed or not.
   */
  def extractStructNeeded(): Boolean = {
    for (expr <- aggregateExpressions) {
      val aggregateFunction = expr.aggregateFunction
      aggregateFunction match {
        // TODO: also handle stddev.
        case _: Average =>
          expr.mode match {
            case Partial =>
              return true
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  /**
   * Add a projection after aggregation to extract subfields from Struct.
   * @param context the Substrait context
   * @param aggRel the aggregation rel
   * @param operatorId the operator id
   * @return a project rel
   */
  def applyExtractStruct(context: SubstraitContext,
                         aggRel: RelNode,
                         operatorId: Long,
                         validation: Boolean): RelNode = {
    val expressionNodes = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(colIdx)
      expressionNodes.add(groupingExpr)
      colIdx += 1
    }

    for (expr <- aggregateExpressions) {
      expr.mode match {
        case Partial =>
        case _ =>
          throw new UnsupportedOperationException(s"${expr.mode} not supported.")
      }
      expr.aggregateFunction match {
        case _: Average =>
          // Select sum from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 0))
          // Select count from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 1))
          colIdx += 1
        case _ =>
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
      }
    }
    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, expressionNodes, context, operatorId)
    } else {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(getPartialAggOutTypes).toProtobuf))
      RelBuilder.makeProjectRel(aggRel, expressionNodes, extensionNode, context, operatorId)
    }
  }

  // Create aggregate function node and add to list.
  override protected def addFunctionNode(
    args: java.lang.Object,
    aggregateFunction: AggregateFunction,
    childrenNodeList: ArrayList[ExpressionNode],
    aggregateMode: AggregateMode,
    aggregateNodeList: java.util.ArrayList[AggregateFunctionNode]): Unit = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    aggregateFunction match {
      case _: Average =>
        aggregateMode match {
          case Partial =>
            // Will use sum and count to replace partial avg.
            assert(childrenNodeList.size() == 1, "Partial Average expects one child node.")

            // Use struct type to represent Velox Row(DOUBLE, BIGINT).
            val structTypeNodes = new util.ArrayList[TypeNode]()
            structTypeNodes.add(ConverterUtils.getTypeNode(DoubleType, nullable = true))
            structTypeNodes.add(ConverterUtils.getTypeNode(LongType, nullable = true))

            val avgNode = ExpressionBuilder.makeAggregateFunction(
              AggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              TypeBuilder.makeStruct(structTypeNodes))
            aggregateNodeList.add(avgNode)
          case Final =>
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              AggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
            aggregateNodeList.add(aggFunctionNode)
          case other =>
            throw new UnsupportedOperationException(s"$other is not supported.")
        }
      case _ =>
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          AggregateFunctionsBuilder.create(args, aggregateFunction),
          childrenNodeList,
          modeToKeyWord(aggregateMode),
          ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
        aggregateNodeList.add(aggFunctionNode)
    }
  }

  /**
   * Return the output types after partial avg through Velox.
   * @return
   */
  def getPartialAggOutTypes: java.util.ArrayList[TypeNode] = {
    val typeNodeList = new java.util.ArrayList[TypeNode]()
    groupingExpressions.foreach(expression => {
      typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
    })
    // TODO: consider stddev here.
    aggregateExpressions.foreach(expression => {
      val aggregateFunction = expression.aggregateFunction
      aggregateFunction match {
        case _: Average =>
          expression.mode match {
            case Partial =>
              val structTypeNodes = new util.ArrayList[TypeNode]()
              structTypeNodes.add(ConverterUtils.getTypeNode(DoubleType, nullable = true))
              structTypeNodes.add(ConverterUtils.getTypeNode(LongType, nullable = true))
              typeNodeList.add(TypeBuilder.makeStruct(structTypeNodes))
            case Final =>
              typeNodeList.add(
                ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          typeNodeList.add(
            ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
      }
    })
    typeNodeList
  }

  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  private def rowConstructNeeded: Boolean = {
    for (aggregateExpression <- aggregateExpressions) {
      aggregateExpression.mode match {
        case PartialMerge | Final =>
          if (aggregateExpression.aggregateFunction.inputAggBufferAttributes.size > 1) {
            return true
          }
        case _ =>
      }
    }
    false
  }

  // Return a scalar function node representing row construct function in Velox.
  private def getRowConstructNode(args: java.lang.Object,
                                  childNodes: util.ArrayList[ExpressionNode],
                                  rowConstructAttributes: Seq[Attribute]): ScalarFunctionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.ROW_CONSTRUCTOR,
      rowConstructAttributes.map(attr => attr.dataType),
      FunctionConfig.NON)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = new util.ArrayList[TypeNode](rowConstructAttributes.map(attr =>
      ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    ).asJava)

    ExpressionBuilder.makeScalarFunction(
      functionId, childNodes, TypeBuilder.makeStruct(structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Currently mainly used for final average.
  // Pre-projection is always not required for final stage.
  private def getAggRelWithRowConstruct(context: SubstraitContext,
                                        originalInputAttributes: Seq[Attribute],
                                        operatorId: Long,
                                        inputRel: RelNode,
                                        validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Create a projection for row construct.
    val exprNodes = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(expr => {
      exprNodes.add(ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes)
        .asInstanceOf[ExpressionTransformer].doTransform(args))
    })

    for (aggregateExpression <- aggregateExpressions) {
      val functionInputAttributes = aggregateExpression.aggregateFunction.inputAggBufferAttributes
      aggregateExpression.aggregateFunction match {
        case Average(_, _) =>
          aggregateExpression.mode match {
            case Final =>
              assert(
                functionInputAttributes.size == 2, "Final Average expects two input attributes.")
              // Use a Velox function to combine sum and count.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList.map(attr => {
                val aggExpr: Expression = ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
                }).asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, functionInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          assert(functionInputAttributes.size == 1, "Only one input attribute is expected.")
          val childNodes = new util.ArrayList[ExpressionNode](
            functionInputAttributes.toList.map(attr => {
            val aggExpr: Expression = ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          }).asJava)
          exprNodes.addAll(childNodes)
      }
    }

    // Create a project rel.
    val projectRel = if (!validation) {
      RelBuilder.makeProjectRel(inputRel, exprNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(inputRel, exprNodes, extensionNode, context, operatorId)
    }

    // Create aggregation rel.
    val groupingList = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach(_ => {
      groupingList.add(ExpressionBuilder.makeSelection(colIdx))
      colIdx += 1
    })

    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodes = new util.ArrayList[ExpressionNode]()
      aggregateFunc match {
        case Average(_, _) =>
          // Only occupies one column due to sum and count are combined by previous projection.
          childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
        case _ =>
          aggregateFunc.children.toList.map(_ => {
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
            aggExpr
          })
      }
      addFunctionNode(args, aggregateFunc, childrenNodes, aggExpr.mode, aggregateFunctionList)
    })
    RelBuilder.makeAggregateRel(
      projectRel, groupingList, aggregateFunctionList, context, operatorId)
  }

  /**
   * Create and return the Rel for the this aggregation.
   * @param context the Substrait context
   * @param operatorId the operator id
   * @param aggParams the params for aggregation mainly used for metrics updating
   * @param input tht input rel node
   * @param validation whether this is for native validation
   * @return the rel node for this aggregation
   */
  override protected def getAggRel(context: SubstraitContext,
                                   operatorId: Long,
                                   aggParams: AggregationParams,
                                   input: RelNode = null,
                                   validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    val preProjectionNeeded = needsPreProjection

    var aggRel = if (preProjectionNeeded) {
      aggParams.preProjectionNeeded = true
      getAggRelWithPreProjection(
        context, originalInputAttributes, operatorId, input, validation)
    } else {
      if (rowConstructNeeded) {
        aggParams.preProjectionNeeded = true
        getAggRelWithRowConstruct(
          context, originalInputAttributes, operatorId, input, validation)
      } else {
        getAggRelWithoutPreProjection(
          context, originalInputAttributes, operatorId, input, validation)
      }
    }

    if (extractStructNeeded()) {
      aggParams.extractionNeeded = true
      aggRel = applyExtractStruct(context, aggRel, operatorId, validation)
    }

    val resRel = if (!needsPostProjection(allAggregateResultAttributes)) {
      aggRel
    } else {
      aggParams.postProjectionNeeded = true
      applyPostProjection(context, aggRel, operatorId, validation)
    }
    context.registerAggregationParam(operatorId, aggParams)
    resRel
  }
  override protected def withNewChildInternal(newChild: SparkPlan)
    : VeloxHashAggregateExecTransformer = {
      copy(child = newChild)
    }
}
