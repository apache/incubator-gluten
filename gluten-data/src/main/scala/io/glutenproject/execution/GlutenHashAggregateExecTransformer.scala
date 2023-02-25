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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, LongType}

case class GlutenHashAggregateExecTransformer(
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
        case _: Average | _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
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
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          // Select count from Velox struct with count casted from LongType into DoubleType.
          expressionNodes.add(ExpressionBuilder
            .makeCast(ConverterUtils.getTypeNode(DoubleType, nullable = true),
              ExpressionBuilder.makeSelection(colIdx, 0), SQLConf.get.ansiEnabled))
          // Select avg from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 1))
          // Select m2 from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 2))
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
        Any.pack(TypeBuilder.makeStruct(false, getPartialAggOutTypes).toProtobuf))
      RelBuilder.makeProjectRel(aggRel, expressionNodes, extensionNode, context, operatorId)
    }
  }

  /**
   * Return the intermediate type node of a partial aggregation in Velox.
   * @param aggregateFunction The aggregation function.
   * @return The type of partial outputs.
   */
  private def getIntermediateTypeNode(aggregateFunction: AggregateFunction): TypeNode = {
    val structTypeNodes = new util.ArrayList[TypeNode]()
    aggregateFunction match {
      case _: Average =>
        // Use struct type to represent Velox Row(DOUBLE, BIGINT).
        structTypeNodes.add(ConverterUtils.getTypeNode(DoubleType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(LongType, nullable = true))
      case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
        // Use struct type to represent Velox Row(BIGINT, DOUBLE, DOUBLE).
        structTypeNodes.add(ConverterUtils.getTypeNode(LongType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(DoubleType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(DoubleType, nullable = true))
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
    TypeBuilder.makeStruct(false, structTypeNodes)
  }

  // Create aggregate function node and add to list.
  override protected def addFunctionNode(
    args: java.lang.Object,
    aggregateFunction: AggregateFunction,
    childrenNodeList: java.util.ArrayList[ExpressionNode],
    aggregateMode: AggregateMode,
    aggregateNodeList: java.util.ArrayList[AggregateFunctionNode]): Unit = {
    aggregateFunction match {
      case _: Average | _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
        aggregateMode match {
          case Partial =>
            assert(childrenNodeList.size() == 1, "Partial stage expects one child node.")
            val partialNode = ExpressionBuilder.makeAggregateFunction(
              AggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              getIntermediateTypeNode(aggregateFunction))
            aggregateNodeList.add(partialNode)
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
   * Return the output types after partial aggregation through Velox.
   * @return
   */
  def getPartialAggOutTypes: java.util.ArrayList[TypeNode] = {
    val typeNodeList = new java.util.ArrayList[TypeNode]()
    groupingExpressions.foreach(expression => {
      typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
    })
    aggregateExpressions.foreach(expression => {
      val aggregateFunction = expression.aggregateFunction
      aggregateFunction match {
        case _: Average | _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          expression.mode match {
            case Partial =>
              typeNodeList.add(getIntermediateTypeNode(aggregateFunction))
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
      functionId, childNodes, TypeBuilder.makeStruct(false, structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Mainly used for aggregation whose intermediate type is a compound type in Velox.
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
        .doTransform(args))
    })

    for (aggregateExpression <- aggregateExpressions) {
      val functionInputAttributes = aggregateExpression.aggregateFunction.inputAggBufferAttributes
      aggregateExpression.aggregateFunction match {
        case Average(_, _) =>
          aggregateExpression.mode match {
            case Final =>
              assert(functionInputAttributes.size == 2,
                "Final stage of Average expects two input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList.map(attr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  .doTransform(args)
                }).asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, functionInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          aggregateExpression.mode match {
            case Final =>
              assert(functionInputAttributes.size == 3,
                "Final stage of StddevSamp expects three input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              var index = 0
              var newInputAttributes: Seq[Attribute] = Seq()
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList.map(attr => {
                  val aggExpr: ExpressionTransformer = ExpressionConverter
                    .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  val aggNode = aggExpr.doTransform(args)
                  val expressionNode = if (index == 0) {
                    // Cast count from DoubleType into LongType to align with Velox semantics.
                    newInputAttributes = newInputAttributes :+
                      attr.copy(attr.name, LongType, attr.nullable, attr.metadata)(
                        attr.exprId, attr.qualifier)
                    ExpressionBuilder.makeCast(
                      ConverterUtils.getTypeNode(LongType, attr.nullable), aggNode,
                      SQLConf.get.ansiEnabled)
                  } else {
                    newInputAttributes = newInputAttributes :+ attr
                    aggNode
                  }
                  index += 1
                  expressionNode
                }).asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, newInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          assert(functionInputAttributes.size == 1, "Only one input attribute is expected.")
          val childNodes = new util.ArrayList[ExpressionNode](
            functionInputAttributes.toList.map(attr => {
            ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
              .doTransform(args)
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
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(inputRel, exprNodes, extensionNode, context, operatorId)
    }

    // Create aggregation rel.
    val groupingList = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach(_ => {
      groupingList.add(ExpressionBuilder.makeSelection(colIdx))
      colIdx += 1
    })

    val aggFilterList = new util.ArrayList[ExpressionNode]()
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      if (aggExpr.filter.isDefined) {
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(aggExpr.filter.get, child.output).doTransform(args)
        aggFilterList.add(exprNode)
      }

      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodes = new util.ArrayList[ExpressionNode]()
      aggregateFunc match {
        case _: Average | _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          // Only occupies one column due to intermediate results are combined
          // by previous projection.
          childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
        case _ =>
          aggregateFunc.inputAggBufferAttributes.toList.map(_ => {
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
            aggExpr
          })
      }
      addFunctionNode(args, aggregateFunc, childrenNodes, aggExpr.mode, aggregateFunctionList)
    })
    RelBuilder.makeAggregateRel(
      projectRel, groupingList, aggregateFunctionList, aggFilterList,
      context, operatorId)
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

  def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)

  override protected def withNewChildInternal(newChild: SparkPlan)
    : GlutenHashAggregateExecTransformer = {
      copy(child = newChild)
    }
}
