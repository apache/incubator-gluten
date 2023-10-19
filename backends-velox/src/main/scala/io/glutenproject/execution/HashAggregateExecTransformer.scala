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

import io.glutenproject.execution.VeloxAggregateFunctionsBuilder.{veloxFourIntermediateTypes, veloxSixIntermediateTypes, veloxThreeIntermediateTypes}
import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.GlutenDecimalUtil

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.google.protobuf.Any

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class HashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override protected def getAttrForAggregateExpr(
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      index: Int): Int = {
    var resIndex = index
    val mode = exp.mode
    val aggregateFunc = exp.aggregateFunction
    aggregateFunc match {
      case hllAdapter: HLLAdapter =>
        mode match {
          case Partial =>
            val aggBufferAttr = hllAdapter.inputAggBufferAttributes
            for (index <- aggBufferAttr.indices) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            resIndex += aggBufferAttr.size
          case Final =>
            aggregateAttr += aggregateAttributeList(resIndex)
            resIndex += 1
          case other =>
            throw new UnsupportedOperationException(s"not currently supported: $other.")
        }
      case _ =>
        resIndex = super.getAttrForAggregateExpr(exp, aggregateAttributeList, aggregateAttr, index)
    }
    resIndex
  }

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }

  /**
   * Returns whether extracting subfield from struct is needed. True when the intermediate type of
   * Velox aggregation is a compound type.
   * @return
   *   extracting needed or not.
   */
  def extractStructNeeded(): Boolean = {
    for (expr <- aggregateExpressions) {
      val aggregateFunction = expr.aggregateFunction
      aggregateFunction match {
        case _: Average | _: First | _: Last | _: StddevSamp | _: StddevPop | _: VarianceSamp |
            _: VariancePop | _: Corr | _: CovPopulation | _: CovSample =>
          expr.mode match {
            case Partial | PartialMerge =>
              return true
            case _ =>
          }
        case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
          expr.mode match {
            case Partial | PartialMerge =>
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
   * @param context
   *   the Substrait context
   * @param aggRel
   *   the aggregation rel
   * @param operatorId
   *   the operator id
   * @return
   *   a project rel
   */
  def applyExtractStruct(
      context: SubstraitContext,
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
        case Partial | PartialMerge =>
        case _ =>
          throw new UnsupportedOperationException(s"${expr.mode} not supported.")
      }
      expr.aggregateFunction match {
        case _: Average | _: First | _: Last =>
          // Select first and second aggregate buffer from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 0))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 1))
          colIdx += 1
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          // Select count from Velox struct with count casted from LongType into DoubleType.
          expressionNodes.add(
            ExpressionBuilder
              .makeCast(
                ConverterUtils.getTypeNode(DoubleType, nullable = false),
                ExpressionBuilder.makeSelection(colIdx, 0),
                SQLConf.get.ansiEnabled))
          // Select avg from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 1))
          // Select m2 from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 2))
          colIdx += 1
        case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
          // Select sum from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 0))
          // Select isEmpty from Velox Struct.
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 1))
          colIdx += 1
        case _: Corr =>
          // Select count from Velox struct with count casted from LongType into DoubleType.
          expressionNodes.add(
            ExpressionBuilder
              .makeCast(
                ConverterUtils.getTypeNode(DoubleType, nullable = false),
                ExpressionBuilder.makeSelection(colIdx, 1),
                SQLConf.get.ansiEnabled))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 4))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 5))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 0))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 2))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 3))
          colIdx += 1
        case _: CovPopulation | _: CovSample =>
          // Select count from Velox struct with count casted from LongType into DoubleType.
          expressionNodes.add(
            ExpressionBuilder
              .makeCast(
                ConverterUtils.getTypeNode(DoubleType, nullable = false),
                ExpressionBuilder.makeSelection(colIdx, 1),
                SQLConf.get.ansiEnabled))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 2))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 3))
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, 0))
          colIdx += 1
        case _ =>
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
      }
    }
    if (!validation) {
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    } else {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, getPartialAggOutTypes).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        extensionNode,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    }
  }

  /**
   * Return the intermediate type node of a partial aggregation in Velox.
   * @param aggregateFunction
   *   The aggregation function.
   * @return
   *   The type of partial outputs.
   */
  private def getIntermediateTypeNode(aggregateFunction: AggregateFunction): TypeNode = {
    val structTypeNodes = new util.ArrayList[TypeNode]()
    aggregateFunction match {
      case avg: Average =>
        structTypeNodes.add(
          ConverterUtils.getTypeNode(GlutenDecimalUtil.getAvgSumDataType(avg), nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(LongType, nullable = true))
      case first: First =>
        structTypeNodes.add(ConverterUtils.getTypeNode(first.dataType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(BooleanType, nullable = true))
      case last: Last =>
        structTypeNodes.add(ConverterUtils.getTypeNode(last.dataType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(BooleanType, nullable = true))
      case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
        // Use struct type to represent Velox Row(BIGINT, DOUBLE, DOUBLE).
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxThreeIntermediateTypes.head, nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxThreeIntermediateTypes(1), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxThreeIntermediateTypes(2), nullable = false))
      case _: Corr =>
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes.head, nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes(1), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes(2), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes(3), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes(4), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxSixIntermediateTypes(5), nullable = false))
      case _: CovPopulation | _: CovSample =>
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxFourIntermediateTypes.head, nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxFourIntermediateTypes(1), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxFourIntermediateTypes(2), nullable = false))
        structTypeNodes.add(
          ConverterUtils
            .getTypeNode(veloxFourIntermediateTypes(3), nullable = false))
      case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
        structTypeNodes.add(ConverterUtils.getTypeNode(sum.dataType, nullable = true))
        structTypeNodes.add(ConverterUtils.getTypeNode(BooleanType, nullable = false))
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
    TypeBuilder.makeStruct(false, structTypeNodes)
  }

  override protected def modeToKeyWord(aggregateMode: AggregateMode): String = {
    super.modeToKeyWord(if (mixedPartialAndMerge) Partial else aggregateMode)
  }

  // Create aggregate function node and add to list.
  override protected def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: java.util.ArrayList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: java.util.ArrayList[AggregateFunctionNode]): Unit = {
    // This is a special handling for PartialMerge in the execution of distinct.
    // Use Partial phase instead for this aggregation.
    val modeKeyWord = modeToKeyWord(aggregateMode)

    def generateMergeCompanionNode(): Unit = {
      aggregateMode match {
        case Partial =>
          val partialNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
            childrenNodeList,
            modeKeyWord,
            getIntermediateTypeNode(aggregateFunction))
          aggregateNodeList.add(partialNode)
        case PartialMerge =>
          val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder
              .create(args, aggregateFunction, mixedPartialAndMerge),
            childrenNodeList,
            modeKeyWord,
            getIntermediateTypeNode(aggregateFunction)
          )
          aggregateNodeList.add(aggFunctionNode)
        case Final =>
          val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
            childrenNodeList,
            modeKeyWord,
            ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
          )
          aggregateNodeList.add(aggFunctionNode)
        case other =>
          throw new UnsupportedOperationException(s"$other is not supported.")
      }
    }

    aggregateFunction match {
      case hllAdapter: HLLAdapter =>
        aggregateMode match {
          case Partial =>
            // For Partial mode output type is binary.
            val partialNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(
                hllAdapter.inputAggBufferAttributes.head.dataType,
                hllAdapter.inputAggBufferAttributes.head.nullable)
            )
            aggregateNodeList.add(partialNode)
          case Final =>
            // For Final mode output type is long.
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
            )
            aggregateNodeList.add(aggFunctionNode)
          case other =>
            throw new UnsupportedOperationException(s"$other is not supported.")
        }
      case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
        generateMergeCompanionNode()
      case _: Average | _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop | _: Corr |
          _: CovPopulation | _: CovSample | _: First | _: Last =>
        generateMergeCompanionNode()
      case _ =>
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          VeloxAggregateFunctionsBuilder.create(
            args,
            aggregateFunction,
            aggregateMode == PartialMerge && mixedPartialAndMerge),
          childrenNodeList,
          modeKeyWord,
          ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
        )
        aggregateNodeList.add(aggFunctionNode)
    }
  }

  /**
   * Return the output types after partial aggregation through Velox.
   * @return
   */
  def getPartialAggOutTypes: java.util.ArrayList[TypeNode] = {
    val typeNodeList = new java.util.ArrayList[TypeNode]()
    groupingExpressions.foreach(
      expression => {
        typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
      })

    aggregateExpressions.foreach(
      expression => {
        val aggregateFunction = expression.aggregateFunction
        aggregateFunction match {
          case _: Average | _: First | _: Last | _: StddevSamp | _: StddevPop | _: VarianceSamp |
              _: VariancePop | _: Corr | _: CovPopulation | _: CovSample =>
            expression.mode match {
              case Partial | PartialMerge =>
                typeNodeList.add(getIntermediateTypeNode(aggregateFunction))
              case Final =>
                typeNodeList.add(
                  ConverterUtils
                    .getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
              case other =>
                throw new UnsupportedOperationException(s"$other is not supported.")
            }
          case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
            expression.mode match {
              case Partial | PartialMerge =>
                typeNodeList.add(getIntermediateTypeNode(aggregateFunction))
              case Final =>
                typeNodeList.add(
                  ConverterUtils
                    .getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
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
  private def getRowConstructNode(
      args: java.lang.Object,
      childNodes: util.ArrayList[ExpressionNode],
      rowConstructAttributes: Seq[Attribute],
      withNull: Boolean = true): ScalarFunctionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      if (withNull) "row_constructor_with_null" else "row_constructor",
      rowConstructAttributes.map(attr => attr.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = new util.ArrayList[TypeNode](
      rowConstructAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava)

    ExpressionBuilder.makeScalarFunction(
      functionId,
      childNodes,
      TypeBuilder.makeStruct(false, structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Mainly used for aggregation whose intermediate type is a compound type in Velox.
  // Pre-projection is always not required for final stage.
  private def getAggRelWithRowConstruct(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      inputRel: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Create a projection for row construct.
    val exprNodes = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        exprNodes.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, originalInputAttributes)
            .doTransform(args))
      })

    for (aggregateExpression <- aggregateExpressions) {
      val functionInputAttributes = aggregateExpression.aggregateFunction.inputAggBufferAttributes
      val aggregateFunction = aggregateExpression.aggregateFunction
      aggregateFunction match {
        case _ if mixedPartialAndMerge && aggregateExpression.mode == Partial =>
          val childNodes = new util.ArrayList[ExpressionNode](
            aggregateFunction.children
              .map(
                attr => {
                  ExpressionConverter
                    .replaceWithExpressionTransformer(attr, originalInputAttributes)
                    .doTransform(args)
                })
              .asJava)
          exprNodes.addAll(childNodes)
        case avg: Average =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 2,
                s"${aggregateExpression.mode.toString} of Average expects two input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList
                  .map(
                    attr => {
                      ExpressionConverter
                        .replaceWithExpressionTransformer(attr, originalInputAttributes)
                        .doTransform(args)
                    })
                  .asJava)
              exprNodes.add(
                getRowConstructNode(
                  args,
                  childNodes,
                  functionInputAttributes,
                  withNull = !avg.dataType.isInstanceOf[DecimalType]))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: First | _: Last =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 2,
                s"${aggregateExpression.mode.toString} of First/Last expects two input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList
                  .map(
                    attr => {
                      ExpressionConverter
                        .replaceWithExpressionTransformer(attr, originalInputAttributes)
                        .doTransform(args)
                    })
                  .asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, functionInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 3,
                s"${aggregateExpression.mode.toString} mode of" +
                  s"${aggregateFunction.getClass.toString} expects three input attributes."
              )
              // Use a Velox function to combine the intermediate columns into struct.
              var index = 0
              var newInputAttributes: Seq[Attribute] = Seq()
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList
                  .map(
                    attr => {
                      val aggExpr: ExpressionTransformer = ExpressionConverter
                        .replaceWithExpressionTransformer(attr, originalInputAttributes)
                      val aggNode = aggExpr.doTransform(args)
                      val expressionNode = if (index == 0) {
                        // Cast count from DoubleType into LongType to align with Velox semantics.
                        newInputAttributes = newInputAttributes :+
                          attr.copy(attr.name, LongType, attr.nullable, attr.metadata)(
                            attr.exprId,
                            attr.qualifier)
                        ExpressionBuilder.makeCast(
                          ConverterUtils.getTypeNode(LongType, attr.nullable),
                          aggNode,
                          SQLConf.get.ansiEnabled)
                      } else {
                        newInputAttributes = newInputAttributes :+ attr
                        aggNode
                      }
                      index += 1
                      expressionNode
                    })
                  .asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, newInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: Corr =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 6,
                s"${aggregateExpression.mode.toString} mode of Corr expects 6 input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              var index = 0
              var newInputAttributes: Seq[Attribute] = Seq()
              val childNodes = new util.ArrayList[ExpressionNode]()
              // Velox's Corr order is [ck, n, xMk, yMk, xAvg, yAvg]
              // Spark's Corr order is [n, xAvg, yAvg, ck, xMk, yMk]
              val sparkCorrOutputAttr = aggregateFunction.inputAggBufferAttributes.map(_.name)
              val veloxInputOrder =
                VeloxAggregateFunctionsBuilder.veloxCorrIntermediateDataOrder.map(
                  name => sparkCorrOutputAttr.indexOf(name))
              for (order <- veloxInputOrder) {
                val attr = functionInputAttributes(order)
                val aggExpr: ExpressionTransformer = ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                val aggNode = aggExpr.doTransform(args)
                val expressionNode = if (order == 0) {
                  // Cast count from DoubleType into LongType to align with Velox semantics.
                  newInputAttributes = newInputAttributes :+
                    attr.copy(attr.name, LongType, attr.nullable, attr.metadata)(
                      attr.exprId,
                      attr.qualifier)
                  ExpressionBuilder.makeCast(
                    ConverterUtils.getTypeNode(LongType, attr.nullable),
                    aggNode,
                    SQLConf.get.ansiEnabled)
                } else {
                  newInputAttributes = newInputAttributes :+ attr
                  aggNode
                }
                index += 1
                childNodes.add(expressionNode)
              }
              exprNodes.add(getRowConstructNode(args, childNodes, newInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: CovPopulation | _: CovSample =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 4,
                s"${aggregateExpression.mode.toString} mode of" +
                  s"${aggregateFunction.getClass.toString} expects 4 input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              var index = 0
              var newInputAttributes: Seq[Attribute] = Seq()
              val childNodes = new util.ArrayList[ExpressionNode]()
              // Velox's Covar order is [ck, n, xAvg, yAvg]
              // Spark's Covar order is [n, xAvg, yAvg, ck]
              val sparkCorrOutputAttr = aggregateFunction.inputAggBufferAttributes.map(_.name)
              val veloxInputOrder =
                VeloxAggregateFunctionsBuilder.veloxCovarIntermediateDataOrder.map(
                  name => sparkCorrOutputAttr.indexOf(name))
              for (order <- veloxInputOrder) {
                val attr = functionInputAttributes(order)
                val aggExpr: ExpressionTransformer = ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                val aggNode = aggExpr.doTransform(args)
                val expressionNode = if (order == 0) {
                  // Cast count from DoubleType into LongType to align with Velox semantics.
                  newInputAttributes = newInputAttributes :+
                    attr.copy(attr.name, LongType, attr.nullable, attr.metadata)(
                      attr.exprId,
                      attr.qualifier)
                  ExpressionBuilder.makeCast(
                    ConverterUtils.getTypeNode(LongType, attr.nullable),
                    aggNode,
                    SQLConf.get.ansiEnabled)
                } else {
                  newInputAttributes = newInputAttributes :+ attr
                  aggNode
                }
                index += 1
                childNodes.add(expressionNode)
              }
              exprNodes.add(getRowConstructNode(args, childNodes, newInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case sum: Sum if sum.dataType.isInstanceOf[DecimalType] =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 2,
                "Final stage of Average expects two input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList
                  .map(
                    attr => {
                      ExpressionConverter
                        .replaceWithExpressionTransformer(attr, originalInputAttributes)
                        .doTransform(args)
                    })
                  .asJava)
              exprNodes.add(
                getRowConstructNode(args, childNodes, functionInputAttributes, withNull = false))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          if (functionInputAttributes.size != 1) {
            throw new UnsupportedOperationException("Only one input attribute is expected.")
          }
          val childNodes = new util.ArrayList[ExpressionNode](
            functionInputAttributes.toList
              .map(
                attr => {
                  ExpressionConverter
                    .replaceWithExpressionTransformer(attr, originalInputAttributes)
                    .doTransform(args)
                })
              .asJava)
          exprNodes.addAll(childNodes)
      }
    }

    // Create a project rel.
    val emitStartIndex = originalInputAttributes.size
    val projectRel = if (!validation) {
      RelBuilder.makeProjectRel(inputRel, exprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        inputRel,
        exprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }

    // Create aggregation rel.
    val groupingList = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach(
      _ => {
        groupingList.add(ExpressionBuilder.makeSelection(colIdx))
        colIdx += 1
      })

    val aggFilterList = new util.ArrayList[ExpressionNode]()
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          throw new UnsupportedOperationException("Filter in final aggregation is not supported.")
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }

        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = new util.ArrayList[ExpressionNode]()
        aggregateFunc match {
          case _: Average | _: First | _: Last | _: StddevSamp | _: StddevPop |
              _: VarianceSamp | _: VariancePop | _: Corr | _: CovPopulation | _: CovSample
              if aggExpr.mode == PartialMerge | aggExpr.mode == Final =>
            // Only occupies one column due to intermediate results are combined
            // by previous projection.
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
          case sum: Sum
              if sum.dataType.isInstanceOf[DecimalType] &&
                (aggExpr.mode == PartialMerge | aggExpr.mode == Final) =>
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
          case _ if aggExpr.mode == PartialMerge | aggExpr.mode == Final =>
            aggregateFunc.inputAggBufferAttributes.toList.map(
              _ => {
                childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
                colIdx += 1
                aggExpr
              })
          case _ if aggExpr.mode == Partial =>
            aggregateFunc.children.toList.map(
              _ => {
                childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
                colIdx += 1
                aggExpr
              })
          case function =>
            throw new UnsupportedOperationException(
              s"$function of ${aggExpr.mode.toString} is not supported.")
        }
        addFunctionNode(args, aggregateFunc, childrenNodes, aggExpr.mode, aggregateFunctionList)
      })
    RelBuilder.makeAggregateRel(
      projectRel,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      context,
      operatorId)
  }

  /**
   * Whether this is a mixed aggregation of partial and partial-merge aggregation functions.
   * @return
   *   whether partial and partial-merge functions coexist.
   */
  def mixedPartialAndMerge: Boolean = {
    val partialMergeExists = aggregateExpressions.exists(
      expression => {
        expression.mode == PartialMerge
      })
    val partialExists = aggregateExpressions.exists(
      expression => {
        expression.mode == Partial
      })
    partialMergeExists && partialExists
  }

  /**
   * Create and return the Rel for the this aggregation.
   * @param context
   *   the Substrait context
   * @param operatorId
   *   the operator id
   * @param aggParams
   *   the params for aggregation mainly used for metrics updating
   * @param input
   *   tht input rel node
   * @param validation
   *   whether this is for native validation
   * @return
   *   the rel node for this aggregation
   */
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output

    var aggRel = if (needsPreProjection) {
      aggParams.preProjectionNeeded = true
      getAggRelWithPreProjection(context, originalInputAttributes, operatorId, input, validation)
    } else {
      if (rowConstructNeeded) {
        aggParams.preProjectionNeeded = true
        getAggRelWithRowConstruct(context, originalInputAttributes, operatorId, input, validation)
      } else {
        getAggRelWithoutPreProjection(
          context,
          originalInputAttributes,
          operatorId,
          input,
          validation)
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
}

/** An aggregation function builder specifically used by Velox backend. */
object VeloxAggregateFunctionsBuilder {

  val veloxCorrIntermediateDataOrder: Seq[String] = Seq("ck", "n", "xMk", "yMk", "xAvg", "yAvg")
  val veloxCovarIntermediateDataOrder: Seq[String] = Seq("ck", "n", "xAvg", "yAvg")

  val veloxThreeIntermediateTypes: Seq[DataType] = Seq(LongType, DoubleType, DoubleType)
  val veloxFourIntermediateTypes: Seq[DataType] = Seq(DoubleType, LongType, DoubleType, DoubleType)
  val veloxSixIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)

  /**
   * Get the compatible input types for a Velox aggregate function.
   * @param aggregateFunc:
   *   the input aggreagate function.
   * @param forMergeCompanion:
   *   whether this is a special case to solve mixed aggregation phases.
   * @return
   *   the input types of a Velox aggregate function.
   */
  private def getInputTypes(
      aggregateFunc: AggregateFunction,
      forMergeCompanion: Boolean): Seq[DataType] = {
    if (!forMergeCompanion) {
      return aggregateFunc.children.map(child => child.dataType)
    }
    if (aggregateFunc.aggBufferAttributes.size == veloxThreeIntermediateTypes.size) {
      return Seq(
        StructType(
          veloxThreeIntermediateTypes
            .map(intermediateType => StructField("", intermediateType))
            .toArray))
    }
    if (aggregateFunc.aggBufferAttributes.size == veloxFourIntermediateTypes.size) {
      return Seq(
        StructType(
          veloxFourIntermediateTypes
            .map(intermediateType => StructField("", intermediateType))
            .toArray))
    }
    if (aggregateFunc.aggBufferAttributes.size == veloxSixIntermediateTypes.size) {
      return Seq(
        StructType(
          veloxSixIntermediateTypes
            .map(intermediateType => StructField("", intermediateType))
            .toArray))
    }
    if (aggregateFunc.aggBufferAttributes.size > 1) {
      return Seq(
        StructType(
          aggregateFunc.aggBufferAttributes
            .map(attribute => StructField("", attribute.dataType))
            .toArray))
    }
    aggregateFunc.aggBufferAttributes.map(child => child.dataType)
  }

  /**
   * Create an scalar function for the input aggregate function.
   * @param args:
   *   the function map.
   * @param aggregateFunc:
   *   the input aggregate function.
   * @param forMergeCompanion:
   *   whether this is a special case to solve mixed aggregation phases.
   * @return
   */
  def create(
      args: java.lang.Object,
      aggregateFunc: AggregateFunction,
      forMergeCompanion: Boolean = false): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    var sigName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
    if (sigName.isEmpty) {
      throw new UnsupportedOperationException(s"not currently supported: $aggregateFunc.")
    }

    aggregateFunc match {
      case First(_, ignoreNulls) =>
        if (ignoreNulls) sigName = Some(ExpressionNames.FIRST_IGNORE_NULL)
      case Last(_, ignoreNulls) =>
        if (ignoreNulls) sigName = Some(ExpressionNames.LAST_IGNORE_NULL)
      case _ =>
    }

    // Use companion function for partial-merge aggregation functions on count distinct.
    val substraitAggFuncName = if (!forMergeCompanion) sigName.get else sigName.get + "_merge"

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitAggFuncName,
        getInputTypes(aggregateFunc, forMergeCompanion),
        FunctionConfig.REQ))
  }
}
