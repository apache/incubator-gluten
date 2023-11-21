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

import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.VeloxIntermediateData

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.google.protobuf.Any

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList}

import scala.collection.JavaConverters._

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

  override protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case _: HLLAdapter =>
        mode match {
          case Partial | Final => true
          case _ => false
        }
      case _ =>
        super.checkAggFuncModeSupport(aggFunc, mode)
    }
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
  private def extractStructNeeded(): Boolean = {
    aggregateExpressions.exists {
      expr =>
        expr.aggregateFunction match {
          case aggFunc if aggFunc.aggBufferAttributes.size > 1 =>
            expr.mode match {
              case Partial | PartialMerge => true
              case _ => false
            }
          case _ => false
        }
    }
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
    val expressionNodes = new JArrayList[ExpressionNode]()
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
      val aggFunc = expr.aggregateFunction
      expr.aggregateFunction match {
        case _ @VeloxIntermediateData.Type(veloxTypes: Seq[DataType]) =>
          val (sparkOrders, sparkTypes) =
            aggFunc.aggBufferAttributes.map(attr => (attr.name, attr.dataType)).unzip
          val veloxOrders = VeloxIntermediateData.veloxIntermediateDataOrder(aggFunc)
          val adjustedOrders = sparkOrders.map(veloxOrders.indexOf(_))
          sparkTypes.zipWithIndex.foreach {
            case (sparkType, idx) =>
              val veloxType = veloxTypes(adjustedOrders(idx))
              if (veloxType != sparkType) {
                // Velox and Spark have different type, adding a cast expression
                expressionNodes.add(
                  ExpressionBuilder
                    .makeCast(
                      ConverterUtils.getTypeNode(sparkType, nullable = false),
                      ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)),
                      SQLConf.get.ansiEnabled))
              } else {
                // Velox and Spark have the same type
                expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)))
              }
          }
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

  override protected def modeToKeyWord(aggregateMode: AggregateMode): String = {
    super.modeToKeyWord(if (mixedPartialAndMerge) Partial else aggregateMode)
  }

  // Create aggregate function node and add to list.
  override protected def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: JList[AggregateFunctionNode]): Unit = {
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
            VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction)
          )
          aggregateNodeList.add(partialNode)
        case PartialMerge =>
          val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder
              .create(args, aggregateFunction, mixedPartialAndMerge),
            childrenNodeList,
            modeKeyWord,
            VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction)
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
          _: CovPopulation | _: CovSample | _: First | _: Last | _: MaxMinBy =>
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
  def getPartialAggOutTypes: JList[TypeNode] = {
    val typeNodeList = new JArrayList[TypeNode]()
    groupingExpressions.foreach(
      expression => {
        typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
      })

    aggregateExpressions.foreach(
      expression => {
        val aggregateFunction = expression.aggregateFunction
        aggregateFunction match {
          case _: Average | _: First | _: Last | _: StddevSamp | _: StddevPop | _: VarianceSamp |
              _: VariancePop | _: Corr | _: CovPopulation | _: CovSample | _: MaxMinBy =>
            expression.mode match {
              case Partial | PartialMerge =>
                typeNodeList.add(VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction))
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
                typeNodeList.add(VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction))
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
    aggregateExpressions.exists {
      aggExpr =>
        aggExpr.mode match {
          case PartialMerge | Final =>
            aggExpr.aggregateFunction.inputAggBufferAttributes.size > 1
          case _ => false
        }
    }
  }

  // Return a scalar function node representing row construct function in Velox.
  private def getRowConstructNode(
      args: java.lang.Object,
      childNodes: JList[ExpressionNode],
      rowConstructAttributes: Seq[Attribute],
      withNull: Boolean = true): ScalarFunctionNode = {
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName = ConverterUtils.makeFuncName(
      if (withNull) "row_constructor_with_null" else "row_constructor",
      rowConstructAttributes.map(attr => attr.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = rowConstructAttributes
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      .asJava

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
    val exprNodes = new JArrayList[ExpressionNode]()
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
          val childNodes = new JArrayList[ExpressionNode](
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
              val childNodes =
                functionInputAttributes.toList
                  .map(
                    ExpressionConverter
                      .replaceWithExpressionTransformer(_, originalInputAttributes)
                      .doTransform(args))
                  .asJava
              exprNodes.add(
                getRowConstructNode(
                  args,
                  childNodes,
                  functionInputAttributes,
                  withNull = !avg.dataType.isInstanceOf[DecimalType]))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _: First | _: Last | _: MaxMinBy =>
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              assert(
                functionInputAttributes.size == 2,
                s"${aggregateExpression.mode.toString} of " +
                  s"${aggregateFunction.getClass.toString} expects two input attributes.")
              // Use a Velox function to combine the intermediate columns into struct.
              val childNodes = functionInputAttributes.toList
                .map(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(_, originalInputAttributes)
                    .doTransform(args)
                )
                .asJava
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
              val childNodes = functionInputAttributes.toList.map {
                attr =>
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
              }.asJava
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
              val childNodes = new JArrayList[ExpressionNode]()
              // Velox's Corr order is [ck, n, xMk, yMk, xAvg, yAvg]
              // Spark's Corr order is [n, xAvg, yAvg, ck, xMk, yMk]
              val sparkCorrOutputAttr = aggregateFunction.inputAggBufferAttributes.map(_.name)
              val veloxInputOrder =
                VeloxIntermediateData.veloxCorrIntermediateDataOrder.map(
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
              val childNodes = new JArrayList[ExpressionNode]()
              // Velox's Covar order is [ck, n, xAvg, yAvg]
              // Spark's Covar order is [n, xAvg, yAvg, ck]
              val sparkCorrOutputAttr = aggregateFunction.inputAggBufferAttributes.map(_.name)
              val veloxInputOrder =
                VeloxIntermediateData.veloxCovarIntermediateDataOrder.map(
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
              val childNodes = functionInputAttributes.toList
                .map(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(_, originalInputAttributes)
                    .doTransform(args)
                )
                .asJava
              exprNodes.add(
                getRowConstructNode(args, childNodes, functionInputAttributes, withNull = false))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          if (functionInputAttributes.size != 1) {
            throw new UnsupportedOperationException("Only one input attribute is expected.")
          }
          val childNodes = functionInputAttributes.toList
            .map(
              ExpressionConverter
                .replaceWithExpressionTransformer(_, originalInputAttributes)
                .doTransform(args)
            )
            .asJava
          exprNodes.addAll(childNodes)
      }
    }

    // Create a project rel.
    val emitStartIndex = originalInputAttributes.size
    val projectRel = if (!validation) {
      RelBuilder.makeProjectRel(inputRel, exprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
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
    val groupingList = new JArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach(
      _ => {
        groupingList.add(ExpressionBuilder.makeSelection(colIdx))
        colIdx += 1
      })

    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          throw new UnsupportedOperationException("Filter in final aggregation is not supported.")
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }

        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = new JArrayList[ExpressionNode]()
        aggregateFunc match {
          case _: Average | _: First | _: Last | _: StddevSamp | _: StddevPop | _: VarianceSamp |
              _: VariancePop | _: Corr | _: CovPopulation | _: CovSample | _: MaxMinBy
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
    val partialMergeExists = aggregateExpressions.exists(_.mode == PartialMerge)
    val partialExists = aggregateExpressions.exists(_.mode == Partial)
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
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]

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
        VeloxIntermediateData.getInputTypes(aggregateFunc, forMergeCompanion),
        FunctionConfig.REQ))
  }
}
