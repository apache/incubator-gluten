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
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import java.util

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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

  override protected def addFunctionNode(
    args: java.lang.Object,
    aggregateFunction: AggregateFunction,
    childrenNodeList: util.ArrayList[ExpressionNode],
    aggregateMode: AggregateMode,
    aggregateNodeList: util.ArrayList[AggregateFunctionNode]): Unit = {
    // Will use sum and count to replace avg.
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    aggregateFunction match {
      case avg: Average =>
        aggregateMode match {
          case Partial =>
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              AggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
            aggregateNodeList.add(aggFunctionNode)
          case Final =>
            assert(childrenNodeList.size() == 2, "Sum and count are expected.")
            val sumChildren = new util.ArrayList[ExpressionNode]()
            sumChildren.add(childrenNodeList.get(0))
            val sumNode = ExpressionBuilder.makeAggregateFunction(
              ExpressionBuilder.newScalarFunction(
                functionMap,
                ConverterUtils.makeFuncName(
                  ConverterUtils.SUM,
                  Seq(aggregateFunction.inputAggBufferAttributes.head.dataType),
                  FunctionConfig.OPT)),
              sumChildren,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(
                aggregateFunction.inputAggBufferAttributes.head.dataType,
                aggregateFunction.inputAggBufferAttributes.head.nullable))
            aggregateNodeList.add(sumNode)

            val countChildren = new util.ArrayList[ExpressionNode]()
            countChildren.add(childrenNodeList.get(1))
            val countNode = ExpressionBuilder.makeAggregateFunction(
              ExpressionBuilder.newScalarFunction(
                functionMap,
                ConverterUtils.makeFuncName(
                  ConverterUtils.COUNT,
                  Seq(aggregateFunction.inputAggBufferAttributes(1).dataType),
                  FunctionConfig.OPT)),
              countChildren,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(
                aggregateFunction.inputAggBufferAttributes(1).dataType,
                aggregateFunction.inputAggBufferAttributes(1).nullable))
            aggregateNodeList.add(countNode)
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

  private def extraProjectionNeeded(): Boolean = {
    for (aggregateExpression <- aggregateExpressions) {
      aggregateExpression.aggregateFunction match {
        case Average(_) =>
          aggregateExpression.mode match {
            case Final =>
              return true
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  private def applyExtraProjectForFinalAvg(args: java.lang.Object, aggRel: RelNode,
                                           validation: Boolean = false): RelNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val exprNodeList = new util.ArrayList[ExpressionNode]()
    var projectInputAttributes: Seq[Attribute] = Seq()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      exprNodeList.add(ExpressionBuilder.makeSelection(colIdx))
      projectInputAttributes = projectInputAttributes :+ groupingExpressions(colIdx).toAttribute
      colIdx += 1
    }

    aggregateExpressions.foreach(aggregateExpression => {
      aggregateExpression.aggregateFunction match {
        case avg: Average =>
          aggregateExpression.mode match {
            case Final =>
              // sum
              val leftNode = ExpressionBuilder.makeSelection(colIdx)
              colIdx += 1

              // cast count from bigint to double
              val rightNode = ExpressionBuilder.makeCast(
                ConverterUtils.getTypeNode(DoubleType, nullable = true),
                ExpressionBuilder.makeSelection(colIdx))
              colIdx += 1
              val functionId = ExpressionBuilder
                .newScalarFunction(functionMap, ConverterUtils.makeFuncName(
                  ConverterUtils.DIVIDE, Seq(avg.child.dataType, DoubleType), FunctionConfig.OPT))
              val expressionNodes = Lists.newArrayList(
                leftNode.asInstanceOf[ExpressionNode], rightNode.asInstanceOf[ExpressionNode])
              projectInputAttributes = projectInputAttributes ++ avg.inputAggBufferAttributes

              exprNodeList.add(ExpressionBuilder.makeScalarFunction(
                functionId,
                expressionNodes,
                ConverterUtils.getTypeNode(DoubleType, nullable = true)))
            case _ =>
          }
        case other =>
          exprNodeList.add(ExpressionBuilder.makeSelection(colIdx))
          projectInputAttributes = projectInputAttributes ++ other.inputAggBufferAttributes
          colIdx += 1
      }
    })

    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, exprNodeList)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- projectInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(aggRel, exprNodeList, extensionNode)
    }
  }

  override protected def getAggRel(args: java.lang.Object,
                                   input: RelNode = null,
                                   validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    var aggRel = if (needsPreProjection) {
      getAggRelWithPreProjection(args, originalInputAttributes, input, validation)
    } else {
      getAggRelWithoutPreProjection(args, originalInputAttributes, input, validation)
    }
    // Use sum and count to calculate the final avg. To get correct result, a projection is applied
    // for sum / count.
    aggRel = if (extraProjectionNeeded()) {
      applyExtraProjectForFinalAvg(args, aggRel, validation)
    } else {
      aggRel
    }
    applyPostProjection(args, aggRel, validation)
  }
}
