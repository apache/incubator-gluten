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
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import scala.collection.mutable.ListBuffer

case class VeloxHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends GlutenHashAggregateExecTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {
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

    aggregateFunction match {
      case hllAdapter: HLLVeloxAdapter =>
        aggregateMode match {
          case Partial =>
            // For Partial mode output type is binary.
            val partialNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(
                hllAdapter.inputAggBufferAttributes.head.dataType,
                hllAdapter.inputAggBufferAttributes.head.nullable))
            aggregateNodeList.add(partialNode)
          case Final =>
            // For Final mode output type is long.
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
            aggregateNodeList.add(aggFunctionNode)
          case other =>
            throw new UnsupportedOperationException(s"$other is not supported.")
        }
      case _ =>
        super.addFunctionNode(
          args, aggregateFunction, childrenNodeList, aggregateMode, aggregateNodeList)
    }
  }

  override protected def getAttrForAggregateExpr(
    exp: AggregateExpression,
    aggregateAttributeList: Seq[Attribute],
    aggregateAttr: ListBuffer[Attribute],
    index: Int): Int = {
    var resIndex = index
    val mode = exp.mode
    val aggregateFunc = exp.aggregateFunction
    aggregateFunc match {
      case hllAdapter: HLLVeloxAdapter =>
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

  override protected def withNewChildInternal(newChild: SparkPlan)
    : VeloxHashAggregateExecTransformer = {
    copy(child = newChild)
  }
}
