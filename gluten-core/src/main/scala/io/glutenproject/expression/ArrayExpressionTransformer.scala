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

package io.glutenproject.expression

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.expression.ByteLiteralNode
import io.glutenproject.substrait.expression.ShortLiteralNode
import io.glutenproject.substrait.expression.IntLiteralNode
import io.glutenproject.substrait.expression.LongLiteralNode
import io.glutenproject.substrait.expression.IfThenNode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

class CreateArrayTransformer(substraitExprName: String, children: Seq[ExpressionTransformer],
  useStringTypeWhenEmpty: Boolean,
  original: CreateArray)
    extends ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // If children is empty,
    // transformation is only supported when useStringTypeWhenEmpty is false
    // because ClickHouse and Velox currently doesn't support this config.
    if (useStringTypeWhenEmpty && children.isEmpty) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

    val childNodes = new java.util.ArrayList[ExpressionNode]()
    children.foreach(child => {
      val childNode = child.doTransform(args)
      childNodes.add(childNode)
    })

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(substraitExprName,
      original.children.map(_.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

class GetArrayItemTransformer(substraitExprName: String, left: ExpressionTransformer,
  right: ExpressionTransformer, failOnError: Boolean, original: GetArrayItem)
    extends ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Ignore failOnError for clickhouse backend
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val leftNode = left.doTransform(args)
    var rightNode = right.doTransform(args)

    // In Spark, the index of getarrayitem starts from 0
    // But in CH and velox, the index of arrayElement starts from 1, besides index argument must
    // So we need to do transform: rightNode = add(rightNode, 1)
    val addFunctionName = ConverterUtils.makeFuncName(ExpressionNames.ADD,
      Seq(IntegerType, original.right.dataType), FunctionConfig.OPT)
    val addFunctionId = ExpressionBuilder.newScalarFunction(functionMap, addFunctionName)
    val literalNode = ExpressionBuilder.makeLiteral(1.toInt, IntegerType, false)
    rightNode = ExpressionBuilder.makeScalarFunction(addFunctionId,
      Lists.newArrayList(literalNode, rightNode),
      ConverterUtils.getTypeNode(original.right.dataType, original.right.nullable))

    val functionName = ConverterUtils.makeFuncName(substraitExprName,
      Seq(original.left.dataType, original.right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val exprNodes = Lists.newArrayList(leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, exprNodes, typeNode)
  }
}

class SequenceTransformer(substraitExprName: String, start: ExpressionTransformer,
  stop: ExpressionTransformer, stepOpt: Option[ExpressionTransformer], original: Sequence)
    extends ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In Spark: sequence(start, stop, step)
    // In CH: if ((end - start) % step = 0, range(start, end + step, step), range(start, end, step))

    val getLiteralNode = (n: Long) => original.start.dataType match {
      case ByteType => new ByteLiteralNode(n.toByte)
      case ShortType => new ShortLiteralNode(n.toShort)
      case IntegerType => new IntLiteralNode(n.toInt)
      case LongType => new LongLiteralNode(n)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported type: ${original.start.dataType}")
    }

    val zeroNode = getLiteralNode(0)
    val oneNode = getLiteralNode(1)
    val minusOneNode = getLiteralNode(-1)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val lessThanFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.LESS_THAN,
        Seq(original.start.dataType, original.stop.dataType),
        FunctionConfig.OPT))
    val rangeFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName("range",
        Seq(original.start.dataType, original.stop.dataType, original.start.dataType),
        FunctionConfig.OPT))
    val addFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.ADD,
        Seq(original.stop.dataType, original.start.dataType), FunctionConfig.OPT))
    val substractFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.SUBTRACT,
        Seq(original.stop.dataType, original.start.dataType),
        FunctionConfig.OPT))
    val remainderFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.REMAINDER,
        Seq(original.stop.dataType, original.start.dataType),
        FunctionConfig.OPT))
    val equalFuncId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.EQUAL,
        Seq(original.start.dataType, original.start.dataType),
        FunctionConfig.OPT))

    val startNode = start.doTransform(args)
    val stopNode = stop.doTransform(args)

    // default step is 1 if start <= stop, otherwise -1
    val lessThanFuncNode = ExpressionBuilder.makeScalarFunction(lessThanFuncId,
      Lists.newArrayList(startNode, stopNode),
      ConverterUtils.getTypeNode(BooleanType, true))
    val stepNode: ExpressionNode = stepOpt match {
      case Some(step) => step.doTransform(args)
      // step = if(start <= stop, 1, -1)
      case None => new IfThenNode(Lists.newArrayList(lessThanFuncNode),
        Lists.newArrayList(oneNode), minusOneNode)
    }

    // end + step
    val addFuncNode = ExpressionBuilder.makeScalarFunction(addFuncId,
      Lists.newArrayList(stopNode, stepNode),
      ConverterUtils.getTypeNode(original.stop.dataType, true))

    // range(start, end + step, step)
    val leftRangeFuncNode = ExpressionBuilder.makeScalarFunction(rangeFuncId,
      Lists.newArrayList(startNode, addFuncNode, stepNode),
      ConverterUtils.getTypeNode(original.dataType, true))

    // range(start, end, step)
    val rightRangeFuncNode = ExpressionBuilder.makeScalarFunction(rangeFuncId,
      Lists.newArrayList(startNode, stopNode, stepNode),
      ConverterUtils.getTypeNode(original.dataType, true))

    // end - start
    val substractFuncNode = ExpressionBuilder.makeScalarFunction(substractFuncId,
      Lists.newArrayList(stopNode, startNode),
      ConverterUtils.getTypeNode(original.stop.dataType, true))

    // (end - start) % step
    val remainderFuncNode = ExpressionBuilder.makeScalarFunction(remainderFuncId,
      Lists.newArrayList(substractFuncNode, stepNode),
      ConverterUtils.getTypeNode(original.start.dataType, true))

    // (end - start) % step = 0
    val equalFuncNode = ExpressionBuilder.makeScalarFunction(equalFuncId,
      Lists.newArrayList(remainderFuncNode, zeroNode),
      ConverterUtils.getTypeNode(BooleanType, true))

    // if ((end - start) % step = 0, range(start, end + step, step), range(start, end, step))
    new IfThenNode(Lists.newArrayList(equalFuncNode),
      Lists.newArrayList(leftRangeFuncNode), rightRangeFuncNode)
  }
}
