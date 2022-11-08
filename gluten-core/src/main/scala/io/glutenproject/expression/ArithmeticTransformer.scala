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

import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * A version of add that supports columnar processing for longs.
 */
class AddTransformer(left: Expression, right: Expression, original: Expression)
  extends Add(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val rightNode =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!leftNode.isInstanceOf[ExpressionNode] ||
      !rightNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.ADD, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(left.dataType, nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class SubtractTransformer(left: Expression, right: Expression, original: Expression)
  extends Subtract(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val rightNode =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!leftNode.isInstanceOf[ExpressionNode] ||
      !rightNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.SUBTRACT, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(left.dataType, nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class MultiplyTransformer(left: Expression, right: Expression, original: Expression)
  extends Multiply(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val rightNode =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!leftNode.isInstanceOf[ExpressionNode] ||
      !rightNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.MULTIPLY, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(left.dataType, nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class DivideTransformer(left: Expression, right: Expression,
                        original: Expression, resType: DecimalType = null)
  extends Divide(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val rightNode =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!leftNode.isInstanceOf[ExpressionNode] ||
      !rightNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.DIVIDE, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(left.dataType, nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class BitwiseAndTransformer(left: Expression, right: Expression, original: Expression)
  extends BitwiseAnd(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: BitwiseAnd.")
  }
}

class BitwiseOrTransformer(left: Expression, right: Expression, original: Expression)
  extends BitwiseOr(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: BitwiseOr.")
  }
}

class BitwiseXorTransformer(left: Expression, right: Expression, original: Expression)
  extends BitwiseXor(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: BitwiseXor.")
  }
}

class PmodTransformer(left: Expression, right: Expression, original: Expression)
  extends Pmod(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val rightNode =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!leftNode.isInstanceOf[ExpressionNode] ||
      !rightNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, ConverterUtils.makeFuncName(
      ConverterUtils.PMOD, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object BinaryArithmeticTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression = {
    original match {
      case a: Add =>
        new AddTransformer(left, right, a)
      case s: Subtract =>
        new SubtractTransformer(left, right, s)
      case m: Multiply =>
        new MultiplyTransformer(left, right, m)
      case d: Divide =>
        new DivideTransformer(left, right, d)
      case a: BitwiseAnd =>
        new BitwiseAndTransformer(left, right, a)
      case o: BitwiseOr =>
        new BitwiseOrTransformer(left, right, o)
      case x: BitwiseXor =>
        new BitwiseXorTransformer(left, right, x)
      case p: Pmod =>
        new PmodTransformer(left, right, p)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  def createDivide(left: Expression, right: Expression,
                   original: Expression, resType: DecimalType): Expression = {
    original match {
      case d: Divide =>
        new DivideTransformer(left, right, d, resType)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
