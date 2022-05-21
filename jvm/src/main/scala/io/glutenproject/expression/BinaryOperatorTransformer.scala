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
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

class AndTransformer(left: Expression, right: Expression, original: Expression)
    extends And(left: Expression, right: Expression)
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
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.AND, Seq(left.dataType, right.dataType)))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(leftNode.asInstanceOf[ExpressionNode])
    expressionNodes.add(rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class OrTransformer(left: Expression, right: Expression, original: Expression)
    extends Or(left: Expression, right: Expression)
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
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.OR, Seq(left.dataType, right.dataType)))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(leftNode.asInstanceOf[ExpressionNode])
    expressionNodes.add(rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class EqualToTransformer(left: Expression, right: Expression, original: Expression)
    extends EqualTo(left: Expression, right: Expression)
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
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.EQUAL, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class EqualNullTransformer(left: Expression, right: Expression, original: Expression)
    extends EqualNullSafe(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: EqualNull.")
  }
}

class LessThanTransformer(left: Expression, right: Expression, original: Expression)
    extends LessThan(left: Expression, right: Expression)
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
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LESS_THAN, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LessThanOrEqualTransformer(left: Expression, right: Expression, original: Expression)
    extends LessThanOrEqual(left: Expression, right: Expression)
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
        ConverterUtils.LESS_THAN_OR_EQUAL, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class GreaterThanTransformer(left: Expression, right: Expression, original: Expression)
    extends GreaterThan(left: Expression, right: Expression)
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
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.GREATER_THAN, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class GreaterThanOrEqualTransformer(left: Expression, right: Expression, original: Expression)
    extends GreaterThanOrEqual(left: Expression, right: Expression)
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
        ConverterUtils.GREATER_THAN_OR_EQUAL, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object BinaryOperatorTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression = {
    original match {
      case a: And =>
        new AndTransformer(left, right, a)
      case o: Or =>
        new OrTransformer(left, right, o)
      case e: EqualTo =>
        new EqualToTransformer(left, right, e)
      case e: EqualNullSafe =>
        new EqualNullTransformer(left, right, e)
      case l: LessThan =>
        new LessThanTransformer(left, right, l)
      case l: LessThanOrEqual =>
        new LessThanOrEqualTransformer(left, right, l)
      case g: GreaterThan =>
        new GreaterThanTransformer(left, right, g)
      case g: GreaterThanOrEqual =>
        new GreaterThanOrEqualTransformer(left, right, g)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
