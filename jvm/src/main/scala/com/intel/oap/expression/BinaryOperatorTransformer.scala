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

package com.intel.oap.expression

import com.google.common.collect.Lists
import com.intel.oap.substrait.`type`.TypeBuiler
import com.intel.oap.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

/**
 * A version of add that supports columnar processing for longs.
 */
class AndTransformer(left: Expression, right: Expression, original: Expression)
    extends And(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.AND, Seq(left.dataType, right.dataType)))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(left_node.asInstanceOf[ExpressionNode])
    expressionNodes.add(right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class OrTransformer(left: Expression, right: Expression, original: Expression)
    extends Or(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.OR, Seq(left.dataType, right.dataType)))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(left_node.asInstanceOf[ExpressionNode])
    expressionNodes.add(right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class EndsWithTransformer(left: Expression, right: Expression, original: Expression)
    extends EndsWith(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class StartsWithTransformer(left: Expression, right: Expression, original: Expression)
    extends StartsWith(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class LikeTransformer(left: Expression, right: Expression, original: Expression)
    extends Like(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LIKE, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class ContainsTransformer(left: Expression, right: Expression, original: Expression)
    extends Contains(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class EqualToTransformer(left: Expression, right: Expression, original: Expression)
    extends EqualTo(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.EQUAL, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class EqualNullTransformer(left: Expression, right: Expression, original: Expression)
    extends EqualNullSafe(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class LessThanTransformer(left: Expression, right: Expression, original: Expression)
    extends LessThan(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LESS_THAN, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class LessThanOrEqualTransformer(left: Expression, right: Expression, original: Expression)
    extends LessThanOrEqual(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, ConverterUtils.makeFuncName(
        ConverterUtils.LESS_THAN_OR_EQUAL, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class GreaterThanTransformer(left: Expression, right: Expression, original: Expression)
    extends GreaterThan(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.GREATER_THAN, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class GreaterThanOrEqualTransformer(left: Expression, right: Expression, original: Expression)
    extends GreaterThanOrEqual(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, ConverterUtils.makeFuncName(
        ConverterUtils.GREATER_THAN_OR_EQUAL, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class ShiftLeftTransformer(left: Expression, right: Expression, original: Expression)
    extends ShiftLeft(left: Expression, right: Expression)
        with ExpressionTransformer
        with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class ShiftRightTransformer(left: Expression, right: Expression, original: Expression)
    extends ShiftRight(left: Expression, right: Expression)
        with ExpressionTransformer
        with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
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
      case e: EndsWith =>
        new EndsWithTransformer(left, right, e)
      case s: StartsWith =>
        new StartsWithTransformer(left, right, s)
      case c: Contains =>
        new ContainsTransformer(left, right, c)
      case l: Like =>
        new LikeTransformer(left, right, l)
      case s: ShiftLeft =>
        new ShiftLeftTransformer(left, right, s)
      case s: ShiftRight =>
        new ShiftRightTransformer(left, right, s)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
