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
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

class InstrTransformer(str: Expression, substr: Expression, original: Expression)
  extends StringInstr(str: Expression, substr: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val strNode = str.asInstanceOf[ExpressionTransformer].doTransform(args)
    val substrNode = substr.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!strNode.isInstanceOf[ExpressionNode] || !substrNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, ConverterUtils.makeFuncName(
      ConverterUtils.INSTR, Seq(str.dataType, substr.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(strNode, substrNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ShiftLeftTransformer(left: Expression, right: Expression, original: Expression)
  extends ShiftLeft(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: ShiftLeft.")
  }
}

class ShiftRightTransformer(left: Expression, right: Expression, original: Expression)
  extends ShiftRight(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: ShiftRight.")
  }
}

class EndsWithTransformer(left: Expression, right: Expression, original: Expression)
  extends EndsWith(left: Expression, right: Expression)
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
      ConverterUtils.ENDS_WITH, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class StartsWithTransformer(left: Expression, right: Expression, original: Expression)
  extends StartsWith(left: Expression, right: Expression)
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
      ConverterUtils.STARTS_WITH, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LikeTransformer(left: Expression, right: Expression, original: Expression)
  extends Like(left: Expression, right: Expression)
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
      ConverterUtils.makeFuncName(ConverterUtils.LIKE, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ContainsTransformer(left: Expression, right: Expression, original: Expression)
  extends Contains(left: Expression, right: Expression)
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
      ConverterUtils.CONTAINS, Seq(left.dataType, right.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)


  }
}

class DateAddIntervalTransformer(start: Expression, interval: Expression, original: DateAddInterval)
  extends DateAddInterval(start, interval, original.timeZoneId, original.ansiEnabled)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: DateAddInterval.")
  }
}

class PowTransformer(left: Expression, right: Expression, original: Expression)
  extends Pow(left: Expression, right: Expression)
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
      ConverterUtils.POWER, Seq(left.dataType, right.dataType)))

    val expressionNodes = Lists.newArrayList(
      leftNode.asInstanceOf[ExpressionNode],
      rightNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object BinaryExpressionTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression =
    original match {
      case instr: StringInstr =>
        new InstrTransformer(left, right, instr)
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
      case s: DateAddInterval =>
        new DateAddIntervalTransformer(left, right, s)
      case s: DateDiff =>
        new DateDiffTransformer(left, right)
      case a: UnixTimestamp =>
        new UnixTimestampTransformer(left, right)
      case p: Pow =>
        new PowTransformer(left, right, p)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
