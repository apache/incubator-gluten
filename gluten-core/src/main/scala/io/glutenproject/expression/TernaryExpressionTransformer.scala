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

class LocateTransformer(first: Expression, second: Expression,
                        third: Expression, original: Expression)
  extends StringLocate(first: Expression, second: Expression, second: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode =
      first.asInstanceOf[ExpressionTransformer].doTransform(args)
    val secondNode =
      second.asInstanceOf[ExpressionTransformer].doTransform(args)
    val thirdNode =
      third.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!firstNode.isInstanceOf[ExpressionNode] ||
      !secondNode.isInstanceOf[ExpressionNode] ||
      !thirdNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.LOCATE,
      Seq(first.dataType, second.dataType, third.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = TypeBuilder.makeI64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LPadTransformer(first: Expression, second: Expression,
                        third: Expression, original: Expression)
  extends StringLPad(first: Expression, second: Expression, second: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode =
      first.asInstanceOf[ExpressionTransformer].doTransform(args)
    val secondNode =
      second.asInstanceOf[ExpressionTransformer].doTransform(args)
    val thirdNode =
      third.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!firstNode.isInstanceOf[ExpressionNode] ||
      !secondNode.isInstanceOf[ExpressionNode] ||
      !thirdNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.LPAD,
      Seq(first.dataType, second.dataType, third.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    // val typeNode = TypeBuilder.makeI64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class RPadTransformer(first: Expression, second: Expression,
                      third: Expression, original: Expression)
  extends StringRPad(first: Expression, second: Expression, second: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode =
      first.asInstanceOf[ExpressionTransformer].doTransform(args)
    val secondNode =
      second.asInstanceOf[ExpressionTransformer].doTransform(args)
    val thirdNode =
      third.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!firstNode.isInstanceOf[ExpressionNode] ||
      !secondNode.isInstanceOf[ExpressionNode] ||
      !thirdNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.RPAD,
      Seq(first.dataType, second.dataType, third.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class RegExpExtractTransformer(subject: Expression, regexp: Expression,
                               index: Expression, original: Expression)
  extends RegExpExtract(subject: Expression, regexp: Expression, regexp: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode =
      subject.asInstanceOf[ExpressionTransformer].doTransform(args)
    val secondNode =
      regexp.asInstanceOf[ExpressionTransformer].doTransform(args)
    val thirdNode =
      index.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!firstNode.isInstanceOf[ExpressionNode] ||
      !secondNode.isInstanceOf[ExpressionNode] ||
      !thirdNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.REGEXP_EXTRACT,
      Seq(subject.dataType, regexp.dataType, index.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ReplaceTransformer(str: Expression, search: Expression, replace: Expression,
                       original: Expression)
  extends StringReplace(str: Expression, search: Expression, search: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val strNode =
      str.asInstanceOf[ExpressionTransformer].doTransform(args)
    val searchNode =
      search.asInstanceOf[ExpressionTransformer].doTransform(args)
    val replaceNode =
      replace.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!strNode.isInstanceOf[ExpressionNode] ||
      !searchNode.isInstanceOf[ExpressionNode] ||
      !replaceNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.REPLACE,
      Seq(str.dataType, search.dataType, replace.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(strNode, searchNode, replaceNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class SplitTransformer(str: Expression, delimiter: Expression, limit: Expression,
                       original: Expression)
  extends StringSplit(str: Expression, delimiter: Expression, limit: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: Split.")
  }
}

class SubStringTransformer(str: Expression, pos: Expression, len: Expression, original: Expression)
  extends Substring(str: Expression, pos: Expression, len: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val strNode =
      str.asInstanceOf[ExpressionTransformer].doTransform(args)
    val posNode =
      pos.asInstanceOf[ExpressionTransformer].doTransform(args)
    val lenNode =
      len.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!strNode.isInstanceOf[ExpressionNode] ||
      !posNode.isInstanceOf[ExpressionNode] ||
      !lenNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.SUBSTRING, Seq(str.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      strNode.asInstanceOf[ExpressionNode],
      posNode.asInstanceOf[ExpressionNode],
      lenNode.asInstanceOf[ExpressionNode])
    // Substring inherits NullIntolerant, the output is nullable
    val typeNode = TypeBuilder.makeString(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object TernaryExpressionTransformer {

  def create(first: Expression, second: Expression, third: Expression,
    original: Expression): Expression = original match {
      case _: StringLocate =>
        // locate() gets incorrect results, so fall back to Vanilla Spark
        throw new UnsupportedOperationException("Not supported: locate().")
      case _: StringSplit =>
        // split() gets incorrect results, so fall back to Vanilla Spark
        throw new UnsupportedOperationException("Not supported: locate().")
      case lpad: StringLPad =>
        new LPadTransformer(first, second, third, lpad)
      case rpad: StringRPad =>
        new RPadTransformer(first, second, third, rpad)
      case extract: RegExpExtract =>
        new RegExpExtractTransformer(first, second, third, extract)
      case replace: StringReplace =>
        new ReplaceTransformer(first, second, third, replace)
      case ss: Substring =>
        new SubStringTransformer(first, second, third, ss)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
