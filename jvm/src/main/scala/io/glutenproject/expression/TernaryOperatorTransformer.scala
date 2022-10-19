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

object TernaryOperatorTransformer {

  def create(str: Expression, pos: Expression, len: Expression, original: Expression): Expression =
    original match {
      case split: StringSplit =>
        new SplitTransformer(str, pos, len, split)
      case replace: StringReplace =>
        new ReplaceTransformer(str, pos, len, replace)
      case ss: Substring =>
        new SubStringTransformer(str, pos, len, ss)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
