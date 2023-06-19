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
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, IfThenNode, IntLiteralNode, StringLiteralNode}
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNull, StringLocate, StringTranslate}
import org.apache.spark.sql.types.IntegerType
import io.glutenproject.substrait.`type`.TypeBuilder

case class StringTranslateTransformer(
  substraitExprName: String,
  srcExpr: ExpressionTransformer,
  matchingExpr: ExpressionTransformer,
  replaceExpr: ExpressionTransformer,
  original: StringTranslate) extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In CH, translateUTF8 requires matchingExpr and replaceExpr argument have the same length
    var matchingNode = matchingExpr.doTransform(args)
    var replaceNode = replaceExpr.doTransform(args)
    if (!matchingNode.isInstanceOf[StringLiteralNode] ||
      !replaceNode.isInstanceOf[StringLiteralNode]) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

    var matchingLiteral = matchingNode.asInstanceOf[StringLiteralNode].getValue()
    var replaceLiteral = replaceNode.asInstanceOf[StringLiteralNode].getValue()
    if (matchingLiteral.length() != replaceLiteral.length()) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

    TernaryExpressionTransformer(substraitExprName, srcExpr, matchingExpr, replaceExpr, original)
      .doTransform(args)
  }
}

case class StringLocateTransformer(
  substraitExprName: String,
  substrExpr: ExpressionTransformer,
  strExpr: ExpressionTransformer,
  startExpr: ExpressionTransformer,
  original: StringLocate) extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (BackendsApiManager.chBackend) {

      val substrNode = substrExpr.doTransform(args)
      val strNode = strExpr.doTransform(args)
      val startNode = startExpr.doTransform(args)

      // Special Case
      // In Spark, return 0 when start_pos is null
      // but when start_pos is not null, return null if either str or substr is null
      // so we need convert it to if(isnull(start_pos), 0, position(substr, str, start_pos)
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
      val locateFuncName = ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT)
      val locateFuncId = ExpressionBuilder.newScalarFunction(functionMap, locateFuncName)
      val exprNodes = Lists.newArrayList(substrNode, strNode, startNode)
      val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
      val locateFuncNode = ExpressionBuilder.makeScalarFunction(locateFuncId, exprNodes, typeNode)

      // isnull(start_pos)
      val isnullFuncName = ConverterUtils.makeFuncName(
        ExpressionNames.IS_NULL, Seq(IntegerType), FunctionConfig.OPT)
      val isnullFuncId = ExpressionBuilder.newScalarFunction(functionMap, isnullFuncName)
      val isnullNode = ExpressionBuilder.makeScalarFunction(
        isnullFuncId, Lists.newArrayList(startNode), TypeBuilder.makeBoolean(false))

      new IfThenNode(
        Lists.newArrayList(isnullNode),
        Lists.newArrayList(new IntLiteralNode(0)),
        locateFuncNode)
    } else {
      TernaryExpressionTransformer(
        substraitExprName, substrExpr, strExpr, startExpr, original).doTransform(args)
    }
  }
}


/**
 * Transformer for the normal ternary expression
 */
class TernaryExpressionTransformer(
    substraitExprName: String,
    first: ExpressionTransformer,
    second: ExpressionTransformer,
    third: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode = first.doTransform(args)
    val secondNode = second.doTransform(args)
    val thirdNode = third.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object TernaryExpressionTransformer {

  def apply(
      substraitExprName: String,
      first: ExpressionTransformer,
      second: ExpressionTransformer,
      third: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    new TernaryExpressionTransformer(
      substraitExprName,
      first,
      second,
      third,
      original
    )
  }
}
