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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.RegExpReplace
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.GlutenConfig
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.IntegerType
import io.glutenproject.substrait.expression.IntLiteralNode

class RegExpReplaceTransformer(substraitExprName: String, subject: ExpressionTransformer,
  regexp: ExpressionTransformer, rep: ExpressionTransformer, pos: ExpressionTransformer,
  original: RegExpReplace)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (BackendsApiManager.getBackendName.equalsIgnoreCase(GlutenConfig.GLUTEN_VELOX_BACKEND)) {
      QuaternaryExpressionTransformer(substraitExprName, subject, regexp, rep, pos, original)
        .doTransform(args)
    }

    // In CH: replaceRegexpAll(subject, regexp, rep), which is equivalent
    // In Spark: regexp_replace(subject, regexp, rep, pos=1)
    val posNode = pos.doTransform(args)
    if (!posNode.isInstanceOf[IntLiteralNode] ||
      posNode.asInstanceOf[IntLiteralNode].getValue() != 1) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

    TernaryExpressionTransformer(substraitExprName, subject, regexp, rep, original)
      .doTransform(args)
  }
}

/**
 * Transformer for the normal quaternary expression
 */
class QuaternaryExpressionTransformer(
    substraitExprName: String,
    first: ExpressionTransformer,
    second: ExpressionTransformer,
    third: ExpressionTransformer,
    forth: ExpressionTransformer,
    original: Expression)
    extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode = first.doTransform(args)
    val secondNode = second.doTransform(args)
    val thirdNode = third.doTransform(args)
    val forthNode = forth.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode, forthNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object QuaternaryExpressionTransformer {

  def apply(
      substraitExprName: String,
      first: ExpressionTransformer,
      second: ExpressionTransformer,
      third: ExpressionTransformer,
      forth: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    new QuaternaryExpressionTransformer(substraitExprName, first, second, third, forth, original)
  }
}
