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
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.types.StringType

/**
 * Transformer for the normal binary expression
 */
class BinaryExpressionTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (ExpressionMappings.REPEAT.equalsIgnoreCase(original.prettyName) &&
      !BackendsApiManager.getBackendName.equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      val errMsg = String.format("%s or %s is not currently supported for non clickhouse backend.",
        original.getClass, original)
      logWarning(errMsg)
      throw new UnsupportedOperationException(errMsg)
    }
    val leftNode = left.doTransform(args)
    val rightNode = right.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LikeTransformer(substraitExprName: String,
                      left: ExpressionTransformer,
                      right: ExpressionTransformer,
                      original: Expression)
  extends ExpressionTransformer with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = left.doTransform(args)
    val rightNode = right.doTransform(args)
    val escapeCharNode = ExpressionBuilder.makeLiteral(
      original.asInstanceOf[Like].escapeChar.toString, StringType, false)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    // CH backend does not support escapeChar, so skip it here.
    val expressionNodes = if (BackendsApiManager.getBackendName
      .equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      Lists.newArrayList(leftNode, rightNode)
    } else {
      Lists.newArrayList(leftNode, rightNode, escapeCharNode)
    }
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object BinaryExpressionTransformer {

  def apply(substraitExprName: String,
            left: ExpressionTransformer,
            right: ExpressionTransformer,
            original: Expression): ExpressionTransformer = {
    original match {
      case _: Like => new LikeTransformer(substraitExprName, left, right, original)
      case _ => new BinaryExpressionTransformer(substraitExprName, left, right, original)
    }
  }
}
