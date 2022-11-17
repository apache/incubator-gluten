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
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import io.glutenproject.substrait.`type`.ListNode

class KnownFloatingPointNormalizedTransformer(
                                               child: ExpressionTransformer,
                                               original: KnownFloatingPointNormalized)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

class CastTransformer(child: ExpressionTransformer,
                      datatype: DataType,
                      timeZoneId: Option[String],
                      original: Expression)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

    val typeNode = ConverterUtils.getTypeNode(datatype, original.nullable)
    ExpressionBuilder.makeCast(typeNode, childNode, SQLConf.get.ansiEnabled)
  }
}

class NormalizeNaNAndZeroTransformer(
                                      child: ExpressionTransformer,
                                      original: NormalizeNaNAndZero)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

class ExplodeTransformer(substraitExprName: String, child: ExpressionTransformer, original: Explode)
    extends ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(substraitExprName, Seq(original.child.dataType)))

    val expressionNodes = Lists.newArrayList(childNode)

    val childTypeNode = ConverterUtils.getTypeNode(original.child.dataType, original.child.nullable)
    val typeNode = childTypeNode.asInstanceOf[ListNode].getNestedType()

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

/**
 * Transformer for the normal unary expression
 */
class UnaryExpressionTransformer(substraitExprName: String,
                                 child: ExpressionTransformer,
                                 original: Expression)
  extends ExpressionTransformer with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object UnaryExpressionTransformer {

  def apply(substraitExprName: String,
            child: ExpressionTransformer,
            original: Expression): ExpressionTransformer = {
    new UnaryExpressionTransformer(substraitExprName, child, original)
  }
}
