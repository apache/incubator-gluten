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
import java.util.ArrayList

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import io.glutenproject.substrait.`type`.ListNode
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.GlutenConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.`type`.MapNode
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{BooleanLiteralNode, ExpressionBuilder, ExpressionNode}

class KnownFloatingPointNormalizedTransformer(
    child: ExpressionTransformer,
    original: KnownFloatingPointNormalized)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

class CastTransformer(
    child: ExpressionTransformer,
    datatype: DataType,
    timeZoneId: Option[String],
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val typeNode = ConverterUtils.getTypeNode(datatype, original.nullable)
    ExpressionBuilder.makeCast(typeNode, child.doTransform(args), SQLConf.get.ansiEnabled)
  }
}

class NormalizeNaNAndZeroTransformer(child: ExpressionTransformer, original: NormalizeNaNAndZero)
  extends ExpressionTransformer
  with Logging {

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
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, Seq(original.child.dataType)))

    val expressionNodes = Lists.newArrayList(childNode)
    val childTypeNode = ConverterUtils.getTypeNode(original.child.dataType, original.child.nullable)
    childTypeNode match {
      case l: ListNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, l.getNestedType())
      case m: MapNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, m.getNestedType())
      case _ =>
        throw new UnsupportedOperationException(s"explode(${childTypeNode}) not supported yet.")
    }
  }
}

class PromotePrecisionTransformer(child: ExpressionTransformer, original: PromotePrecision)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

class CheckOverflowTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: CheckOverflow)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.dataType, BooleanType),
        FunctionConfig.OPT))

    val expressionNodes =
      Lists.newArrayList(childNode, new BooleanLiteralNode(original.nullOnOverflow))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class Md5Transformer(substraitExprName: String, child: ExpressionTransformer, original: Md5)
  extends ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (BackendsApiManager.getBackendName.equalsIgnoreCase(
      GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      // In Spark: md5(str)
      // In CH: lower(hex(md5(str)))
      // So we need to wrap md5(str) with lower and hex in substrait plan for clickhouse backend.
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

      val md5FuncId = ExpressionBuilder.newScalarFunction( functionMap,
        ConverterUtils.makeFuncName(substraitExprName, Seq(original.dataType), FunctionConfig.OPT))
      val md5ChildNode = child.doTransform(args)
      val md5ExprNodes = Lists.newArrayList(md5ChildNode)
      /// In CH, the output type of md5 is FixedString(16)
      val md5TypeNode = TypeBuilder.makeFixedChar(original.nullable, 16)
      val md5FuncNode = ExpressionBuilder.makeScalarFunction(md5FuncId, md5ExprNodes, md5TypeNode)

      // wrap in hex: hex(md5(str))
      val hexFuncId = ExpressionBuilder.newScalarFunction(
        functionMap, ConverterUtils.makeFuncName("hex", Seq(CharType(16)), FunctionConfig.OPT))
      val hexExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(md5FuncNode)
      val hexTypeNode = TypeBuilder.makeString(original.nullable)
      val hexFuncNode = ExpressionBuilder.makeScalarFunction(hexFuncId, hexExprNodes, hexTypeNode)

      // wrap in lower: lower(hex(md5(str)))
      val lowerFuncId = ExpressionBuilder.newScalarFunction(
        functionMap, ConverterUtils.makeFuncName("lower", Seq(StringType), FunctionConfig.OPT))
      val lowerExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(hexFuncNode)
      val lowerTypeNode = TypeBuilder.makeString(original.nullable)
      ExpressionBuilder.makeScalarFunction(lowerFuncId, lowerExprNodes, lowerTypeNode)
    } else {
      UnaryExpressionTransformer(substraitExprName, child, original).doTransform(args)
    }
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
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, original.map(_.dataType), FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object UnaryExpressionTransformer {

  def apply(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    original match {
      case c: CheckOverflow =>
        new CheckOverflowTransformer(substraitExprName, child, c)
      case p: PromotePrecision =>
        new PromotePrecisionTransformer(child, p)
      case extract if extract.isInstanceOf[GetDateField] || extract.isInstanceOf[GetTimeField] =>
        new ExtractDateTransformer(substraitExprName, child, extract)
      case _ =>
        new UnaryExpressionTransformer(substraitExprName, child, original)
    }
  }
}
