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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.ListNode
import io.glutenproject.substrait.`type`.MapNode
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{BooleanLiteralNode, ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

import java.util.ArrayList

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
        throw new UnsupportedOperationException(s"explode($childTypeNode) not supported yet.")
    }
  }
}

class Sha1Transformer(substraitExprName: String, child: ExpressionTransformer, original: Sha1)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    UnaryExpressionTransformer(substraitExprName, child, original).doTransform(args)
  }
}

class PosExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: PosExplode,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    // sequence(1, size(array_or_map))
    val startExpr = new Literal(1, IntegerType)
    val stopExpr = new Size(original.child, false)
    val stepExpr = new Literal(1, IntegerType)
    val sequenceExpr = new Sequence(startExpr, stopExpr, stepExpr)
    val sequenceExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(sequenceExpr, attributeSeq)
      .doTransform(args)

    val funcMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // map_from_arrays(sequence(1, size(array_or_map)), array_or_map)
    val mapFromArraysFuncId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(
        ExpressionMappings.MAP_FROM_ARRAYS,
        Seq(sequenceExpr.dataType, original.child.dataType),
        FunctionConfig.OPT))

    // Notice that in CH mapFromArrays accepts the second arguments as MapType or ArrayType
    // But in Spark, it accepts ArrayType.
    val keyType = IntegerType
    val (valType, valContainsNull) = original.child.dataType match {
      case a: ArrayType => (a.elementType, a.containsNull)
      case m: MapType =>
        (
          StructType(
            StructField("", m.keyType, false) ::
              StructField("", m.valueType, m.valueContainsNull) :: Nil),
          false)
      case _ =>
        throw new UnsupportedOperationException(
          s"posexplode(${original.child.dataType}) not supported yet.")
    }
    val outputType = MapType(keyType, valType, valContainsNull)
    val mapFromArraysExprNode = ExpressionBuilder.makeScalarFunction(
      mapFromArraysFuncId,
      Lists.newArrayList(sequenceExprNode, childNode),
      ConverterUtils.getTypeNode(outputType, original.child.nullable))

    // posexplode(map_from_arrays(sequence(1, size(array_or_map)), array_or_map))
    val funcId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(
        ExpressionMappings.POSEXPLODE,
        Seq(outputType),
        FunctionConfig.OPT))

    val childType = original.child.dataType
    childType match {
      case a: ArrayType =>
        // Output pos, col when input is array
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, false),
            StructField("col", a.elementType, a.containsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(mapFromArraysExprNode),
          ConverterUtils.getTypeNode(structType, false))
      case m: MapType =>
        // Output pos, key, value when input is map
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, false),
            StructField("key", m.keyType, false),
            StructField("value", m.valueType, m.valueContainsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(mapFromArraysExprNode),
          ConverterUtils.getTypeNode(structType, false))
      case _ =>
        throw new UnsupportedOperationException(s"posexplode($childType) not supported yet.")
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

    // just make a fake toType value, because native engine cannot accept datatype itself
    val toTypeNodes = ExpressionBuilder.makeDecimalLiteral(
      new Decimal().set(0, original.dataType.precision, original.dataType.scale))
    val expressionNodes =
      Lists.newArrayList(childNode, new BooleanLiteralNode(original.nullOnOverflow), toTypeNodes)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class MakeDecimalTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: MakeDecimal)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.dataType, BooleanType),
        FunctionConfig.OPT))

    // use fake decimal literal, because velox function signature need to get return type
    // scale and precision by input type variable
    val toTypeNodes =
      ExpressionBuilder.makeDecimalLiteral(new Decimal().set(0, original.precision, original.scale))
    val expressionNodes =
      Lists.newArrayList(childNode, toTypeNodes, new BooleanLiteralNode(original.nullOnOverflow))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class Md5Transformer(substraitExprName: String, child: ExpressionTransformer, original: Md5)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (
      BackendsApiManager.getBackendName.equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)
    ) {
      // In Spark: md5(str)
      // In CH: lower(hex(md5(str)))
      // So we need to wrap md5(str) with lower and hex in substrait plan for clickhouse backend.
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

      val md5FuncId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName(
          substraitExprName,
          Seq(original.child.dataType),
          FunctionConfig.OPT))
      val md5ChildNode = child.doTransform(args)
      val md5ExprNodes = Lists.newArrayList(md5ChildNode)
      /// In CH, the output type of md5 is FixedString(16)
      val md5TypeNode = TypeBuilder.makeFixedChar(original.nullable, 16)
      val md5FuncNode = ExpressionBuilder.makeScalarFunction(md5FuncId, md5ExprNodes, md5TypeNode)

      // wrap in hex: hex(md5(str))
      val hexFuncId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName("hex", Seq(CharType(16)), FunctionConfig.OPT))
      val hexExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(md5FuncNode)
      val hexTypeNode = TypeBuilder.makeString(original.nullable)
      val hexFuncNode = ExpressionBuilder.makeScalarFunction(hexFuncId, hexExprNodes, hexTypeNode)

      // wrap in lower: lower(hex(md5(str)))
      val lowerFuncId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName("lower", Seq(StringType), FunctionConfig.OPT))
      val lowerExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(hexFuncNode)
      val lowerTypeNode = TypeBuilder.makeString(original.nullable)
      ExpressionBuilder.makeScalarFunction(lowerFuncId, lowerExprNodes, lowerTypeNode)
    } else {
      UnaryExpressionTransformer(substraitExprName, child, original).doTransform(args)
    }
  }
}

/** Transformer for the normal unary expression */
class UnaryExpressionTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer
  with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

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
      case m: MakeDecimal =>
        new MakeDecimalTransformer(substraitExprName, child, m)
      case p: PromotePrecision =>
        new PromotePrecisionTransformer(child, p)
      case extract if extract.isInstanceOf[GetDateField] || extract.isInstanceOf[GetTimeField] =>
        new ExtractDateTransformer(substraitExprName, child, extract)
      case _ =>
        new UnaryExpressionTransformer(substraitExprName, child, original)
    }
  }
}
