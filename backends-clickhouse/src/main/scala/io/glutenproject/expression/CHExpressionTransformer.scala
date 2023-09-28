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

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.collect.Lists

import java.util.{ArrayList, Locale}

case class CHSha1Transformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Sha1)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Spark sha1(child) = CH lower(hex(sha1(child)))
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // sha1(child)
    var fixedCharLength = 20
    val sha1FuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.child.dataType),
        FunctionConfig.OPT))
    val sha1TypeNode = TypeBuilder.makeFixedChar(original.child.nullable, fixedCharLength)
    val sha1FuncNode = ExpressionBuilder.makeScalarFunction(
      sha1FuncId,
      Lists.newArrayList(child.doTransform(args)),
      sha1TypeNode)

    // wrap in hex: hex(sha1(str))
    val hexFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("hex", Seq(CharType(fixedCharLength)), FunctionConfig.OPT))
    val hexExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(sha1FuncNode)
    val hexTypeNode = TypeBuilder.makeString(original.child.nullable)
    val hexFuncNode = ExpressionBuilder.makeScalarFunction(hexFuncId, hexExprNodes, hexTypeNode)

    // wrap in lower: lower(hex(sha1(str)))
    val lowerFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("lower", Seq(StringType), FunctionConfig.OPT))
    val lowerExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(hexFuncNode)
    val lowerTypeNode = TypeBuilder.makeString(original.child.nullable)
    ExpressionBuilder.makeScalarFunction(lowerFuncId, lowerExprNodes, lowerTypeNode)
  }
}

case class CHSha2Transformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    original: Sha2)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // bitLength must be literal for CH backend
    val rightNode = right.doTransform(args)
    if (!rightNode.isInstanceOf[IntLiteralNode]) {
      throw new UnsupportedOperationException(s"right in function sha2 must be a literal")
    }

    // CH backend only support bitLength in (0, 224, 256, 384, 512)
    val bitLength = rightNode.asInstanceOf[IntLiteralNode].getValue.toInt
    if (
      bitLength != 0 && bitLength != 224 && bitLength != 256 && bitLength != 384 && bitLength != 512
    ) {
      throw new UnsupportedOperationException(
        s"bit length in function sha2 must be 224, 256, 384 or 512")
    }

    // In Spark: sha2(str, bitLength)
    // in CH: lower(hex(SHA224(str))) or lower(hex(SHA256(str))) or ...
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val sha2FuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.left.dataType, original.right.dataType),
        FunctionConfig.OPT))
    val fixedCharLength = bitLength match {
      case 0 => 32
      case 224 => 28
      case 256 => 32
      case 384 => 48
      case 512 => 64
    }
    val sha2TypeNode = TypeBuilder.makeFixedChar(original.nullable, fixedCharLength)
    val leftNode = left.doTransform(args)
    val sha2FuncNode = ExpressionBuilder.makeScalarFunction(
      sha2FuncId,
      Lists.newArrayList(leftNode, rightNode),
      sha2TypeNode)

    // wrap in hex: hex(sha2(str, bitLength))
    val hexFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("hex", Seq(CharType(fixedCharLength)), FunctionConfig.OPT))
    val hexExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(sha2FuncNode)
    val hexTypeNode = TypeBuilder.makeString(original.nullable)
    val hexFuncNode = ExpressionBuilder.makeScalarFunction(hexFuncId, hexExprNodes, hexTypeNode)

    // wrap in lower: lower(hex(sha2(str, bitLength)))
    val lowerFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("lower", Seq(StringType), FunctionConfig.OPT))
    val lowerExprNodes: ArrayList[ExpressionNode] = Lists.newArrayList(hexFuncNode)
    val lowerTypeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(lowerFuncId, lowerExprNodes, lowerTypeNode)
  }
}

case class CHEqualNullSafeTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    original: EqualNullSafe)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {

    val leftNode = left.doTransform(args)
    val rightNode = right.doTransform(args)

    // if isnull(left) && isnull(right), then true
    // else if isnull(left) || isnull(right), then false
    // else equal(left, right)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // isnull(left)
    val isNullFuncNameLeft = ConverterUtils.makeFuncName(
      ExpressionNames.IS_NULL,
      original.left.children.map(_.dataType),
      FunctionConfig.OPT)
    val isNullFuncIdLeft = ExpressionBuilder.newScalarFunction(functionMap, isNullFuncNameLeft)
    val isNullNodeLeft = ExpressionBuilder.makeScalarFunction(
      isNullFuncIdLeft,
      Lists.newArrayList(leftNode),
      TypeBuilder.makeBoolean(false))

    // isnull(right)
    val isNullFuncNameRight = ConverterUtils.makeFuncName(
      ExpressionNames.IS_NULL,
      original.right.children.map(_.dataType),
      FunctionConfig.OPT)
    val isNullFuncIdRight = ExpressionBuilder.newScalarFunction(functionMap, isNullFuncNameRight)
    val isNullNodeRight = ExpressionBuilder.makeScalarFunction(
      isNullFuncIdRight,
      Lists.newArrayList(rightNode),
      TypeBuilder.makeBoolean(false))

    // isnull(left) && isnull(right)
    val andFuncName = ConverterUtils.makeFuncName(
      ExpressionNames.AND,
      Seq(BooleanType, BooleanType),
      FunctionConfig.OPT)
    val andFuncId = ExpressionBuilder.newScalarFunction(functionMap, andFuncName)
    val andNode = ExpressionBuilder.makeScalarFunction(
      andFuncId,
      Lists.newArrayList(isNullNodeLeft, isNullNodeRight),
      TypeBuilder.makeBoolean(false))

    // isnull(left) || isnull(right)
    val orFuncName = ConverterUtils.makeFuncName(
      ExpressionNames.OR,
      Seq(BooleanType, BooleanType),
      FunctionConfig.OPT)
    val orFuncId = ExpressionBuilder.newScalarFunction(functionMap, orFuncName)
    val orNode = ExpressionBuilder.makeScalarFunction(
      orFuncId,
      Lists.newArrayList(isNullNodeLeft, isNullNodeRight),
      TypeBuilder.makeBoolean(false))

    // equal(left, right)
    val equalFuncName = ConverterUtils.makeFuncName(
      ExpressionNames.EQUAL,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val equalFuncId = ExpressionBuilder.newScalarFunction(functionMap, equalFuncName)
    val equalNode = ExpressionBuilder.makeScalarFunction(
      equalFuncId,
      Lists.newArrayList(leftNode, rightNode),
      TypeBuilder.makeBoolean(original.left.nullable || original.right.nullable))

    new IfThenNode(
      Lists.newArrayList(andNode, orNode),
      Lists.newArrayList(new BooleanLiteralNode(true), new BooleanLiteralNode(false)),
      equalNode)
  }
}

case class CHSizeExpressionTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Size)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {

    if (original.legacySizeOfNull) {
      // when legacySizeOfNull is true, size(null) should return -1
      // so we wrap it to if(isnull(child), -1, size(child))
      val childNode = child.doTransform(args)
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

      val sizeFuncName = ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT)
      val sizeFuncId = ExpressionBuilder.newScalarFunction(functionMap, sizeFuncName)

      val exprNodes = Lists.newArrayList(childNode)
      val typeNode = ConverterUtils.getTypeNode(original.dataType, original.child.nullable)
      val sizeFuncNode = ExpressionBuilder.makeScalarFunction(sizeFuncId, exprNodes, typeNode)

      // isnull(child)
      val isNullFuncName = ConverterUtils.makeFuncName(
        ExpressionNames.IS_NULL,
        original.children.map(_.dataType),
        FunctionConfig.OPT)
      val isNullFuncId = ExpressionBuilder.newScalarFunction(functionMap, isNullFuncName)
      val isNullNode = ExpressionBuilder.makeScalarFunction(
        isNullFuncId,
        Lists.newArrayList(childNode),
        TypeBuilder.makeBoolean(false))

      new IfThenNode(
        Lists.newArrayList(isNullNode),
        Lists.newArrayList(new IntLiteralNode(-1)),
        sizeFuncNode)
    } else {
      GenericExpressionTransformer(substraitExprName, Seq(child), original).doTransform(args)
    }
  }
}

case class CHTruncTimestampTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: TruncTimestamp)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // The format must be constant string in the fucntion date_trunc of ch.
    if (!original.format.foldable) {
      throw new UnsupportedOperationException(
        s"The format ${original.format} must be constant string.")
    }

    val formatStr = original.format.eval().asInstanceOf[UTF8String]
    if (formatStr == null) {
      throw new UnsupportedOperationException("The format is null.")
    }

    val (newFormatStr, timeZoneIgnore) = formatStr.toString.toLowerCase(Locale.ROOT) match {
      case "second" => ("second", false)
      case "minute" => ("minute", false)
      case "hour" => ("hour", false)
      case "day" | "dd" => ("day", false)
      case "week" => ("week", true)
      case "mon" | "month" | "mm" => ("month", true)
      case "quarter" => ("quarter", true)
      case "year" | "yyyy" | "yy" => ("year", true)
      // Can not support now.
      // case "microsecond" => "microsecond"
      // case "millisecond" => "millisecond"
      case _ => throw new UnsupportedOperationException(s"The format $formatStr is invalidate.")
    }

    // Currently, data_trunc function can not support to set the specified timezone,
    // which is different with session_time_zone.
    if (
      timeZoneIgnore && timeZoneId.nonEmpty &&
      !timeZoneId.get.equalsIgnoreCase(
        SQLConf.get.getConfString(
          s"${CHBackendSettings.getBackendConfigPrefix}.runtime_config.timezone")
      )
    ) {
      throw new UnsupportedOperationException(
        s"It doesn't support trunc the format $newFormatStr with the specified timezone " +
          s"${timeZoneId.get}.")
    }

    val timestampNode = timestamp.doTransform(args)
    val lowerFormatNode = ExpressionBuilder.makeStringLiteral(newFormatStr)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val dataTypes = if (timeZoneId.nonEmpty) {
      Seq(original.format.dataType, original.timestamp.dataType, StringType)
    } else {
      Seq(original.format.dataType, original.timestamp.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(lowerFormatNode)
    expressionNodes.add(timestampNode)
    if (timeZoneId.isDefined) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CHStringLocateTransformer(
    substraitExprName: String,
    substrExpr: ExpressionTransformer,
    strExpr: ExpressionTransformer,
    startExpr: ExpressionTransformer,
    original: StringLocate)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
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
    val isnullFuncName =
      ConverterUtils.makeFuncName(ExpressionNames.IS_NULL, Seq(IntegerType), FunctionConfig.OPT)
    val isnullFuncId = ExpressionBuilder.newScalarFunction(functionMap, isnullFuncName)
    val isnullNode = ExpressionBuilder.makeScalarFunction(
      isnullFuncId,
      Lists.newArrayList(startNode),
      TypeBuilder.makeBoolean(false))

    new IfThenNode(
      Lists.newArrayList(isnullNode),
      Lists.newArrayList(new IntLiteralNode(0)),
      locateFuncNode)
  }
}

case class CHMd5Transformer(substraitExprName: String, child: ExpressionTransformer, original: Md5)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
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
    // In CH, the output type of md5 is FixedString(16)
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
  }
}
