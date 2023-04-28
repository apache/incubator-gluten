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

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, IntLiteralNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

import java.util.ArrayList

case class CHSha1Transformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Sha1)
  extends Sha1Transformer(substraitExprName, child, original)
  with Logging {

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
  extends Sha2Transformer(substraitExprName, left, right, original)
  with Logging {
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
