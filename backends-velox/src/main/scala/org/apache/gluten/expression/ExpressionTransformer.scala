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
package org.apache.gluten.expression

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.expression.ExpressionConverter.replaceWithExpressionTransformer
import org.apache.gluten.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType}

import java.lang.{Integer => JInteger, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.language.existentials

case class VeloxAliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class VeloxNamedStructTransformer(
    substraitExprName: String,
    original: CreateNamedStruct,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    original.valExprs.map(replaceWithExpressionTransformer(_, attributeSeq))
  }
}

case class VeloxGetStructFieldTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: GetStructField)
  extends UnaryExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    childNode match {
      case node: StructLiteralNode =>
        node.getFieldLiteral(original.ordinal)
      case node: SelectionNode =>
        // Append the nested index to selection node.
        node.addNestedChildIdx(JInteger.valueOf(original.ordinal))
      case other =>
        throw new GlutenNotSupportException(s"$other is not supported.")
    }
  }
}

case class VeloxHashExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: HashExpression[_])
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // As of Spark 3.3, there are 3 kinds of HashExpression.
    // HiveHash is not supported in native backend and will fail native validation.
    val (seedNode, seedType) = original match {
      case XxHash64(_, seed) =>
        (ExpressionBuilder.makeLongLiteral(seed), LongType)
      case Murmur3Hash(_, seed) =>
        (ExpressionBuilder.makeIntLiteral(seed), IntegerType)
      case HiveHash(_) =>
        (ExpressionBuilder.makeIntLiteral(0), IntegerType)
    }
    val nodes = new JArrayList[ExpressionNode]()
    // Seed as the first argument
    nodes.add(seedNode)
    children.foreach(
      expression => {
        nodes.add(expression.doTransform(args))
      })
    val childrenTypes = seedType +: original.children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName =
      ConverterUtils.makeFuncName(substraitExprName, childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}

case class VeloxStringSplitTransformer(
    substraitExprName: String,
    srcExpr: ExpressionTransformer,
    regexExpr: ExpressionTransformer,
    limitExpr: ExpressionTransformer,
    original: StringSplit)
  extends ExpressionTransformer {
  // TODO: split function support limit arg
  override def children: Seq[ExpressionTransformer] = srcExpr :: regexExpr :: Nil

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (
      !regexExpr.isInstanceOf[LiteralTransformer] ||
      !limitExpr.isInstanceOf[LiteralTransformer]
    ) {
      throw new GlutenNotSupportException(
        "Gluten only supports literal input as limit/regex for split function.")
    }

    val limit = limitExpr.doTransform(args).asInstanceOf[IntLiteralNode].getValue
    val regex = regexExpr.doTransform(args).asInstanceOf[StringLiteralNode].getValue
    if (limit > 0 || regex.length > 1) {
      throw new GlutenNotSupportException(
        s"$original supported single-length regex and negative limit, but given $limit and $regex")
    }

    super.doTransform(args)
  }
}
