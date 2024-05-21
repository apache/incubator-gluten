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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.`type`.ListNode
import org.apache.gluten.substrait.`type`.MapNode
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, StructLiteralNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

case class ChildTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {
  override def dataType: DataType = child.dataType

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class CastTransformer(substraitExprName: String, child: ExpressionTransformer, original: Cast)
  extends UnaryExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val typeNode = ConverterUtils.getTypeNode(dataType, original.nullable)
    ExpressionBuilder.makeCast(typeNode, child.doTransform(args), original.ansiEnabled)
  }
}

case class ExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Explode)
  extends UnaryExpressionTransformer {

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
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, l.getNestedType)
      case m: MapNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, m.getNestedType)
      case _ =>
        throw new GlutenNotSupportException(s"explode($childTypeNode) not supported yet.")
    }
  }
}

case class CheckOverflowTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: CheckOverflow)
  extends UnaryExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    BackendsApiManager.getTransformerApiInstance.createCheckOverflowExprNode(
      args,
      substraitExprName,
      child.doTransform(args),
      original.child.dataType,
      original.dataType,
      original.nullable,
      original.nullOnOverflow)
  }
}

/**
 * User can specify a seed for this function. If lacked, spark will generate a random number as
 * seed. We also need to pass a unique partitionIndex provided by framework to native library for
 * each thread. Then, seed plus partitionIndex will be the actual seed for generator, similar to
 * vanilla spark. This is based on the fact that partitioning is deterministic and one partition is
 * corresponding to one task thread.
 */
case class RandTransformer(
    substraitExprName: String,
    explicitSeed: ExpressionTransformer,
    original: Rand)
  extends LeafExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (!original.hideSeed) {
      // TODO: for user-specified seed, we need to pass partition index to native engine.
      throw new GlutenNotSupportException("User-specified seed is not supported.")
    }
    super.doTransform(args)
  }
}

case class GetStructFieldTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: GetStructField)
  extends BinaryExpressionTransformer {
  override def left: ExpressionTransformer = child
  override def right: ExpressionTransformer = LiteralTransformer(original.ordinal)

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    childNode match {
      case node: StructLiteralNode =>
        node.getFieldLiteral(original.ordinal)
      case _ =>
        super.doTransform(args)
    }
  }
}
