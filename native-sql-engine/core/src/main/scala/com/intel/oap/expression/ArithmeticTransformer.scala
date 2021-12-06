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

package com.intel.oap.expression

import com.google.common.collect.Lists
import com.intel.oap.substrait.`type`.TypeBuiler
import com.intel.oap.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil

/**
 * A version of add that supports columnar processing for longs.
 */
class AddTransformer(left: Expression, right: Expression, original: Expression)
    extends Add(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

class SubtractTransformer(left: Expression, right: Expression, original: Expression)
    extends Subtract(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

class MultiplyTransformer(left: Expression, right: Expression, original: Expression)
    extends Multiply(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  val left_val: Any = left match {
    case c: CastTransformer =>
      if (c.child.dataType.isInstanceOf[DecimalType] &&
        c.dataType.isInstanceOf[DecimalType]) {
        c.child
      } else {
        left
      }
    case _ =>
      left
  }
  val right_val: Any = right match {
    case c: CastTransformer =>
      if (c.child.dataType.isInstanceOf[DecimalType] &&
        c.dataType.isInstanceOf[DecimalType]) {
        c.child
      } else {
        right
      }
    case _ =>
      right
  }

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left_val.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right_val.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, Long]]
    val functionName = "MULTIPLY"
    var functionId = functionMap.size().asInstanceOf[java.lang.Integer].longValue()
    if (!functionMap.containsKey(functionName)) {
      functionMap.put(functionName, functionId)
    } else {
      functionId = functionMap.get(functionName)
    }
    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(left.dataType, "res", nullable = true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class DivideTransformer(left: Expression, right: Expression,
                        original: Expression, resType: DecimalType = null)
    extends Divide(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

class BitwiseAndTransformer(left: Expression, right: Expression, original: Expression)
    extends BitwiseAnd(left: Expression, right: Expression)
        with ExpressionTransformer
        with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

class BitwiseOrTransformer(left: Expression, right: Expression, original: Expression)
    extends BitwiseOr(left: Expression, right: Expression)
        with ExpressionTransformer
        with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

class BitwiseXorTransformer(left: Expression, right: Expression, original: Expression)
    extends BitwiseXor(left: Expression, right: Expression)
        with ExpressionTransformer
        with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

object BinaryArithmeticTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression = {
      original match {
      case a: Add =>
        new AddTransformer(left, right, a)
      case s: Subtract =>
        new SubtractTransformer(left, right, s)
      case m: Multiply =>
        new MultiplyTransformer(left, right, m)
      case d: Divide =>
        new DivideTransformer(left, right, d)
      case a: BitwiseAnd =>
        new BitwiseAndTransformer(left, right, a)
      case o: BitwiseOr =>
        new BitwiseOrTransformer(left, right, o)
      case x: BitwiseXor =>
        new BitwiseXorTransformer(left, right, x)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  def createDivide(left: Expression, right: Expression,
                   original: Expression, resType: DecimalType): Expression = {
    original match {
      case d: Divide =>
        new DivideTransformer(left, right, d, resType)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
