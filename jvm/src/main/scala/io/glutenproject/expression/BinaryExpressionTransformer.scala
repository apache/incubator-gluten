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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import io.glutenproject.expression.DateTimeExpressionsTransformer.{DateDiffTransformer, UnixTimestampTransformer}
import io.glutenproject.substrait.`type`.TypeBuiler
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

class ShiftLeftTransformer(left: Expression, right: Expression, original: Expression)
  extends ShiftLeft(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class ShiftRightTransformer(left: Expression, right: Expression, original: Expression)
  extends ShiftRight(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class EndsWithTransformer(left: Expression, right: Expression, original: Expression)
  extends EndsWith(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class StartsWithTransformer(left: Expression, right: Expression, original: Expression)
  extends StartsWith(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class LikeTransformer(left: Expression, right: Expression, original: Expression)
  extends Like(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val left_node =
      left.asInstanceOf[ExpressionTransformer].doTransform(args)
    val right_node =
      right.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!left_node.isInstanceOf[ExpressionNode] ||
        !right_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LIKE, Seq(left.dataType, right.dataType)))

    val expressNodes = Lists.newArrayList(
      left_node.asInstanceOf[ExpressionNode],
      right_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class ContainsTransformer(left: Expression, right: Expression, original: Expression)
  extends Contains(left: Expression, right: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class DateAddIntervalTransformer(start: Expression, interval: Expression, original: DateAddInterval)
    extends DateAddInterval(start, interval, original.timeZoneId, original.ansiEnabled)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

object BinaryExpressionTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression =
    original match {
      case e: EndsWith =>
        new EndsWithTransformer(left, right, e)
      case s: StartsWith =>
        new StartsWithTransformer(left, right, s)
      case c: Contains =>
        new ContainsTransformer(left, right, c)
      case l: Like =>
        new LikeTransformer(left, right, l)
      case s: ShiftLeft =>
        new ShiftLeftTransformer(left, right, s)
      case s: ShiftRight =>
        new ShiftRightTransformer(left, right, s)
      case s: DateAddInterval =>
        new DateAddIntervalTransformer(left, right, s)
      case s: DateDiff =>
        new DateDiffTransformer(left, right)
      case a: UnixTimestamp =>
        new UnixTimestampTransformer(left, right)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
