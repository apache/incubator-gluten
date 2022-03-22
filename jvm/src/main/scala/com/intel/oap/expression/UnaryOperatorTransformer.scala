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
import com.intel.oap.expression.DateTimeExpressionsTransformer._
import com.intel.oap.substrait.`type`.TypeBuiler
import com.intel.oap.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.expression._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.types._

/**
 * A version of add that supports columnar processing for longs.
 */
class IsNotNullTransformer(child: Expression, original: Expression)
    extends IsNotNull(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node: ExpressionNode =
      child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName("is_not_null", Seq(child.dataType)))
    val expressNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)
    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class IsNullTransformer(child: Expression, original: Expression)
    extends IsNotNull(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node: ExpressionNode =
      child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName("is_null", Seq(child.dataType)))
    val expressNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuiler.makeBoolean(true)
    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class MonthTransformer(child: Expression, original: Expression)
    extends Month(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class DayOfMonthTransformer(child: Expression, original: Expression)
  extends DayOfMonth(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class YearTransformer(child: Expression, original: Expression)
  extends Year(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class NotTransformer(child: Expression, original: Expression)
    extends Not(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class AbsTransformer(child: Expression, original: Expression)
    extends Abs(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class UpperTransformer(child: Expression, original: Expression)
    extends Upper(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class BitwiseNotTransformer(child: Expression, original: Expression)
    extends BitwiseNot(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class CheckOverflowTransformer(child: Expression, original: CheckOverflow)
    extends CheckOverflow(
      child: Expression,
      original.dataType: DecimalType,
      original.nullOnOverflow: Boolean)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class CastTransformer(
    child: Expression,
    datatype: DataType,
    timeZoneId: Option[String],
    original: Expression)
    extends Cast(child: Expression, datatype: DataType, timeZoneId: Option[String])
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName("cast", Seq(child.dataType)))
    val expressNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable = true)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class UnscaledValueTransformer(child: Expression, original: Expression)
    extends UnscaledValue(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class MakeDecimalTransformer(
    child: Expression,
    precision: Int,
    scale: Int,
    nullOnOverflow: Boolean,
    original: Expression)
    extends MakeDecimal(child: Expression, precision: Int, scale: Int, nullOnOverflow: Boolean)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

class NormalizeNaNAndZeroTransformer(child: Expression, original: NormalizeNaNAndZero)
    extends NormalizeNaNAndZero(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Not supported currently.
    val child_node = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet")
    }
    child_node
  }
}

object UnaryOperatorTransformer {

  def create(child: Expression, original: Expression): Expression = original match {
    case in: IsNull =>
      new IsNullTransformer(child, in)
    case i: IsNotNull =>
      new IsNotNullTransformer(child, i)
    case y: Year =>
      if (child.dataType.isInstanceOf[TimestampType]) {
        new DateTimeExpressionsTransformer.YearTransformer(child)
      } else {
        new YearTransformer(child, y)
      }
    case m: Month =>
      if (child.dataType.isInstanceOf[TimestampType]) {
        new DateTimeExpressionsTransformer.MonthTransformer(child)
      } else {
        new MonthTransformer(child, m)
      }
    case d: DayOfMonth =>
      if (child.dataType.isInstanceOf[TimestampType]) {
        new DateTimeExpressionsTransformer.DayOfMonthTransformer(child)
      } else {
        new DayOfMonthTransformer(child, d)
      }
    case n: Not =>
      new NotTransformer(child, n)
    case a: Abs =>
      new AbsTransformer(child, a)
    case u: Upper =>
      new UpperTransformer(child, u)
    case c: Cast =>
      new CastTransformer(child, c.dataType, c.timeZoneId, c)
    case u: UnscaledValue =>
      new UnscaledValueTransformer(child, u)
    case u: MakeDecimal =>
      new MakeDecimalTransformer(child, u.precision, u.scale, u.nullOnOverflow, u)
    case n: BitwiseNot =>
      new BitwiseNotTransformer(child, n)
    case a: KnownFloatingPointNormalized =>
      child
    case n: NormalizeNaNAndZero =>
      new NormalizeNaNAndZeroTransformer(child, n)
    case a: PromotePrecision =>
      child
    case a: CheckOverflow =>
      new CheckOverflowTransformer(child, a)
    case a: UnixDate =>
      new UnixDateTransformer(child)
    case a: UnixSeconds =>
      new UnixSecondsTransformer(child)
    case a: UnixMillis =>
      new UnixMillisTransformer(child)
    case a: UnixMicros =>
      new UnixMicrosTransformer(child)
    case a: SecondsToTimestamp =>
      new SecondsToTimestampTransformer(child)
    case a: MillisToTimestamp =>
      new MillisToTimestampTransformer(child)
    case a: MicrosToTimestamp =>
      new MicrosToTimestampTransformer(child)
    case other =>
      child.dataType match {
        case _: DateType => other match {
          case a: DayOfYear =>
            new DayOfYearTransformer(new CastTransformer(child, TimestampType, None, null))
          case a: DayOfWeek =>
            new DayOfWeekTransformer(new CastTransformer(child, TimestampType, None, null))
          case other =>
            throw new UnsupportedOperationException(s"not currently supported: $other.")
        }
        case _: TimestampType => other match {
          case a: Hour =>
            new HourTransformer(child)
          case a: Minute =>
            new MinuteTransformer(child)
          case a: Second =>
            new SecondTransformer(child)
          case other =>
            throw new UnsupportedOperationException(s"not currently supported: $other.")
        }
        case _ =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
  }
}
