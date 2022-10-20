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
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.types._

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
      ConverterUtils.makeFuncName(ConverterUtils.IS_NOT_NULL, Seq(child.dataType)))
    val expressionNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
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
      ConverterUtils.makeFuncName(ConverterUtils.IS_NULL, Seq(child.dataType)))
    val expressionNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class NotTransformer(child: Expression, original: Expression)
  extends Not(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.NOT, Seq(child.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class AbsTransformer(child: Expression, original: Expression)
  extends Abs(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: Abs.")
  }
}

class AsciiTransformer(child: Expression, original: Expression)
  extends Ascii(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ASCII, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LengthTransformer(child: Expression, original: Expression)
  extends Length(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LENGTH, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LowerTransformer(child: Expression, original: Expression)
  extends Lower(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOWER, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class UpperTransformer(child: Expression, original: Expression)
  extends Upper(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.UPPER, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class BitwiseNotTransformer(child: Expression, original: Expression)
  extends BitwiseNot(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: BitwiseNot.")
  }
}

class KnownFloatingPointNormalizedTransformer(
                                               child: Expression,
                                               original: KnownFloatingPointNormalized)
  extends KnownFloatingPointNormalized(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.asInstanceOf[ExpressionTransformer].doTransform(args)
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
    throw new UnsupportedOperationException("Not supported: CheckOverflow.")
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

    val typeNode = ConverterUtils.getTypeNode(dataType, original.nullable)
    ExpressionBuilder.makeCast(typeNode, child_node.asInstanceOf[ExpressionNode])
  }
}

class UnscaledValueTransformer(child: Expression, original: Expression)
  extends UnscaledValue(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnscaledValue.")
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
    throw new UnsupportedOperationException("Not supported: MakeDecimal.")
  }
}

class NormalizeNaNAndZeroTransformer(child: Expression, original: NormalizeNaNAndZero)
  extends NormalizeNaNAndZero(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // TODO: A Temporary workaround to make shuffle repartition expression
    // pass with double/float type.
    // We need to support converting substrait to gandiva expressions in native.
    child.asInstanceOf[ExpressionTransformer].doTransform(args)
  }
}

class PromotePrecisionTransformer(child: Expression, original: PromotePrecision)
  extends PromotePrecision(child: Expression) with ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: PromotePrecision.")
  }
}

object UnaryOperatorTransformer {

  def create(child: Expression, original: Expression): Expression = original match {
    case in: IsNull =>
      new IsNullTransformer(child, in)
    case i: IsNotNull =>
      new IsNotNullTransformer(child, i)
    case y: Year =>
      new YearTransformer(child)
    case m: Month =>
      new MonthTransformer(child)
    case d: DayOfMonth =>
      new DayOfMonthTransformer(child)
    case doy: DayOfYear =>
      new DayOfYearTransformer(child)
    case dow: DayOfWeek =>
      new DayOfWeekTransformer(child)
    case n: Not =>
      new NotTransformer(child, n)
    case a: Abs =>
      new AbsTransformer(child, a)
    case ascii: Ascii =>
      new AsciiTransformer(child, ascii)
    case len: Length =>
      new LengthTransformer(child, len)
    case lower: Lower =>
      new LowerTransformer(child, lower)
    case upper: Upper =>
      new UpperTransformer(child, upper)
    case c: Cast =>
      new CastTransformer(child, c.dataType, c.timeZoneId, c)
    case u: UnscaledValue =>
      new UnscaledValueTransformer(child, u)
    case u: MakeDecimal =>
      new MakeDecimalTransformer(child, u.precision, u.scale, u.nullOnOverflow, u)
    case n: BitwiseNot =>
      new BitwiseNotTransformer(child, n)
    case k: KnownFloatingPointNormalized =>
      new KnownFloatingPointNormalizedTransformer(child, k)
    case n: NormalizeNaNAndZero =>
      new NormalizeNaNAndZeroTransformer(child, n)
    case p: PromotePrecision =>
      new PromotePrecisionTransformer(child, p)
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
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
