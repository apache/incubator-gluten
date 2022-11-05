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
package io.substrait.spark.expression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._
import org.apache.spark.substrait.TypeConverter

import io.substrait.expression.{Expression => SExpression}
import io.substrait.expression.ExpressionCreator.{decimal, _}

class LiteralConverter extends Logging {

  object Nonnull {
    private def sparkDecimal2Substrait(d: Decimal): SExpression.Literal =
      decimal(false, d.toJavaBigDecimal, d.precision, d.scale)

    val _bool: Boolean => SExpression.Literal = bool(false, _)
    val _i8: Byte => SExpression.Literal = i8(false, _)
    val _i16: Short => SExpression.Literal = i16(false, _)
    val _i32: Int => SExpression.Literal = i32(false, _)
    val _i64: Long => SExpression.Literal = i64(false, _)
    val _fp32: Float => SExpression.Literal = fp32(false, _)
    val _fp64: Double => SExpression.Literal = fp64(false, _)
    val _decimal: Decimal => SExpression.Literal = sparkDecimal2Substrait
    val _date: Int => SExpression.Literal = date(false, _)
  }

  private def convertWithValue(literal: Literal): Option[SExpression.Literal] = {
    Option.apply(
      literal match {
        case Literal(b: Boolean, BooleanType) => Nonnull._bool(b)
        case Literal(b: Byte, ByteType) => Nonnull._i8(b)
        case Literal(s: Short, ShortType) => Nonnull._i16(s)
        case Literal(i: Integer, IntegerType) => Nonnull._i32(i)
        case Literal(l: Long, LongType) => Nonnull._i64(l)
        case Literal(f: Float, FloatType) => Nonnull._fp32(f)
        case Literal(d: Double, DoubleType) => Nonnull._fp64(d)
        case Literal(d: Decimal, _) => Nonnull._decimal(d)
        case Literal(d: Integer, DateType) => Nonnull._date(d)
        case _ => null
      }
    )
  }

  def convert(literal: Literal): Option[SExpression.Literal] = {
    if (literal.nullable) {
      TypeConverter
        .convert(literal.dataType, nullable = true)
        .map(typedNull)
    } else {
      convertWithValue(literal)
    }
  }

  def convertWithThrow(literal: Literal): SExpression.Literal = {
    convert(literal)
      .getOrElse(
        throw new UnsupportedOperationException(
          s"Unable to convert the type ${literal.dataType.typeName}"))
  }
}

object LiteralConverter extends LiteralConverter
