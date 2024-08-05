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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, IntervalUtils, MapData, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.UTF8StringBuilder
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.time.ZoneOffset

trait ToStringBase { self: UnaryExpression with TimeZoneAwareExpression =>

  private lazy val dateFormatter = DateFormatter()
  private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)
  private lazy val timestampNTZFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)

  // The brackets that are used in casting structs and maps to strings
  protected def leftBracket: String
  protected def rightBracket: String

  // The string value to use to represent null elements in array/struct/map.
  protected def nullString: String

  protected def useDecimalPlainString: Boolean

  protected def useHexFormatForBinary: Boolean

  // Makes the function accept Any type input by doing `asInstanceOf[T]`.
  @inline private def acceptAny[T](func: T => Any): Any => Any = i => func(i.asInstanceOf[T])

  // Returns a function to convert a value to pretty string. The function assumes input is not null.
  final protected def castToString(from: DataType): Any => Any = from match {
    case CalendarIntervalType =>
      acceptAny[CalendarInterval](i => UTF8String.fromString(i.toString))
    case BinaryType if useHexFormatForBinary =>
      acceptAny[Array[Byte]](
        binary => UTF8String.fromString(binary.map("%02X".format(_)).mkString("[", " ", "]")))
    case BinaryType =>
      acceptAny[Array[Byte]](UTF8String.fromBytes)
    case DateType =>
      acceptAny[Int](d => UTF8String.fromString(dateFormatter.format(d)))
    case TimestampType =>
      acceptAny[Long](t => UTF8String.fromString(timestampFormatter.format(t)))
    case TimestampNTZType =>
      acceptAny[Long](t => UTF8String.fromString(timestampNTZFormatter.format(t)))
    case ArrayType(et, _) =>
      acceptAny[ArrayData](
        array => {
          val builder = new UTF8StringBuilder
          builder.append("[")
          if (array.numElements > 0) {
            val toUTF8String = castToString(et)
            if (array.isNullAt(0)) {
              if (nullString.nonEmpty) builder.append(nullString)
            } else {
              builder.append(toUTF8String(array.get(0, et)).asInstanceOf[UTF8String])
            }
            var i = 1
            while (i < array.numElements) {
              builder.append(",")
              if (array.isNullAt(i)) {
                if (nullString.nonEmpty) builder.append(" " + nullString)
              } else {
                builder.append(" ")
                builder.append(toUTF8String(array.get(i, et)).asInstanceOf[UTF8String])
              }
              i += 1
            }
          }
          builder.append("]")
          builder.build()
        })
    case MapType(kt, vt, _) =>
      acceptAny[MapData](
        map => {
          val builder = new UTF8StringBuilder
          builder.append(leftBracket)
          if (map.numElements > 0) {
            val keyArray = map.keyArray()
            val valueArray = map.valueArray()
            val keyToUTF8String = castToString(kt)
            val valueToUTF8String = castToString(vt)
            builder.append(keyToUTF8String(keyArray.get(0, kt)).asInstanceOf[UTF8String])
            builder.append(" ->")
            if (valueArray.isNullAt(0)) {
              if (nullString.nonEmpty) builder.append(" " + nullString)
            } else {
              builder.append(" ")
              builder.append(valueToUTF8String(valueArray.get(0, vt)).asInstanceOf[UTF8String])
            }
            var i = 1
            while (i < map.numElements) {
              builder.append(", ")
              builder.append(keyToUTF8String(keyArray.get(i, kt)).asInstanceOf[UTF8String])
              builder.append(" ->")
              if (valueArray.isNullAt(i)) {
                if (nullString.nonEmpty) builder.append(" " + nullString)
              } else {
                builder.append(" ")
                builder.append(
                  valueToUTF8String(valueArray.get(i, vt))
                    .asInstanceOf[UTF8String])
              }
              i += 1
            }
          }
          builder.append(rightBracket)
          builder.build()
        })
    case StructType(fields) =>
      acceptAny[InternalRow](
        row => {
          val builder = new UTF8StringBuilder
          builder.append(leftBracket)
          if (row.numFields > 0) {
            val st = fields.map(_.dataType)
            val toUTF8StringFuncs = st.map(castToString)
            if (row.isNullAt(0)) {
              if (nullString.nonEmpty) builder.append(nullString)
            } else {
              builder.append(toUTF8StringFuncs(0)(row.get(0, st(0))).asInstanceOf[UTF8String])
            }
            var i = 1
            while (i < row.numFields) {
              builder.append(",")
              if (row.isNullAt(i)) {
                if (nullString.nonEmpty) builder.append(" " + nullString)
              } else {
                builder.append(" ")
                builder.append(toUTF8StringFuncs(i)(row.get(i, st(i))).asInstanceOf[UTF8String])
              }
              i += 1
            }
          }
          builder.append(rightBracket)
          builder.build()
        })
    case pudt: PythonUserDefinedType => castToString(pudt.sqlType)
    case udt: UserDefinedType[_] =>
      o => UTF8String.fromString(udt.deserialize(o).toString)
    case YearMonthIntervalType(startField, endField) =>
      acceptAny[Int](
        i =>
          UTF8String.fromString(
            IntervalUtils.toYearMonthIntervalString(i, ANSI_STYLE, startField, endField)))
    case DayTimeIntervalType(startField, endField) =>
      acceptAny[Long](
        i =>
          UTF8String.fromString(
            IntervalUtils.toDayTimeIntervalString(i, ANSI_STYLE, startField, endField)))
    case _: DecimalType if useDecimalPlainString =>
      acceptAny[Decimal](d => UTF8String.fromString(d.toJavaBigDecimal.toPlainString))
    case StringType => identity
    case _ => o => UTF8String.fromString(o.toString)
  }

  // Returns a function to generate code to convert a value to pretty string. It assumes the input
  // is not null.
  final protected def castToStringCode(
      from: DataType,
      ctx: CodegenContext): (ExprValue, ExprValue) => Block = {
    throw new UnsupportedOperationException()
  }
}
