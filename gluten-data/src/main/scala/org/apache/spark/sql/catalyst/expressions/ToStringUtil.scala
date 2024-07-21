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

import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

// Similar with ToPrettyString Expression
// Because case class ToPrettyString is forbid to inheritable by code style
case class ToStringUtil(timeZoneId: Option[String] = None)
  extends UnaryExpression
  with TimeZoneAwareExpression
  with ToStringBase {

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def withTimeZone(timeZoneId: String): ToStringUtil =
    copy(timeZoneId = Some(timeZoneId))

  override protected def withNewChildInternal(newChild: Expression): Expression =
    throw new UnsupportedOperationException()

  override protected def leftBracket: String = "{"
  override protected def rightBracket: String = "}"

  override protected def nullString: String = "NULL"

  override protected def useDecimalPlainString: Boolean = true

  override protected def useHexFormatForBinary: Boolean = true

  private[this] lazy val castFunc = (v: Any, inputType: DataType) => castToString(inputType)(v)

  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException()
  }

  def evalValue(input: Any, inputType: DataType): Any = {
    if (input == null) UTF8String.fromString(nullString) else castFunc(input, inputType)
  }

  // origins from ColumnarBatchRow.get()
  protected def vectorToString(vector: ColumnVector, from: DataType): Integer => Any =
    (ordinal: Integer) =>
      from match {
        case BooleanType => vector.getBoolean(ordinal)
        case ByteType => vector.getByte(ordinal)
        case ShortType => vector.getShort(ordinal)
        case IntegerType | DateType | YearMonthIntervalType(_, _) => vector.getInt(ordinal)
        case LongType | TimestampType | TimestampNTZType | DayTimeIntervalType(_, _) =>
          vector.getLong(ordinal)
        case FloatType => vector.getFloat(ordinal)
        case DoubleType => vector.getFloat(ordinal)
        case StringType => vector.getUTF8String(ordinal)
        case BinaryType => vector.getBinary(ordinal)
        case i: DecimalType => vector.getDecimal(ordinal, i.precision, i.scale)
        case ArrayType(_, _) => vector.getArray(ordinal)
        case MapType(_, _, _) => vector.getMap(ordinal)
        case StructType(_) => vector.getStruct(ordinal)
        case _ => throw new GlutenNotSupportException()
      }

  def evalBatch(
      batch: ColumnarBatch,
      start: Int,
      length: Int,
      dataType: StructType,
      truncate: Int = 20): String = {
    val rowsBuilder = new Array[mutable.StringBuilder](length)
    for (i <- 0 until length) {
      rowsBuilder(i) = new mutable.StringBuilder()
    }
    val end = start + length
    val endCol = batch.numCols() - 1
    for (col <- 0 until batch.numCols) {
      val colType = dataType.fields(col).dataType
      val vec = batch.column(col)
      val stringFunc = vectorToString(vec, colType)
      val castFunc = castToString(colType)
      for (i <- start until end) {
        val str =
          if (vec.isNullAt(i)) "NULL"
          else castFunc(stringFunc(i)).toString
        val truncateStr = if (truncate > 0 && str.length > truncate) {
          str.substring(0, truncate) + "..."
        } else {
          str
        }
        rowsBuilder(i - start).append(truncateStr)
        if (col != endCol) {
          rowsBuilder(i - start).append(" | ")
        }
      }
    }
    rowsBuilder.mkString(System.lineSeparator())
  }

  def evalRows(
      rows: Iterator[InternalRow],
      start: Int,
      length: Int,
      dataType: StructType,
      truncate: Int = 20): String = {
    rows
      .slice(start, start + length)
      .map(row => evalRow(row, dataType, truncate))
      .mkString(System.lineSeparator())
  }

  def evalRow(row: InternalRow, dataType: StructType, truncate: Int = 20): String = {
    dataType.zipWithIndex
      .map {
        case (colType, i) =>
          val str = evalValue(row.get(i, colType.dataType), colType.dataType).toString
          if (truncate > 0 && str.length > truncate) {
            str.substring(0, truncate) + "..."
          } else {
            str
          }
      }
      .mkString(" | ")
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException()
  }

  override def child : Expression = null
}
