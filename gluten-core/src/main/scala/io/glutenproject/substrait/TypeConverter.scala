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
package io.glutenproject.substrait

import io.glutenproject.exception.GlutenNotSupportException

import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import com.google.protobuf.CodedInputStream
import io.substrait.proto.Type.KindCase._

case class ExpressionType(dataType: DataType, nullable: Boolean) {}

object TypeConverter {
  def isNullable(n: io.substrait.proto.Type.Nullability): Boolean = {
    n match {
      case io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE => true
      case _ => false
    }
  }

  def from(bytes: Array[Byte]): ExpressionType = {
    val input = CodedInputStream.newInstance(bytes)
    val parsed = io.substrait.proto.Type.parseFrom(input)
    from(parsed)
  }

  def from(t: io.substrait.proto.Type): ExpressionType = {
    t.getKindCase match {
      case BOOL => ExpressionType(BooleanType, isNullable(t.getBool.getNullability))
      case I8 => ExpressionType(ByteType, isNullable(t.getI8.getNullability))
      case I16 => ExpressionType(ShortType, isNullable(t.getI16.getNullability))
      case I32 => ExpressionType(IntegerType, isNullable(t.getI32.getNullability))
      case I64 => ExpressionType(LongType, isNullable(t.getI64.getNullability))
      case FP32 => ExpressionType(FloatType, isNullable(t.getFp32.getNullability))
      case FP64 => ExpressionType(DoubleType, isNullable(t.getFp64.getNullability))
      case STRING => ExpressionType(StringType, isNullable(t.getString.getNullability))
      case BINARY => ExpressionType(BinaryType, isNullable(t.getBinary.getNullability))
      case TIMESTAMP => ExpressionType(TimestampType, isNullable(t.getTimestamp.getNullability))
      case t =>
        throw new GlutenNotSupportException(s"Conversion from substrait type not supported: $t")
    }
  }
}
