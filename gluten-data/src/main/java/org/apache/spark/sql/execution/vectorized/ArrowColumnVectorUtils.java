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
package org.apache.spark.sql.execution.vectorized;

import org.apache.gluten.vectorized.ArrowWritableColumnVector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;

/**
 * Utilities to help manipulate data associate with ColumnVectors. These should be used mostly for
 * debugging or other non-performance critical paths. These utilities are mostly used to convert
 * ColumnVectors into other formats.
 */
public class ArrowColumnVectorUtils {
  /**
   * Populates the entire `col` with `row[fieldIdx]` This is copied from {@link
   * ColumnVectorUtils#populate}. We changed the way to putByteArrays.
   */
  public static void populate(WritableColumnVector col, InternalRow row, int fieldIdx) {
    ArrowWritableColumnVector arrowCol = (ArrowWritableColumnVector) col;

    int capacity = arrowCol.capacity;
    if (row.isNullAt(fieldIdx)) {
      arrowCol.putNulls(0, capacity);
    } else {
      if (arrowCol.dataType() == DataTypes.StringType) {
        UTF8String v = row.getUTF8String(fieldIdx);
        byte[] bytes = v.getBytes();
        for (int i = 0; i < capacity; i++) {
          col.putByteArray(i, bytes);
        }
      } else {
        populatePrimitive(col, row, fieldIdx);
      }
    }
  }

  public static void populatePrimitive(WritableColumnVector col, InternalRow row, int fieldIdx) {
    int capacity = col.capacity;
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.putNulls(0, capacity);
    } else {
      if (t == DataTypes.BooleanType) {
        col.putBooleans(0, capacity, row.getBoolean(fieldIdx));
      } else if (t == DataTypes.BinaryType) {
        col.putByteArray(0, row.getBinary(fieldIdx));
      } else if (t == DataTypes.ByteType) {
        col.putBytes(0, capacity, row.getByte(fieldIdx));
      } else if (t == DataTypes.ShortType) {
        col.putShorts(0, capacity, row.getShort(fieldIdx));
      } else if (t == DataTypes.IntegerType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t == DataTypes.LongType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      } else if (t == DataTypes.FloatType) {
        col.putFloats(0, capacity, row.getFloat(fieldIdx));
      } else if (t == DataTypes.DoubleType) {
        col.putDoubles(0, capacity, row.getDouble(fieldIdx));
      } else if (t == DataTypes.StringType) {
        UTF8String v = row.getUTF8String(fieldIdx);
        byte[] bytes = v.getBytes();
        for (int i = 0; i < capacity; i++) {
          col.putByteArray(i, bytes);
        }
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType) t;
        Decimal d = row.getDecimal(fieldIdx, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.putInts(0, capacity, (int) d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.putLongs(0, capacity, d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          for (int i = 0; i < capacity; i++) {
            col.putByteArray(i, bytes, 0, bytes.length);
          }
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval) row.get(fieldIdx, t);
        col.getChild(0).putInts(0, capacity, c.months);
        col.getChild(1).putInts(0, capacity, c.days);
        col.getChild(2).putLongs(0, capacity, c.microseconds);
      } else if (t instanceof DateType || t instanceof YearMonthIntervalType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t instanceof TimestampType
          || t instanceof TimestampNTZType
          || t instanceof DayTimeIntervalType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      } else {
        throw new RuntimeException(
            String.format(
                "DataType %s is not supported" + " in column vectorized reader.", t.sql()));
      }
    }
  }
}
