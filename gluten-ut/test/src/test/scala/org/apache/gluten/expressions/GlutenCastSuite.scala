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
package org.apache.gluten.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Cast, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

class GlutenCastSuite extends SparkFunSuite with ExpressionEvalHelper {
  def checkCast(v: Literal, targetType: DataType, expected: Any): Unit = {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(Cast(v, targetType), expected)
    }
  }

  test("cast array of integer types to array of double") {
    val intArray = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val bigintArray = Literal.create(Seq(10000000000L), ArrayType(LongType))
    val smallintArray = Literal.create(Seq(1.toShort, -1.toShort), ArrayType(ShortType))
    val tinyintArray = Literal.create(Seq(1.toByte, -1.toByte), ArrayType(ByteType))

    checkCast(intArray, ArrayType(DoubleType), Seq(1.0, 2.0, 3.0))
    checkCast(bigintArray, ArrayType(DoubleType), Seq(1.0e10))
    checkCast(smallintArray, ArrayType(DoubleType), Seq(1.0, -1.0))
    checkCast(tinyintArray, ArrayType(DoubleType), Seq(1.0, -1.0))
  }

  test("cast array of double to array of integer types") {
    val doubleArray = Literal.create(Seq(1.9, -2.1), ArrayType(DoubleType))

    checkCast(doubleArray, ArrayType(IntegerType), Seq(1, -2))
    checkCast(doubleArray, ArrayType(LongType), Seq(1L, -2L))
    checkCast(doubleArray, ArrayType(ShortType), Seq(1.toShort, -2.toShort))
    checkCast(doubleArray, ArrayType(ByteType), Seq(1.toByte, -2.toByte))
  }

  test("cast array element from allowed types to string (varchar)") {
    val doubleArray = Literal.create(Seq(1.1, null, 3.3), ArrayType(DoubleType))
    val boolArray = Literal.create(Seq(true, false), ArrayType(BooleanType))
    val timestampArray = Literal.create(
      Seq(Timestamp.valueOf("2023-01-01 12:00:00"), null),
      ArrayType(TimestampType)
    )
    val integerArray = Literal.create(Seq(1, null, 2), ArrayType(IntegerType))
    val longArray = Literal.create(Seq(1L, null, 2L), ArrayType(LongType))
    val dateArray = Literal.create(
      Seq(Date.valueOf("2024-01-01"), null, Date.valueOf("2024-01-03")),
      ArrayType(DateType)
    )

    checkCast(doubleArray, ArrayType(StringType), Seq("1.1", null, "3.3"))
    checkCast(boolArray, ArrayType(StringType), Seq("true", "false"))
    checkCast(timestampArray, ArrayType(StringType), Seq("2023-01-01 12:00:00", null))
    checkCast(integerArray, ArrayType(StringType), Seq("1", null, "2"))
    checkCast(longArray, ArrayType(StringType), Seq("1", null, "2"))
    checkCast(dateArray, ArrayType(StringType), Seq("2024-01-01", null, "2024-01-03"))
  }

  test("cast array of numeric types to array of boolean") {
    val tinyintArray = Literal.create(Seq(0.toByte, 1.toByte, -1.toByte), ArrayType(ByteType))
    val smallintArray = Literal.create(Seq(0.toShort, 2.toShort, -2.toShort), ArrayType(ShortType))
    val intArray = Literal.create(Seq(0, 3, -3), ArrayType(IntegerType))
    val bigintArray = Literal.create(Seq(0L, 100L, -100L), ArrayType(LongType))
    val floatArray = Literal.create(Seq(0.0f, 1.5f, -2.3f), ArrayType(FloatType))
    val doubleArray = Literal.create(Seq(0.0, 2.5, -3.7), ArrayType(DoubleType))

    checkCast(tinyintArray, ArrayType(BooleanType), Seq(false, true, true))
    checkCast(smallintArray, ArrayType(BooleanType), Seq(false, true, true))
    checkCast(intArray, ArrayType(BooleanType), Seq(false, true, true))
    checkCast(bigintArray, ArrayType(BooleanType), Seq(false, true, true))
    checkCast(floatArray, ArrayType(BooleanType), Seq(false, true, true))
    checkCast(doubleArray, ArrayType(BooleanType), Seq(false, true, true))
  }

  test("cast array of string") {
    val integerStringArray = Literal.create(Seq("123", "-98", "abc"), ArrayType(StringType))
    val floatingPointStringArray =
      Literal.create(Seq("123e-2", "-234.548", "xyz"), ArrayType(StringType))
    val timestampStringArray = Literal.create(
      Seq("2023-01-01 12:00:00", "2023-01-02 12:00:00", "def"),
      ArrayType(StringType))
    val dateStringArray =
      Literal.create(Seq("2024-01-01", "2024-01-02", "uvw"), ArrayType(StringType))

    checkCast(integerStringArray, ArrayType(ByteType), Seq(123.toByte, -98.toByte, null))
    checkCast(integerStringArray, ArrayType(ShortType), Seq(123.toShort, -98.toShort, null))
    checkCast(integerStringArray, ArrayType(IntegerType), Seq(123, -98, null))
    checkCast(integerStringArray, ArrayType(LongType), Seq(123L, -98L, null))
    checkCast(floatingPointStringArray, ArrayType(DoubleType), Seq(1.23, -234.548, null))
    checkCast(floatingPointStringArray, ArrayType(FloatType), Seq(1.23f, -234.548f, null))
    checkCast(
      timestampStringArray,
      ArrayType(TimestampType),
      Seq(Timestamp.valueOf("2023-01-01 12:00:00"), Timestamp.valueOf("2023-01-02 12:00:00"), null))
    checkCast(
      dateStringArray,
      ArrayType(DateType),
      Seq(Date.valueOf("2024-01-01"), Date.valueOf("2024-01-02"), null))
  }

  test("cast maps") {
    val byteMap = Literal.create(
      Map(0.toByte -> 1.toByte, 2.toByte -> 3.toByte, 4.toByte -> 0.toByte),
      MapType(ByteType, ByteType))
    checkCast(
      byteMap,
      MapType(ByteType, DoubleType),
      Map(0.toByte -> 1.0, 2.toByte -> 3.0, 4.toByte -> 0.0))
    checkCast(
      byteMap,
      MapType(DoubleType, ByteType),
      Map(0.0 -> 1.toByte, 2.0 -> 3.toByte, 4.0 -> 0.toByte))
    checkCast(
      byteMap,
      MapType(ByteType, StringType),
      Map(0.toByte -> "1", 2.toByte -> "3", 4.toByte -> "0"))
    checkCast(
      byteMap,
      MapType(StringType, ByteType),
      Map("0" -> 1.toByte, "2" -> 3.toByte, "4" -> 0.toByte))
    checkCast(
      byteMap,
      MapType(ByteType, BooleanType),
      Map(0.toByte -> true, 2.toByte -> true, 4.toByte -> false))

    val shortMap = Literal.create(
      Map(1000.toShort -> 1001.toShort, 1002.toShort -> 1003.toShort, 1004.toShort -> 0.toShort),
      MapType(ShortType, ShortType))
    checkCast(
      shortMap,
      MapType(ShortType, DoubleType),
      Map(1000.toShort -> 1001.0, 1002.toShort -> 1003.0, 1004.toShort -> 0.0))
    checkCast(
      shortMap,
      MapType(DoubleType, ShortType),
      Map(1000.0 -> 1001.toShort, 1002.0 -> 1003.toShort, 1004.0 -> 0.toShort))
    checkCast(
      shortMap,
      MapType(ShortType, StringType),
      Map(1000.toShort -> "1001", 1002.toShort -> "1003", 1004.toShort -> "0"))
    checkCast(
      shortMap,
      MapType(StringType, ShortType),
      Map("1000" -> 1001.toShort, "1002" -> 1003.toShort, "1004" -> 0.toShort))
    checkCast(
      shortMap,
      MapType(ShortType, BooleanType),
      Map(1000.toShort -> true, 1002.toShort -> true, 1004.toShort -> false))

    val intMap = Literal.create(
      Map(100 -> "101", 102 -> "103", 104 -> "xyz"),
      MapType(IntegerType, StringType))
    checkCast(
      intMap,
      MapType(DoubleType, IntegerType),
      Map(100.0 -> 101, 102.0 -> 103, 104.0 -> null))
    checkCast(
      intMap,
      MapType(StringType, StringType),
      Map("100" -> "101", "102" -> "103", "104" -> "xyz"))

    val floatMap = Literal.create(
      Map(1.0f -> "2.0", -3.0f -> "40e-1", 5.0f -> "xyz"),
      MapType(FloatType, StringType))
    checkCast(
      floatMap,
      MapType(FloatType, FloatType),
      Map(1.0f -> 2.0f, -3.0f -> 4.0f, 5.0f -> null))
    checkCast(
      floatMap,
      MapType(StringType, StringType),
      Map("1.0" -> "2.0", "-3.0" -> "40e-1", "5.0" -> "xyz"))

    val timestampMap = Literal.create(
      Map(
        Timestamp.valueOf("2023-01-01 12:00:00") -> "2023-01-01 13:00:00",
        Timestamp.valueOf("2023-01-02 12:00:00") -> "xyz"),
      MapType(TimestampType, StringType)
    )
    checkCast(
      timestampMap,
      MapType(StringType, TimestampType),
      Map(
        "2023-01-01 12:00:00" -> Timestamp.valueOf("2023-01-01 13:00:00"),
        "2023-01-02 12:00:00" -> null))

    val dateMap = Literal.create(
      Map(Date.valueOf("2024-01-01") -> "2024-01-02", Date.valueOf("2024-02-01") -> "xyz"),
      MapType(DateType, StringType))
    checkCast(
      dateMap,
      MapType(StringType, DateType),
      Map("2024-01-01" -> Date.valueOf("2024-01-02"), "2024-02-01" -> null))
  }

  test("cast structs") {
    val struct = Literal.create(
      Row(
        Seq("123", "456.7", "2023-01-01 12:00:00", "2024-01-01"),
        Map(1.toByte -> 2.toShort, 3.toByte -> 4.toShort),
        Row(
          123.0,
          456.1f,
          0
        )
      ),
      StructType(
        Seq(
          StructField("a", ArrayType(StringType)),
          StructField("b", MapType(ByteType, ShortType)),
          StructField(
            "c",
            StructType(
              Seq(
                StructField("x", DoubleType),
                StructField("y", FloatType),
                StructField("z", IntegerType)
              ))
          )
        ))
    )

    checkCast(
      struct,
      StructType(
        Seq(
          StructField("a", ArrayType(LongType)),
          StructField("b", MapType(ShortType, IntegerType)),
          StructField(
            "c",
            StructType(
              Seq(
                StructField("x", StringType),
                StructField("y", StringType),
                StructField("z", BooleanType)
              ))
          )
        )),
      Row(
        Seq(123L, 456L, null, null),
        Map(1.toShort -> 2, 3.toShort -> 4),
        Row(
          "123.0",
          "456.1",
          false
        )
      )
    )

    checkCast(
      struct,
      StructType(
        Seq(
          StructField("a", ArrayType(DoubleType)),
          StructField("b", MapType(IntegerType, LongType)),
          StructField(
            "c",
            StructType(
              Seq(
                StructField("x", IntegerType),
                StructField("y", BooleanType),
                StructField("z", StringType)
              ))
          )
        )),
      Row(
        Seq(123.0, 456.7, null, null),
        Map(1 -> 2L, 3 -> 4L),
        Row(
          123,
          true,
          "0"
        )
      )
    )

    checkCast(
      struct,
      StructType(
        Seq(
          StructField("a", ArrayType(TimestampType)),
          StructField("b", MapType(LongType, BooleanType)),
          StructField(
            "c",
            StructType(
              Seq(
                StructField("x", ByteType),
                StructField("y", ShortType),
                StructField("z", DoubleType)
              ))
          )
        )),
      Row(
        Seq(
          null,
          null,
          Timestamp.valueOf("2023-01-01 12:00:00"),
          Timestamp.valueOf("2024-01-01 00:00:00")),
        Map(1L -> true, 3L -> true),
        Row(
          123.toByte,
          456.toShort,
          0.0
        )
      )
    )

    checkCast(
      struct,
      StructType(
        Seq(
          StructField("a", ArrayType(DateType)),
          StructField("b", MapType(StringType, DoubleType)),
          StructField(
            "c",
            StructType(
              Seq(
                StructField("x", IntegerType),
                StructField("y", LongType),
                StructField("z", FloatType)
              ))
          )
        )),
      Row(
        Seq(null, null, Date.valueOf("2023-01-01"), Date.valueOf("2024-01-01")),
        Map("1" -> 2.0, "3" -> 4.0),
        Row(
          123,
          456L,
          0.0f
        )
      )
    )
  }
}
