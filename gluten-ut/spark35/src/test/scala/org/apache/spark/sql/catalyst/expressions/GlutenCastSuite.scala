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

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, ALL_TIMEZONES, UTC, UTC_OPT}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{fromJavaTimestamp, millisToMicros, TimeZoneUTC}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ThreadUtils

import java.sql.{Date, Timestamp}
import java.util.{Calendar, TimeZone}

class GlutenCastSuite extends CastWithAnsiOffSuite with GlutenTestsTrait {

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression =>
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
    }
  }

  // Register UDT For test("SPARK-32828")
  UDTRegistration.register(classOf[IExampleBaseType].getName, classOf[ExampleBaseTypeUDT].getName)
  UDTRegistration.register(classOf[IExampleSubType].getName, classOf[ExampleSubTypeUDT].getName)

  test("cast array element from integer to string") {
    val inputWithNull = Literal.create(Seq(1, null, 3), ArrayType(IntegerType))
    val expectedWithNull = Seq("1", null, "3")
    checkEvaluation(cast(inputWithNull, ArrayType(StringType)), expectedWithNull)

    val emptyInput = Literal.create(Seq.empty[Int], ArrayType(IntegerType))
    val expectedEmpty = Seq.empty[String]
    checkEvaluation(cast(emptyInput, ArrayType(StringType)), expectedEmpty)

    val inputNegative = Literal.create(Seq(-1, 0, 999999), ArrayType(IntegerType))
    val expectedNegative = Seq("-1", "0", "999999")
    checkEvaluation(cast(inputNegative, ArrayType(StringType)), expectedNegative)
  }

  test("cast array element from double to string") {
    val inputWithNull = Literal.create(Seq(1.1, null, 3.3), ArrayType(DoubleType))
    val expectedWithNull = Seq("1.1", null, "3.3")
    checkEvaluation(cast(inputWithNull, ArrayType(StringType)), expectedWithNull)

    val inputScientific = Literal.create(Seq(1.23e4, -5.67e-3), ArrayType(DoubleType))
    val expectedScientific = Seq("12300.0", "-0.00567")
    checkEvaluation(cast(inputScientific, ArrayType(StringType)), expectedScientific)
  }

  test("cast array element from bool to string") {
    val inputWithNull = Literal.create(Seq(true, null, false), ArrayType(BooleanType))
    val expectedWithNull = Seq("true", null, "false")
    checkEvaluation(cast(inputWithNull, ArrayType(StringType)), expectedWithNull)

    val emptyInput = Literal.create(Seq.empty[Boolean], ArrayType(BooleanType))
    val expectedEmpty = Seq.empty[String]
    checkEvaluation(cast(emptyInput, ArrayType(StringType)), expectedEmpty)
  }

  test("cast array element from date to string") {
    val inputWithNull = Literal.create(
      Seq(Date.valueOf("2024-01-01"), null, Date.valueOf("2024-01-03")),
      ArrayType(DateType)
    )
    val expectedWithNull = Seq("2024-01-01", null, "2024-01-03")
    checkEvaluation(cast(inputWithNull, ArrayType(StringType)), expectedWithNull)

    val inputLeapYear = Literal.create(
      Seq(Date.valueOf("2020-02-29")),
      ArrayType(DateType)
    )
    val expectedLeapYear = Seq("2020-02-29")
    checkEvaluation(cast(inputLeapYear, ArrayType(StringType)), expectedLeapYear)
  }

  test("cast array from timestamp to string") {
    val inputWithNull = Literal.create(
      Seq(Timestamp.valueOf("2023-01-01 12:00:00"), null, Timestamp.valueOf("2023-12-31 23:59:59")),
      ArrayType(TimestampType)
    )
    val expectedWithNull = Seq("2023-01-01 12:00:00", null, "2023-12-31 23:59:59")
    checkEvaluation(cast(inputWithNull, ArrayType(StringType)), expectedWithNull)

    val emptyInput = Literal.create(Seq.empty[Timestamp], ArrayType(TimestampType))
    val expectedEmpty = Seq.empty[String]
    checkEvaluation(cast(emptyInput, ArrayType(StringType)), expectedEmpty)
  }

  test("cast array of integer types to array of double") {
    val intArray = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val bigintArray = Literal.create(Seq(10000000000L), ArrayType(LongType))
    val smallintArray = Literal.create(Seq(1.toShort, -1.toShort), ArrayType(ShortType))
    val tinyintArray = Literal.create(Seq(1.toByte, -1.toByte), ArrayType(ByteType))

    checkEvaluation(cast(intArray, ArrayType(DoubleType)), Seq(1.0, 2.0, 3.0))
    checkEvaluation(cast(bigintArray, ArrayType(DoubleType)), Seq(1.0e10))
    checkEvaluation(cast(smallintArray, ArrayType(DoubleType)), Seq(1.0, -1.0))
    checkEvaluation(cast(tinyintArray, ArrayType(DoubleType)), Seq(1.0, -1.0))
  }

  test("cast array of double to array of integer types") {
    val doubleArray = Literal.create(Seq(1.9, -2.1), ArrayType(DoubleType))

    checkEvaluation(cast(doubleArray, ArrayType(IntegerType)), Seq(1, -2))
    checkEvaluation(cast(doubleArray, ArrayType(LongType)), Seq(1L, -2L))
    checkEvaluation(cast(doubleArray, ArrayType(ShortType)), Seq(1.toShort, -2.toShort))
    checkEvaluation(cast(doubleArray, ArrayType(ByteType)), Seq(1.toByte, -2.toByte))
  }

  test("cast array element from allowed types to string (varchar)") {
    val doubleArray = Literal.create(Seq(1.1, null, 3.3), ArrayType(DoubleType))
    val boolArray = Literal.create(Seq(true, false), ArrayType(BooleanType))
    val timestampArray = Literal.create(
      Seq(Timestamp.valueOf("2023-01-01 12:00:00"), null),
      ArrayType(TimestampType)
    )

    checkEvaluation(cast(doubleArray, ArrayType(StringType)), Seq("1.1", null, "3.3"))
    checkEvaluation(cast(boolArray, ArrayType(StringType)), Seq("true", "false"))
    checkEvaluation(cast(timestampArray, ArrayType(StringType)), Seq("2023-01-01 12:00:00", null))
  }

  test("cast array of numeric types to array of boolean") {
    val tinyintArray = Literal.create(Seq(0.toByte, 1.toByte, -1.toByte), ArrayType(ByteType))
    val smallintArray = Literal.create(Seq(0.toShort, 2.toShort, -2.toShort), ArrayType(ShortType))
    val intArray = Literal.create(Seq(0, 3, -3), ArrayType(IntegerType))
    val bigintArray = Literal.create(Seq(0L, 100L, -100L), ArrayType(LongType))
    val floatArray = Literal.create(Seq(0.0f, 1.5f, -2.3f), ArrayType(FloatType))
    val doubleArray = Literal.create(Seq(0.0, 2.5, -3.7), ArrayType(DoubleType))

    checkEvaluation(cast(tinyintArray, ArrayType(BooleanType)), Seq(false, true, true))
    checkEvaluation(cast(smallintArray, ArrayType(BooleanType)), Seq(false, true, true))
    checkEvaluation(cast(intArray, ArrayType(BooleanType)), Seq(false, true, true))
    checkEvaluation(cast(bigintArray, ArrayType(BooleanType)), Seq(false, true, true))
    checkEvaluation(cast(floatArray, ArrayType(BooleanType)), Seq(false, true, true))
    checkEvaluation(cast(doubleArray, ArrayType(BooleanType)), Seq(false, true, true))
  }

  testGluten("missing cases - from boolean") {
    (DataTypeTestUtils.numericTypeWithoutDecimal ++ Set(BooleanType)).foreach {
      t =>
        t match {
          case BooleanType =>
            checkEvaluation(cast(cast(true, BooleanType), t), true)
            checkEvaluation(cast(cast(false, BooleanType), t), false)
          case _ =>
            checkEvaluation(cast(cast(true, BooleanType), t), 1)
            checkEvaluation(cast(cast(false, BooleanType), t), 0)
        }
    }
  }

  testGluten("missing cases - from byte") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ByteType), t), 0)
        checkEvaluation(cast(cast(-1, ByteType), t), -1)
        checkEvaluation(cast(cast(1, ByteType), t), 1)
    }
  }

  testGluten("missing cases - from short") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ShortType), t), 0)
        checkEvaluation(cast(cast(-1, ShortType), t), -1)
        checkEvaluation(cast(cast(1, ShortType), t), 1)
    }
  }

  testGluten("missing cases - date self check") {
    val d = Date.valueOf("1970-01-01")
    checkEvaluation(cast(d, DateType), d)
  }

  testGluten("data type casting") {
    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = withDefaultTimeZone(UTC)(Timestamp.valueOf(nts))

    // SystemV timezones are a legacy way of specifying timezones in Unix-like OS.
    // It is not supported by Velox.
    for (tz <- ALL_TIMEZONES.filterNot(_.getId.contains("SystemV"))) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz.getId
      ) {
        val timeZoneId = Option(tz.getId)
        var c = Calendar.getInstance(TimeZoneUTC)
        c.set(2015, 2, 8, 2, 30, 0)
        checkEvaluation(
          cast(
            cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
            TimestampType,
            timeZoneId),
          millisToMicros(c.getTimeInMillis))
        c = Calendar.getInstance(TimeZoneUTC)
        c.set(2015, 10, 1, 2, 30, 0)
        checkEvaluation(
          cast(
            cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
            TimestampType,
            timeZoneId),
          millisToMicros(c.getTimeInMillis))
      }
    }

    checkEvaluation(cast("abdef", StringType), "abdef")
    checkEvaluation(cast("12.65", DecimalType.SYSTEM_DEFAULT), Decimal(12.65))

    checkEvaluation(cast(cast(sd, DateType), StringType), sd)
    checkEvaluation(cast(cast(d, StringType), DateType), 0)

    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC_OPT.get
    ) {
      checkEvaluation(cast(cast(nts, TimestampType, UTC_OPT), StringType, UTC_OPT), nts)
      checkEvaluation(
        cast(cast(ts, StringType, UTC_OPT), TimestampType, UTC_OPT),
        fromJavaTimestamp(ts))

      // all convert to string type to check
      checkEvaluation(
        cast(cast(cast(nts, TimestampType, UTC_OPT), DateType, UTC_OPT), StringType),
        sd)
      checkEvaluation(
        cast(cast(cast(ts, DateType, UTC_OPT), TimestampType, UTC_OPT), StringType, UTC_OPT),
        zts)
    }

    checkEvaluation(cast(cast("abdef", BinaryType), StringType), "abdef")

    checkEvaluation(
      cast(
        cast(cast(cast(cast(cast("5", ByteType), ShortType), IntegerType), FloatType), DoubleType),
        LongType),
      5.toLong)

    checkEvaluation(cast("23", DoubleType), 23d)
    checkEvaluation(cast("23", IntegerType), 23)
    checkEvaluation(cast("23", FloatType), 23f)
    checkEvaluation(cast("23", DecimalType.USER_DEFAULT), Decimal(23))
    checkEvaluation(cast("23", ByteType), 23.toByte)
    checkEvaluation(cast("23", ShortType), 23.toShort)
    checkEvaluation(cast(123, IntegerType), 123)

    checkEvaluation(cast(Literal.create(null, IntegerType), ShortType), null)
  }

  test("cast from boolean to timestamp") {
    val tsTrue = new Timestamp(0)
    tsTrue.setNanos(1000)

    val tsFalse = new Timestamp(0)

    checkEvaluation(cast(true, TimestampType), tsTrue)

    checkEvaluation(cast(false, TimestampType), tsFalse)
  }

  testGluten("cast string to timestamp") {
    ThreadUtils.parmap(
      ALL_TIMEZONES
        .filterNot(_.getId.contains("SystemV"))
        .filterNot(_.getId.contains("Europe/Kyiv"))
        .filterNot(_.getId.contains("America/Ciudad_Juarez"))
        .filterNot(_.getId.contains("Antarctica/Vostok"))
        .filterNot(_.getId.contains("Pacific/Kanton")),
      prefix = "CastSuiteBase-cast-string-to-timestamp",
      maxThreads = 1
    ) {
      zid =>
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
        ) {
          def checkCastStringToTimestamp(str: String, expected: Timestamp): Unit = {
            checkEvaluation(cast(Literal(str), TimestampType, Option(zid.getId)), expected)
          }

          val tz = TimeZone.getTimeZone(zid)
          var c = Calendar.getInstance(tz)
          c.set(2015, 0, 1, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015", new Timestamp(c.getTimeInMillis))
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 1, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03", new Timestamp(c.getTimeInMillis))
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 ", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18 12:03:17", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17", new Timestamp(c.getTimeInMillis))

          // If the string value includes timezone string, it represents the timestamp string
          // in the timezone regardless of the timeZoneId parameter.
          c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17Z", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 12:03:17Z", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          // Unsupported timezone format for Velox backend.
          // checkCastStringToTimestamp("2015-03-18T12:03:17-1:0", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17-01:00", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17+07:30", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          // Unsupported timezone format for Velox backend.
          // checkCastStringToTimestamp("2015-03-18T12:03:17+7:3",
          // new Timestamp(c.getTimeInMillis))

          // tests for the string including milliseconds.
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp("2015-03-18 12:03:17.123", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17.123", new Timestamp(c.getTimeInMillis))

          // If the string value includes timezone string, it represents the timestamp string
          // in the timezone regardless of the timeZoneId parameter.
          c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 456)
          checkCastStringToTimestamp("2015-03-18T12:03:17.456Z", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 12:03:17.456Z", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          // Unsupported timezone format for Velox backend.
          // checkCastStringToTimestamp("2015-03-18T12:03:17.123-1:0",
          // new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123-01:00",
            new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123+07:30",
            new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          // Unsupported timezone format for Velox backend.
          // checkCastStringToTimestamp("2015-03-18T12:03:17.123+7:3",
          // new Timestamp(c.getTimeInMillis))
        }
    }
  }

  testGluten("cast decimal to timestamp") {
    val tz = TimeZone.getTimeZone(TimeZone.getDefault.getID)
    val c = Calendar.getInstance(tz)
    c.set(2015, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 123)
    val d = Decimal(c.getTimeInMillis.toDouble / 1000)
    checkEvaluation(cast(d, TimestampType), new Timestamp(c.getTimeInMillis))
  }

}
