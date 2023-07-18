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
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{JST, PST, UTC, UTC_OPT}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{GlutenTestConstants, GlutenTestsTrait}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

//import java.time.ZoneId

class GlutenDateExpressionsSuite extends DateExpressionsSuite with GlutenTestsTrait {
  private val PST_OPT = Option(PST.getId)
  private val JST_OPT = Option(JST.getId)

  override def testIntegralInput(testFunc: Number => Unit): Unit = {
    def checkResult(input: Long): Unit = {
      if (input.toByte == input) {
        testFunc(input.toByte)
      } else if (input.toShort == input) {
        testFunc(input.toShort)
      } else if (input.toInt == input) {
        testFunc(input.toInt)
      } else {
        testFunc(input)
      }
    }

    checkResult(0)
    checkResult(Byte.MaxValue)
    checkResult(Byte.MinValue)
    checkResult(Short.MaxValue)
    checkResult(Short.MinValue)
    // Spark collect causes integer overflow.
    // checkResult(Int.MaxValue)
    // checkResult(Int.MinValue)
    // checkResult(Int.MaxValue.toLong + 100)
    // checkResult(Int.MinValue.toLong - 100)
  }

  test(GlutenTestConstants.GLUTEN_TEST + "TIMESTAMP_MICROS") {
    def testIntegralFunc(value: Number): Unit = {
      checkEvaluation(
        MicrosToTimestamp(Literal(value)),
        value.longValue())
    }

    // test null input
    checkEvaluation(
      MicrosToTimestamp(Literal(null, IntegerType)),
      null)

    // test integral input
    testIntegralInput(testIntegralFunc)
    // test max/min input
    // Spark collect causes long overflow.
    // testIntegralFunc(Long.MaxValue)
    // testIntegralFunc(Long.MinValue)
  }

  // gluten only support set timezone with config
  test("DateFormat adapted") {
    Seq("legacy", "corrected").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
        checkEvaluation(
          DateFormatClass(Literal.create(null, TimestampType), Literal("y"), UTC_OPT),
          null)
        checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, UTC_OPT),
          Literal.create(null, StringType), UTC_OPT), null)

        //Seq((UTC.getId, "13"), (PST.getId, "5"), (JST.getId, "22")).foreach {case (timezone: String, expected_hour: String) =>
        Seq((UTC.getId, "13"), (JST.getId, "22")).foreach { case (timezone: String, expected_hour: String) =>
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
            checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, Option(timezone)),
              Literal("y")), "2015")
            checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
            checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, Option(timezone)),
              Literal("H")), "0")
            checkEvaluation(DateFormatClass(Literal(ts), Literal("H")), expected_hour)
          }
        }

        // Test escaping of format
        GenerateUnsafeProjection.generate(
          DateFormatClass(Literal(ts), Literal("\""), JST_OPT) :: Nil)

        // SPARK-28072 The codegen path should work
        checkEvaluation(
          expression = DateFormatClass(
            BoundReference(ordinal = 0, dataType = TimestampType, nullable = true),
            BoundReference(ordinal = 1, dataType = StringType, nullable = true),
            JST_OPT),
          expected = "22",
          inputRow = InternalRow(DateTimeUtils.fromJavaTimestamp(ts), UTF8String.fromString("H")))
      }
    }
  }

  test("DateFormat timezone hour adapted") {
    Seq("legacy", "corrected").foreach { legacyParserPolicy =>
      //Seq((UTC.getId, "13"), (PST.getId, "5"), (JST.getId, "22")).foreach {case (timezone: String, expected_hour: String) =>
      Seq(("Asia/Shanghai", "21"), (JST.getId, "22")).foreach { case (timezone: String, expected_hour: String) =>
        withSQLConf(
          (SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy),
          (SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone)) {
          checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, Option(timezone)),
            Literal("y")), "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
          checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, Option(timezone)),
            Literal("H")), "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), Option(timezone)), expected_hour)
        }
      }
    }
  }
}
