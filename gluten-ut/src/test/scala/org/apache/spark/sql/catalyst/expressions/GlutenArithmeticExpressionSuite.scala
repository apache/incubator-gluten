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

import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import org.apache.spark.{SparkArithmeticException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


class GlutenArithmeticExpressionSuite extends ArithmeticExpressionSuite with GlutenTestsTrait {

//  override def whiteTestNameList: Seq[String] = Seq("function least", "function greatest")

//  override def blackTestNameList: Seq[String] = Seq("function least", "function greatest")

  test("function least1") {
/**    val row = create_row(1, 2, "a", "b", "c")
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.string.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    checkEvaluation(Least(Seq(c4, c3, c5)), "a", row)
    checkEvaluation(Least(Seq(c1, c2)), 1, row)
    checkEvaluation(Least(Seq(c1, c2, Literal(-1))), -1, row)
    checkEvaluation(Least(Seq(c4, c5, c3, c3, Literal("a"))), "a", row)

    val nullLiteral = Literal.create(null, IntegerType)
    checkEvaluation(Least(Seq(nullLiteral, nullLiteral)), null)
    checkEvaluation(Least(Seq(Literal(null), Literal(null))), null, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(-1.0), Literal(2.5))), -1.0, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(-1), Literal(2))), -1)
    checkEvaluation(
      Least(Seq(Literal((-1.0).toFloat), Literal(2.5.toFloat))), (-1.0).toFloat)
    checkEvaluation(
      Least(Seq(Literal(Long.MaxValue), Literal(Long.MinValue))), Long.MinValue)
    checkEvaluation(Least(Seq(Literal(1.toByte), Literal(2.toByte))), 1.toByte)
    checkEvaluation(
      Least(Seq(Literal(1.toShort), Literal(2.toByte.toShort))), 1.toShort)
    checkEvaluation(Least(Seq(Literal("abc"), Literal("aaaa"))), "aaaa")
    checkEvaluation(Least(Seq(Literal(true), Literal(false))), false)
    checkEvaluation(
      Least(Seq(
        Literal(BigDecimal("1234567890987654321123456")),
        Literal(BigDecimal("1234567890987654321123458")))),
      BigDecimal("1234567890987654321123456"))
    **/
    checkEvaluation(
      Least(Seq(Literal(Date.valueOf("2015-01-01")), Literal(Date.valueOf("2015-07-01")))),
      Date.valueOf("2015-01-01"))
//    checkEvaluation(
//      Least(Seq(
//        Literal(Timestamp.valueOf("2015-07-01 08:00:00")),
//        Literal(Timestamp.valueOf("2015-07-01 10:00:00")))),
//      Timestamp.valueOf("2015-07-01 08:00:00"))
//
//    // Type checking error
//    assert(
//      Least(Seq(Literal(1), Literal("1"))).checkInputDataTypes() ==
//          TypeCheckFailure("The expressions should all have the same type, " +
//              "got LEAST(int, string)."))
//
//    DataTypeTestUtils.ordered.foreach { dt =>
//      checkConsistencyBetweenInterpretedAndCodegen(Least, dt, 2)
//    }
//
//    val least = Least(Seq(
//      Literal.create(Seq(1, 2), ArrayType(IntegerType, containsNull = false)),
//      Literal.create(Seq(1, 3, null), ArrayType(IntegerType, containsNull = true))))
//    assert(least.dataType === ArrayType(IntegerType, containsNull = true))
//    checkEvaluation(least, Seq(1, 2))
  }
}
