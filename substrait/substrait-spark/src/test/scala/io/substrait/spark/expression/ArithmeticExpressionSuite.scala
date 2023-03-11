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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}

class ArithmeticExpressionSuite extends SparkFunSuite with SubstraitExpressionTestBase {

  test("+ (Add)") {
    runTest(
      "add:i64_i64",
      Add(Literal(1), Literal(2L)),
      func => {
        assertResult(true)(func.arguments().get(1).isInstanceOf[SExpression.I64Literal])
        assertResult(ExpressionCreator.cast(Type.REQUIRED.I64, ExpressionCreator.i32(false, 1)))(
          func.arguments().get(0))
      },
      bidirectional = false
    ) // TODO: implicit calcite cast

    runTest(
      "add:i64_i64",
      Add(Cast(Literal(1), LongType), Literal(2L)),
      func => {},
      bidirectional = true)

    runTest("add:i32_i32", Add(Literal(1), Cast(Literal(2L), IntegerType)))

    runTest(
      "add:i32_i32",
      Add(Literal(1), Literal(2)),
      func => {
        assertResult(true)(func.arguments().get(0).isInstanceOf[SExpression.I32Literal])
        assertResult(true)(func.arguments().get(1).isInstanceOf[SExpression.I32Literal])
      },
      bidirectional = true
    )
  }

  test("MakeDecimal") {
    val makeDecimal = MakeDecimal(Literal(1000000L), 10, 0)
    runTest(
      "make_decimal:i64_i32_i32_bool",
      makeDecimal,
      func => {},
      bidirectional = true
    )
  }

  test("round") {
    val roundDec = Round(Literal(BigDecimal.decimal(12.1111)), Literal(2))
    runTest("round:dec_i32", roundDec)

    val roundFloat = Round(Literal(12.1111f), Literal(2))
    runTest("round:fp32_i32", roundFloat)
  }

  test("check_overflow") {
    val check = CheckOverflow(
      Literal(Literal(BigDecimal.decimal(12.1111))),
      DecimalType(10, 2),
      nullOnOverflow = false)
    runTest("check_overflow:dec_type_bool", check)
  }
}
