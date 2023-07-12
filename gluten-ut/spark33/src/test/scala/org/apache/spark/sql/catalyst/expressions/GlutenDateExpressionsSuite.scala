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

import org.apache.spark.sql.{GlutenTestConstants, GlutenTestsTrait}
import org.apache.spark.sql.types.IntegerType

class GlutenDateExpressionsSuite extends DateExpressionsSuite with GlutenTestsTrait {
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
}
