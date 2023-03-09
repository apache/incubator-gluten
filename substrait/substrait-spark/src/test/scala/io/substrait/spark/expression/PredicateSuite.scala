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
import org.apache.spark.unsafe.types.UTF8String

class PredicateSuite extends SparkFunSuite with SubstraitExpressionTestBase {

  test("And") {
    runTest("and:bool", And(Literal(true), Literal(false)))
  }

  test("inset") {
    val inSet = InSet(Literal(1), Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    runTest("", inSet)

    val inSetString = InSet(
      Literal("a"),
      Set("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "x").map(UTF8String.fromString))
    runTest("", inSetString)
  }
}
