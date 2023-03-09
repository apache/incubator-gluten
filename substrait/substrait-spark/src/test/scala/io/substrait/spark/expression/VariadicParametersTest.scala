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

class VariadicParametersTest extends SparkFunSuite with SubstraitExpressionTestBase {
  test("concat") {
    // val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val concat = Concat(Seq(Literal("a"), Literal("b"), Literal("c")))
    runTest("concat:str", concat)
  }

  test("coalesce") {
    val coalesce = Coalesce(Seq(Literal(null, IntegerType), Literal(3), Literal(2)))
    runTest("coalesce:any", coalesce)

    val Zero = (new Decimal).set(0, 12, 2)
    val coalesceDecimal =
      Coalesce(Seq(Cast(Literal(1), DecimalType(12, 2)), Literal(Zero)))
    runTest("coalesce:any", coalesceDecimal)
  }
}
