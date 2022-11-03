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
package io.substrait.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._

import io.substrait.expression.{Expression => SExpression}

class ArithmeticExpressionSuite extends SparkFunSuite {

  test("+ (Add)") {
    val e = Add(Literal(1L), Literal(2L))
    val pexp = ExpressionConverter.defaultConverter
      .convert(e)
      .map(_.asInstanceOf[SExpression.ScalarFunctionInvocation])
    assert(pexp.isDefined)
    assertResult(Some("add:opt_i64_i64"))(pexp.map(_.declaration().key()))

  }
}
