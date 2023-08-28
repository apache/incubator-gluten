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
import org.apache.spark.sql.types.{Decimal, DecimalType}

class GlutenDecimalExpressionSuite extends DecimalExpressionSuite with GlutenTestsTrait {
  // Replace CheckOverflow with Cast to test the decimal precision change.
  // nullOnOverflow is true because ANSI mode is not supported in expressions.
  test("change decimal precision by cast") {
    val d1 = Decimal("10.1")
    checkEvaluation(Cast(Literal(d1), DecimalType(4, 0)), Decimal("10"))
    checkEvaluation(Cast(Literal(d1), DecimalType(4, 1)), d1)
    checkEvaluation(Cast(Literal(d1), DecimalType(4, 2)), d1)
    checkEvaluation(Cast(Literal(d1), DecimalType(4, 3)), null)

    val d2 = Decimal(101, 3, 1)
    checkEvaluation(Cast(Literal(d2), DecimalType(4, 0)), Decimal("10"))
    checkEvaluation(Cast(Literal(d2), DecimalType(4, 1)), d2)
    checkEvaluation(Cast(Literal(d2), DecimalType(4, 2)), d2)
    checkEvaluation(Cast(Literal(d2), DecimalType(4, 3)), null)

    checkEvaluation(Cast(Literal.create(null, DecimalType(2, 1)), DecimalType(3, 2)), null)
    checkEvaluation(Cast(Literal.create(null, DecimalType(2, 1)), DecimalType(3, 2)), null)
  }
}
