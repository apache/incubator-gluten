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

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.{GlutenTestConstants, GlutenTestsTrait}

class GlutenDateExpressionsSuite extends DateExpressionsSuite with GlutenTestsTrait {

  override protected def checkEvaluation(expression: => Expression,
                                         expected: Any,
                                         inputRow: InternalRow = EmptyRow): Unit = {
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)

    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    // Consistent with the evaluation vanilla spark UT to avoid overflow issue in
    // resultDF.collect() for some corner cases.
    glutenCheckExpression(expr, catalystValue, inputRow, true)
  }
}
