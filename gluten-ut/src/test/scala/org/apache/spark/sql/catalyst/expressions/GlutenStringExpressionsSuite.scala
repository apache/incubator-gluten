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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.IntegerType

class GlutenStringExpressionsSuite extends StringExpressionsSuite with GlutenTestsTrait {

  test(GlutenTestConstants.GLUTEN_TEST + "Substring") {
    val row = create_row("example", 1, 1L, 1.0, true, null, false)

    val s = 'a.string.at(0)

    // substring from zero position with less-than-full length
    // can not support 0 pos
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)), "ex", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)), "ex", row)

    // substring from zero position with full length
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(7, IntegerType)), "example", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(7, IntegerType)), "example", row)

    // substring from zero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(100, IntegerType)),
      "example", row)
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(100, IntegerType)),
      "example", row)

    // substring from nonzero position with less-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(2, IntegerType)),
      "xa", row)

    // substring from nonzero position with full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(6, IntegerType)),
      "xample", row)

    // substring from nonzero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(100, IntegerType)),
      "xample", row)

    // zero-length substring (within string bounds)
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(0, IntegerType)),
      "", row)

    // zero-length substring (beyond string bounds)
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      "", row)

    /*
    // substring(null, _, _) -> null
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      null, create_row(null))

    // substring(_, null, _) -> null
    checkEvaluation(Substring(s, Literal.create(null, IntegerType), Literal.create(4, IntegerType)),
      null, row)

    // substring(_, _, null) -> null
    checkEvaluation(
      Substring(s, Literal.create(100, IntegerType), Literal.create(null, IntegerType)),
      null,
      row) */

    // 2-arg substring from zero position
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)

    // 2-arg substring from nonzero position
    checkEvaluation(
      Substring(s, Literal.create(2, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "xample",
      row)

    // Substring with from negative position with negative length
    checkEvaluation(Substring(s, Literal.create(-1207959552, IntegerType),
      Literal.create(-1207959552, IntegerType)), "", row)

    val s_notNull = 'a.string.notNull.at(0)

    assert(Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)).nullable)
    assert(
      Substring(s_notNull, Literal.create(1, IntegerType), Literal.create(2, IntegerType)).nullable
        === false)
    assert(Substring(s_notNull,
      Literal.create(null, IntegerType), Literal.create(2, IntegerType)).nullable)
    assert(Substring(s_notNull,
      Literal.create(0, IntegerType), Literal.create(null, IntegerType)).nullable)

    checkEvaluation(s.substr(1, 2), "ex", row)
    checkEvaluation(s.substr(1), "example", row)
    checkEvaluation(s.substring(1, 2), "ex", row)
    checkEvaluation(s.substring(1), "example", row)

    /* val bytes = Array[Byte](1, 2, 3, 4)
    checkEvaluation(Substring(bytes, 0, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 1, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 2, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, 3, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, 4, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, 8, 2), Array.empty[Byte])
    checkEvaluation(Substring(bytes, -1, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, -2, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, -3, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, -4, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, -5, 2), Array[Byte](1))
    checkEvaluation(Substring(bytes, -8, 2), Array.empty[Byte]) */
  }
}
