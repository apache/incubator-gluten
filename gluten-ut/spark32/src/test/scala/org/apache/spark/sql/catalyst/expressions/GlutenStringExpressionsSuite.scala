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

class GlutenStringExpressionsSuite extends StringExpressionsSuite with GlutenTestsTrait {

  // Ported from spark 3.3.1, applicable to spark 3.2.3 or higher.
  testGluten("SPARK-40213: ascii for Latin-1 Supplement characters") {
    // scalastyle:off
    checkEvaluation(Ascii(Literal("¥")), 165, create_row("¥"))
    checkEvaluation(Ascii(Literal("®")), 174, create_row("®"))
    checkEvaluation(Ascii(Literal("©")), 169, create_row("©"))
    // scalastyle:on
    (128 until 256).foreach {
      c => checkEvaluation(Ascii(Chr(Literal(c.toLong))), c, create_row(c.toLong))
    }
  }
}
