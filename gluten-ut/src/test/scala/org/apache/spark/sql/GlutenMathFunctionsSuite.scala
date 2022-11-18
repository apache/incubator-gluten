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

package org.apache.spark.sql

import io.glutenproject.utils.SystemParameters
import io.glutenproject.GlutenConfig

class GlutenMathFunctionsSuite extends MathFunctionsSuite with GlutenSQLTestsTrait {
  override def whiteTestNameList: Seq[String] = Seq(
    // "round/bround", // Scale argument of round/bround function currently don't support negative.
    // "radians",      // Relies on the transformation of function `CheckOverflow`.
    // "degrees",      // Relies on the transformation of function `CheckOverflow`.
    // "hex",          // Leading 0 is cut in different ways between CH and Spark.
    // "log1p",        // In CH log1p(1) returns -inf, in spark it returns null.
    // "rint",         // Relies on the right transformation of function `cast` when null is input
    "cos",
    "cosh",
    "sin",
    "sinh",
    "tan",
    "tanh",
    "acos",
    "asin",
    "atan",
    "atan2",
    "cbrt",
    "unhex",
    "hypot",
    "log10",
    "log2",
    "log / ln"
  )

  override def blackTestNameList: Seq[String] = Seq()
}
