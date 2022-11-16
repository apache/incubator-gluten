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

package io.glutenproject.utils.velox

import io.glutenproject.utils.NotSupport
import org.apache.spark.sql.DateFunctionsSuite
import org.apache.spark.sql.StringFunctionsSuite
import org.apache.spark.sql.catalyst.expressions._

object VeloxNotSupport extends NotSupport {

  override lazy val partialSupportSuiteList: Map[String, Seq[String]] = Map.empty

  override lazy val fullSupportSuiteList: Set[String] = Set(
    simpleClassName[LiteralExpressionSuite],
    simpleClassName[IntervalExpressionsSuite],
    simpleClassName[HashExpressionsSuite],
    simpleClassName[DateExpressionsSuite],
    simpleClassName[DecimalExpressionSuite],
    simpleClassName[StringFunctionsSuite],
    simpleClassName[RegexpExpressionsSuite],
    simpleClassName[PredicateSuite]
  )
}
