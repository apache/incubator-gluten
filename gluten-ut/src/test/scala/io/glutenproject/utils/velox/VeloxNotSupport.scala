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

import org.apache.spark.sql.{MathFunctionsSuite, DataFrameAggregateSuite, GlutenDataFrameAggregateSuite, StringFunctionsSuite}
import org.apache.spark.sql.catalyst.expressions._

object VeloxNotSupport extends NotSupport {

  override lazy val partialSupportSuiteList: Map[String, Seq[String]] = Map(
    simpleClassName[CastSuite] -> Seq(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    ),
    simpleClassName[DataFrameAggregateSuite] -> Seq(
      "zero moments", // [velox does not return NaN]
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      "rollup overlapping columns", // wait velox to fix
      "cube overlapping columns", // wait velox to fix
      // Type YearMonthIntervalType(0,1) not supported
      "SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`",
      "SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`",
      // numCols=0, empty intermediate batch
      "count",
      "SPARK-38185: Fix data incorrect if aggregate function is empty",
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      "SPARK-32136: NormalizeFloatingNumbers should work on null struct" // integer overflow
    )
  )

  override lazy val fullSupportSuiteList: Set[String] = Set(
    simpleClassName[LiteralExpressionSuite],
    simpleClassName[IntervalExpressionsSuite],
    simpleClassName[HashExpressionsSuite],
    simpleClassName[CollectionExpressionsSuite],
    simpleClassName[DateExpressionsSuite],
    simpleClassName[DecimalExpressionSuite],
    simpleClassName[StringFunctionsSuite],
    simpleClassName[RegexpExpressionsSuite],
    simpleClassName[PredicateSuite],
    simpleClassName[MathExpressionsSuite],
    simpleClassName[MathFunctionsSuite],
    simpleClassName[SortOrderExpressionsSuite],
    simpleClassName[BitwiseExpressionsSuite],
    simpleClassName[StringExpressionsSuite],
    simpleClassName[MiscExpressionsSuite],
    simpleClassName[NondeterministicSuite],
    simpleClassName[RandomSuite],
    simpleClassName[ArithmeticExpressionSuite],
    simpleClassName[ConditionalExpressionSuite],
    simpleClassName[GlutenDataFrameAggregateSuite]
  )

}
