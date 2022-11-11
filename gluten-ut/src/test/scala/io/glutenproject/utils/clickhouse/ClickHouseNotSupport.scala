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

package io.glutenproject.utils.clickhouse

import io.glutenproject.utils.NotSupport

import org.apache.spark.sql.catalyst.expressions._

object ClickHouseNotSupport extends NotSupport {

  override lazy val partialSupportSuiteList: Map[String, Seq[String]] = Map(
    "DataFrameAggregateSuite" -> Seq(
      "average", // [overwritten by Gluten - xxx]
      "groupBy", // [overwritten by Gluten - xxx]
      "count", // [overwritten by Gluten - xxx]
      "null count", // [overwritten by Gluten - xxx]
      "multiple column distinct count", // [overwritten by Gluten - xxx]
      "agg without groups and functions", // [not urgent]
      "zero moments", // [not urgent]
      "collect functions", // [not urgent]
      "collect functions structs", // [not urgent]
      "SPARK-31500: collect_set() of BinaryType returns duplicate elements", // [not urgent]
      "SPARK-17641: collect functions should not collect null values", // [not urgent]
      "collect functions should be able to cast to array type with no null values", // [not urgent]
      "SPARK-14664: Decimal sum/avg over window should work.", // [wishlist] support decimal
      "SQL decimal test (used for catching certain decimal " +
        "handling bugs in aggregates)", // [wishlist] support decimal
      "SPARK-17616: distinct aggregate combined with a non-partial aggregate", // [not urgent]
      "SPARK-17237 remove backticks in a pivot result schema", // [not urgent]
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
        " before using it", // [not urgent]
      "SPARK-22951", // [not urgent] dropDuplicates
      "SPARK-26021", // [not urgent] behavior on NaN and -0.0 are different
      "max_by", // [not urgent]
      "min_by", // [not urgent]
      "count_if", // [not urgent]
      "SPARK-31620", // [not urgent] sum_if
      "SPARK-32136", // [not urgent] struct type
      "SPARK-32344", // [not urgent] FIRST/LAST
      "SPARK-34713", // [not urgent] struct type
      "SPARK-38221", // [not urgent] struct type
      "SPARK-34716", // [not urgent] interval
      "SPARK-34837", // [not urgent] interval
      "SPARK-35412", // [not urgent] interval
      "SPARK-36926", // [wishlist] support decimal
      "SPARK-38185", // [not urgent] empty agg
      "SPARK-18952", // [not urgent]
      "SPARK-32038" // [not urgent]
    ))
  override lazy val fullSupportSuiteList: Set[String] = Set(
    "ComplexTypesSuite"
  )
}
