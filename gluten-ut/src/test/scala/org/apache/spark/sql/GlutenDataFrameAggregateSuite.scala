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

import org.apache.spark.sql.functions.{count, sum}

class GlutenDataFrameAggregateSuite extends DataFrameAggregateSuite with GlutenSQLTestsTrait {

  import testImplicits._

  override def blackTestNameList: Seq[String] = Seq(
    GlutenTestConstants.IGNORE_ALL
  )

  test(GlutenTestConstants.GLUTEN_TEST + "groupBy") {
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum($"totB")),
      Row(9)
    )
    checkAnswer(
      testData2.groupBy("a").agg(count("*")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("*" -> "count")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil
    )

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(
      df1.groupBy("key").min(),
      df1.groupBy("key").min("value1", "value2").collect()
    )
    checkAnswer(
      df1.groupBy("key").min("value2"),
      Seq(Row("a", 0), Row("b", 4))
    )

    // can not support decimal data type
    /* checkAnswer(
      decimalData.groupBy("a").agg(sum("b")),
      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)))
    )

    val decimalDataWithNulls = spark.sparkContext.parallelize(
      DecimalData(1, 1) ::
        DecimalData(1, null) ::
        DecimalData(2, 1) ::
        DecimalData(2, null) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) ::
        DecimalData(null, 2) :: Nil).toDF()
    checkAnswer(
      decimalDataWithNulls.groupBy("a").agg(sum("b")),
      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(1)),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(1)),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)),
        Row(null, new java.math.BigDecimal(2)))
    ) */
  }
}
