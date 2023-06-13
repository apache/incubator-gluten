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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

class GlutenDataFramePivotSuite extends DataFramePivotSuite with GlutenSQLTestsTrait {
  import testImplicits._

  override def testNameBlackList: Seq[String] = Seq(
    // Replaced with the below test.
    "pivot with column definition in groupby"
  )

  // This test is ported from vanilla spark with pos value (1-based) changed from 0 to 1 for
  // substring. In vanilla spark, pos=0 has same effectiveness as pos=1. But in velox, pos=0
  // will return an empty string as substring result.
  test("pivot with column definition in groupby - using pos=1") {
    val df = courseSales.groupBy(substring(col("course"), 1, 1).as("foo"))
        .pivot("year", Seq(2012, 2013))
        .sum("earnings").queryExecution.executedPlan

    checkAnswer(
      courseSales.groupBy(substring(col("course"), 1, 1).as("foo"))
          .pivot("year", Seq(2012, 2013))
          .sum("earnings"),
      Row("d", 15000.0, 48000.0) :: Row("J", 20000.0, 30000.0) :: Nil
    )
  }

  test("gluten optimized pivot DecimalType") {
    val df = courseSales.select($"course", $"year", $"earnings".cast(DecimalType(10, 2)))
      .groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
      .select("year", "dotNET", "Java")
    df.printSchema()

        assertResult(IntegerType)(df.schema("year").dataType)
        assertResult(DecimalType(20, 2))(df.schema("Java").dataType)
        assertResult(DecimalType(20, 2))(df.schema("dotNET").dataType)

        checkAnswer(df, Row(2012, BigDecimal(1500000, 2), BigDecimal(2000000, 2)) ::
          Row(2013, BigDecimal(4800000, 2), BigDecimal(3000000, 2)) :: Nil)
  }

  test("gluten optimized pivot DecimalType 2") {
    val df = courseSales.select($"course", $"year", $"earnings".cast(DecimalType(10, 2)))
      .groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
      .select("Java")
    df.printSchema()

//    assertResult(IntegerType)(df.schema("year").dataType)
    assertResult(DecimalType(20, 2))(df.schema("Java").dataType)
//    assertResult(DecimalType(20, 2))(df.schema("dotNET").dataType)

    checkAnswer(df, Row(2012, BigDecimal(1500000, 2), BigDecimal(2000000, 2)) ::
      Row(2013, BigDecimal(4800000, 2), BigDecimal(3000000, 2)) :: Nil)
  }
}
