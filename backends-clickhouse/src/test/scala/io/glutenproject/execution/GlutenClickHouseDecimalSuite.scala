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
package io.glutenproject.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import java.util
case class DataTypesWithNonPrimitiveType(
    string_field: String,
    int_field: java.lang.Integer,
    decimal_field: java.math.BigDecimal
)

class GlutenClickHouseDecimalSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = super.sparkConf

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .createDataFrame(genTestData())
      .createTempView(decimalTable)
  }

  private val decimalTable: String = "decimal_table"

  test("fix decimal precision overflow") {
    val sql =
      s"""
         | select
         |     cast(int_field  as decimal(20, 2))
         |         * cast(decimal_field as decimal(30, 2)) * decimal_field
         | from $decimalTable
         | limit 1
         |""".stripMargin
    withSQLConf(vanillaSparkConfs(): _*) {
      val df2 = spark.sql(sql)
      print(df2.queryExecution.executedPlan)
    }
    testFromRandomBase(
      sql,
      _ => {}
    )
  }

  test("fix decimal32 with negative value") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("decimal32_field", DecimalType.apply(8, 4), nullable = true)
      ))
    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(new java.math.BigDecimal(1.123)))
    dataCorrect.add(Row(new java.math.BigDecimal(-2.123)))
    dataCorrect.add(Row(new java.math.BigDecimal(-3.123)))
    dataCorrect.add(Row(null))
    spark.createDataFrame(dataCorrect, schema).createTempView("decimal32_table")

    val sql_nullable =
      s"""
         | select
         |     *
         | from decimal32_table
         |""".stripMargin

    val sql_not_null =
      s"""
         | select
         |     *
         | from decimal32_table
         | where decimal32_field < 0
         |""".stripMargin

    compareResultsAgainstVanillaSpark(sql_nullable, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(sql_not_null, compareResult = true, _ => {})
  }

  def testFromRandomBase(
      sql: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true
  ): Unit = {
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      customCheck,
      noFallBack = noFallBack)
  }

  def genTestData(): Seq[DataTypesWithNonPrimitiveType] = {
    (0 to 300).map {
      i =>
        if (i % 100 == 1) {
          // scalastyle:off nonascii
          DataTypesWithNonPrimitiveType(
            "测试中文",
            Integer.MAX_VALUE,
            new java.math.BigDecimal(Integer.MAX_VALUE + ".56"))
          // scalastyle:on nonascii
        } else if (i % 10 == 0) {
          DataTypesWithNonPrimitiveType(
            s"${i / 1000}",
            Integer.MAX_VALUE,
            new java.math.BigDecimal(i + ".56"))
        } else if (i % 50 == 0) {
          DataTypesWithNonPrimitiveType(null, null, null)
        } else {
          DataTypesWithNonPrimitiveType(s"${i / 1000}", i, new java.math.BigDecimal(i + ".56"))
        }
    }
  }
}
