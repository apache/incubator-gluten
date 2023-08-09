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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

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

  protected val orcDataPath: String = rootPath + "orc-data"
  protected val csvDataPath: String = rootPath + "csv-data"

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

  test("read data from csv file format") {
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
      df => {
        val a = df.collect()
        print(a)
      }
    )
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
