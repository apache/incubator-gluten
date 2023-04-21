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
import org.apache.spark.sql.execution.{FileSourceScanExec, LocalTableScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.types.{StringType, StructType}

import java.util.Date

import scala.language.implicitConversions

case class AllDataTypesWithNonPrimitiveType(
    string_field: String,
    int_field: java.lang.Integer,
    long_field: java.lang.Long,
    float_field: java.lang.Float,
    double_field: java.lang.Double,
    short_field: java.lang.Short,
    byte_field: java.lang.Byte,
    boolean_field: java.lang.Boolean,
    decimal_field: java.math.BigDecimal,
    date_field: java.sql.Date
    // TODO: support below data types
    // array: Seq[Int],
    // arrayContainsNull: Seq[Option[Int]],
    // map: Map[Int, Long],
    // mapValueContainsNull: Map[Int, Option[Long]],
    // data: (Seq[Int], (Int, String))
)

class GlutenClickHouseFileFormatSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  protected val orcDataPath = rootPath + "orc-data"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
  }

  test("read data from csv file format") {
    val filePath = basePath + "/csv_test.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select *
         | from $csvFileFormat.`$filePath`
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        assert(df.queryExecution.executedPlan.isInstanceOf[FileSourceScanExec])
      })
  }

  test("read data from csv file format with filter") {
    val filePath = basePath + "/csv_test_filter.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select *
         | from $csvFileFormat.`$filePath`
         | where _c1 > 30
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExec if f.relation.fileFormat.isInstanceOf[CSVFileFormat] => f
        }
        assert(csvFileScan.size == 1)
      }
    )
  }

  test("read data from csv file format witsh agg") {
    val filePath = basePath + "/csv_test_agg.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select _c7, sum(_c1), avg(_c2), min(_c3), max(_c4), sum(_c5), sum(_c8)
         | from $csvFileFormat.`$filePath`
         | group by _c7
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExec if f.relation.fileFormat.isInstanceOf[CSVFileFormat] => f
        }
        assert(csvFileScan.size == 1)
      }
    )
  }

  ignore("read data from csv file format with table") {
    val tableName = "csv_test"
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $tableName
         | group by string_field
         | order by string_field
         |""".stripMargin
    spark.createDataFrame(genTestData()).createOrReplaceTempView(tableName)
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case l: LocalTableScanExec => l
        }
        assert(csvFileScan.size == 1)
      })
  }

  test("read data from orc file format") {
    val filePath = basePath + "/orc_test.orc"
    // val filePath = "/data2/case_insensitive_column_matching.orc"
    val orcFileFormat = "orc"
    val sql =
      s"""
         | select *
         | from $orcFileFormat.`$filePath`
         | where long_field > 30
         |""".stripMargin
    testFileFormatBase(filePath, orcFileFormat, sql, df => {})
  }

  // TODO: Fix: if the field names has upper case form, it will return null value
  ignore("read data from orc file format with upper case schema names") {
    val filePath = orcDataPath + "/case_insensitive_column_matching.orc"
    val orcFileFormat = "orc"
    val sql =
      s"""
         | select *
         | from $orcFileFormat.`$filePath`
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, df => {})
  }

  def testFileFormatBase(
      filePath: String,
      fileFormat: String,
      sql: String,
      customCheck: DataFrame => Unit
  ): Unit = {
    spark
      .createDataFrame(genTestData())
      .write
      .mode("overwrite")
      .format(fileFormat)
      .save(filePath)
    compareResultsAgainstVanillaSpark(sql, true, customCheck)
  }

  /** Generate test data for primitive type */
  def genTestData(): Seq[AllDataTypesWithNonPrimitiveType] = {
    (0 to 199).map {
      i =>
        if (i % 25 == 0) {
          if (i % 50 == 0) {
            AllDataTypesWithNonPrimitiveType(
              "",
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null)
          } else {
            AllDataTypesWithNonPrimitiveType(null, null, null, null, null, null, null, null, null,
              null)
          }
        } else {
          AllDataTypesWithNonPrimitiveType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            new java.sql.Date(System.currentTimeMillis()))
        }
    }
  }

  test("empty parquet") {
    val df = spark.read.parquet(createEmptyParquet()).toDF().select($"a")
    assert(df.collect().isEmpty)
  }

  def createEmptyParquet(): String = {
    val data = spark.sparkContext.emptyRDD[Row]
    val schema = new StructType()
      .add("a", StringType)

    val fileName = basePath + "/parquet_test_" + new Date().getTime + "_empty.parquet"

    spark.createDataFrame(data, schema).toDF().write.parquet(fileName)
    fileName
  }
}
