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
import org.apache.spark.sql.{functions, DataFrame, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{StructField, _}

import java.sql.{Date, Timestamp}
import java.util

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

  protected val orcDataPath: String = rootPath + "orc-data"
  protected val csvDataPath: String = rootPath + "csv-data"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.date_time_input_format",
        "best_effort_us")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.use_excel_serialization", "true")
  }

  // in this case, FakeRowAdaptor does R2C
  test("parquet native writer writing a in memory DF") {
    val filePath = basePath + "/native_parquet_test"
    val format = "parquet"

    val df1 = spark
      .createDataFrame(genTestData())
    df1.write
      .mode("overwrite")
      .format("native_parquet")
      .save(filePath)
    val sql =
      s"""
         | select *
         | from $format.`$filePath`
         |""".stripMargin
    val df2 = spark.sql(sql)
    df2.collect()
    WholeStageTransformerSuite.checkFallBack(df2)
    checkAnswer(df2, df1)
  }

  // in this case, FakeRowAdaptor only wrap&transfer
  test("parquet native writer writing a DF from file") {
    val filePath = basePath + "/native_parquet_test"
    val format = "parquet"

    val df1 = spark.read.parquet(tablesPath + "/customer")
    df1.write
      .mode("overwrite")
      .format("native_parquet")
      .save(filePath)
    val sql =
      s"""
         | select *
         | from $format.`$filePath`
         |""".stripMargin
    val df2 = spark.sql(sql)
    df2.collect()
    WholeStageTransformerSuite.checkFallBack(df2)
    checkAnswer(df2, df1)
  }

  // in this case, FakeRowAdaptor only wrap&transfer
  test("parquet native writer writing a DF from an aggregate") {
    val filePath = basePath + "/native_parquet_test_agg"
    val format = "parquet"

    val df0 = spark
      .createDataFrame(genTestData())
    val df1 = df0
      .select("string_field", "int_field", "double_field")
      .groupBy("string_field")
      .agg(
        functions.sum("int_field").as("a"),
        functions.max("double_field").as("b"),
        functions.count("*").as("c"))
    df1.write
      .mode("overwrite")
      .format("native_parquet")
      .save(filePath)

    val sql =
      s"""
         | select *
         | from $format.`$filePath`
         |""".stripMargin
    val df2 = spark.sql(sql)
    df2.collect()
    WholeStageTransformerSuite.checkFallBack(df2)
    checkAnswer(df2, df1)
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
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(csvFileScan.size == 1)
      }
    )
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
          case f: FileSourceScanExecTransformer => f
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
         | select _c7, count(_c0), sum(_c1), avg(_c2), min(_c3), max(_c4), sum(_c5), sum(_c8)
         | from $csvFileFormat.`$filePath`
         | group by _c7
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(csvFileScan.size == 1)
      },
      noFallBack = false
    )
  }

  test("read excel export csv base") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", DateType, nullable = true),
        StructField.apply("c2", TimestampType, nullable = true),
        StructField.apply("c3", FloatType, nullable = true),
        StructField.apply("c4", DoubleType, nullable = true),
        StructField.apply("c5", IntegerType, nullable = true),
        StructField.apply("c6", LongType, nullable = true),
        StructField.apply("c7", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("nullValue", "null")
      .schema(schema)
      .csv(csvDataPath + "/excel_data_base.csv")
      .toDF()

    val result = df.collect()
    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
    assert(result.length == 14)
    assert(result.apply(0).getString(6) == null)
  }

  test("read excel export csv delimiter") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", DateType, nullable = true),
        StructField.apply("b", TimestampType, nullable = true),
        StructField.apply("c", FloatType, nullable = true),
        StructField.apply("d", DoubleType, nullable = true),
        StructField.apply("e", IntegerType, nullable = true),
        StructField.apply("f", LongType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", "|")
      .schema(schema)
      .csv(csvDataPath + "/excel_data_delimiter.csv")
      .toDF()

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
    assert(df.collect().length == 12)
  }

  test("expected_end_of_line") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", IntegerType, nullable = true),
        StructField.apply("c2", StringType, nullable = true),
        StructField.apply("c3", StringType, nullable = true),
        StructField.apply("c4", StringType, nullable = true),
        StructField.apply("c5", StringType, nullable = true),
        StructField.apply("c6", StringType, nullable = true),
        StructField.apply("c7", StringType, nullable = true),
        StructField.apply("c8", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "false")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/expected_end_of_line.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("header", "false")
        .option("quote", "\"")
        .schema(schema)
        .csv(csvDataPath + "/expected_end_of_line.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("csv pruning") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/double_quote.csv")
      .toDF()

    df.createTempView("pruning")

    compareResultsAgainstVanillaSpark(
      """
        |select
        |          c2
        |        from
        |          pruning
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("csv count(*)") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/double_quote.csv")
      .toDF()

    df.createTempView("countallt")
    compareResultsAgainstVanillaSpark(
      """
        |select
        |          count(*)
        |        from
        |          countallt
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("csv \\r") {
    val csv_files = Seq("csv_r.csv", "中文.csv")

    csv_files.foreach(
      file => {
        val csv_path = csvDataPath + "/" + file
        val schema = StructType.apply(
          Seq(
            StructField.apply("c1", StringType, nullable = true)
          ))

        val df = spark.read
          .option("delimiter", ",")
          .option("header", "false")
          .schema(schema)
          .csv(csv_path)
          .toDF()

        var expectedAnswer: Seq[Row] = null
        withSQLConf(vanillaSparkConfs(): _*) {
          expectedAnswer = spark.read
            .option("delimiter", ",")
            .option("header", "false")
            .schema(schema)
            .csv(csv_path)
            .toDF()
            .collect()
        }
        checkAnswer(df, expectedAnswer)
      })
  }

  test("fix: read field value wrong") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
      def timestamp: Timestamp = Timestamp.valueOf(s)
    }

    val csv_path = csvDataPath + "/field_value_wrong.csv"
    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")

    val schema = StructType.apply(
      Seq(
        StructField.apply("a", DateType, nullable = true),
        StructField.apply("b", TimestampType, nullable = true)
      ))

    val data = new util.ArrayList[Row]()
    data.add(Row("2023-06-16".date, "2023-06-16 18:00:05".timestamp))

    spark
      .createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .format("csv")
      .options(options)
      .save(csv_path)

    spark.read
      .options(options)
      .schema(schema)
      .csv(csv_path)
      .toDF()
      .createTempView("field_read_wrong")

    compareResultsAgainstVanillaSpark(
      "select * from field_read_wrong",
      compareResult = true,
      _ => {})
  }

  test("cannot_parse_input") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true),
        StructField.apply("c3", StringType, nullable = true),
        StructField.apply("c4", StringType, nullable = true),
        StructField.apply("c5", StringType, nullable = true),
        StructField.apply("c6", StringType, nullable = true),
        StructField.apply("c7", StringType, nullable = true),
        StructField.apply("c8", StringType, nullable = true),
        StructField.apply("c9", StringType, nullable = true),
        StructField.apply("c10", StringType, nullable = true),
        StructField.apply("c11", DoubleType, nullable = true),
        StructField.apply("c12", DoubleType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "false")
      .schema(schema)
      .csv(csvDataPath + "/cannot_parse_input.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("header", "false")
        .schema(schema)
        .csv(csvDataPath + "/cannot_parse_input.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df1 = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/double_quote.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("quote", "\"")
        .schema(schema)
        .csv(csvDataPath + "/double_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df1, expectedAnswer)

    var csvFileScan = collect(df1.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)

    val df2 = spark.read
      .option("delimiter", ",")
      .option("quote", "\'")
      .schema(schema)
      .csv(csvDataPath + "/single_quote.csv")
      .toDF()

    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("quote", "\'")
        .schema(schema)
        .csv(csvDataPath + "/single_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df2, expectedAnswer)

    csvFileScan = collect(df2.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel with header") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ";")
      .option("quote", "")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/with_header.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ";")
        .option("quote", "")
        .option("header", "true")
        .schema(schema)
        .csv(csvDataPath + "/with_header.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel with escape with quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("escape", "\\")
      .option("quote", "\'")
      .schema(schema)
      .csv(csvDataPath + "/escape_quote.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("quote", "\'")
        .option("escape", "\\")
        .schema(schema)
        .csv(csvDataPath + "/escape_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel with escape without quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("escape", "\\")
      .schema(schema)
      .csv(csvDataPath + "/escape_without_quote.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("escape", "\\")
        .schema(schema)
        .csv(csvDataPath + "/escape_without_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("read data from csv file format with table") {
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
      compareResult = true,
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
    compareResultsAgainstVanillaSpark(sql, compareResult = true, df => {}, noFallBack = false)
  }

  def testFileFormatBase(
      filePath: String,
      fileFormat: String,
      sql: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true
  ): Unit = {
    spark
      .createDataFrame(genTestData())
      .write
      .mode("overwrite")
      .format(fileFormat)
      .option("quote", "\"")
      .save(filePath)
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      customCheck,
      noFallBack = noFallBack)
  }

  /** Generate test data for primitive type */
  def genTestData(): Seq[AllDataTypesWithNonPrimitiveType] = {
    (0 to 299).map {
      i =>
        if (i % 100 == 1) {
          AllDataTypesWithNonPrimitiveType(
            "测试中文",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null)
        } else if (i % 25 == 0) {
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

    val fileName = basePath + "/parquet_test_" + System.currentTimeMillis() + "_empty.parquet"

    spark.createDataFrame(data, schema).toDF().write.parquet(fileName)
    fileName
  }
}
