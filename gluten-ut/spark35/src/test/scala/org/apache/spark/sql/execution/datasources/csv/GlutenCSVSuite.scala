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
package org.apache.spark.sql.execution.datasources.csv

import org.apache.gluten.GlutenConfig
import org.apache.gluten.exception.GlutenException

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType, TimestampType}

import org.scalatest.exceptions.TestFailedException

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters.seqAsJavaListConverter

class GlutenCSVSuite extends CSVSuite with GlutenSQLTestsBaseTrait {

  override def sparkConf: SparkConf =
    super.sparkConf
      .set(GlutenConfig.NATIVE_ARROW_READER_ENABLED.key, "true")

  /** Returns full path to the given file in the resource folder */
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }
}

class GlutenCSVv1Suite extends GlutenCSVSuite {
  import testImplicits._
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "csv")

  testGluten("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath {
        path =>
          val oschema = new StructType().add("A", StringType)
          // change the row content 0 to string bbb in  Gluten for test
          val odf = spark.createDataFrame(List(Row("bbb")).asJava, oschema)
          odf.write.option("header", true).csv(path.getCanonicalPath)
          val ischema = new StructType().add("a", StringType)
          val idf = spark.read
            .schema(ischema)
            .option("header", true)
            .option("enforceSchema", false)
            .csv(path.getCanonicalPath)
          checkAnswer(idf, odf)
      }
    }
  }

  testGluten("case sensitivity of filters references") {
    Seq(true, false).foreach {
      filterPushdown =>
        withSQLConf(SQLConf.CSV_FILTER_PUSHDOWN_ENABLED.key -> filterPushdown.toString) {
          withTempPath {
            path =>
              Seq("""aaa,BBB""", """0,1""", """2,3""")
                .toDF()
                .repartition(1)
                .write
                .text(path.getCanonicalPath)
              withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
                // change the schema to Arrow schema to support read in Gluten
                val readback = spark.read
                  .schema("aaa long, BBB long")
                  .option("header", true)
                  .csv(path.getCanonicalPath)
                checkAnswer(readback, Seq(Row(2, 3), Row(0, 1)))
                checkAnswer(readback.filter($"AAA" === 2 && $"bbb" === 3), Seq(Row(2, 3)))
              }
              withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
                val readback = spark.read
                  .schema("aaa long, BBB long")
                  .option("header", true)
                  .csv(path.getCanonicalPath)
                checkAnswer(readback, Seq(Row(2, 3), Row(0, 1)))
                checkError(
                  exception = intercept[AnalysisException] {
                    readback.filter($"AAA" === 2 && $"bbb" === 3).collect()
                  },
                  errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
                  parameters = Map("objectName" -> "`AAA`", "proposal" -> "`BBB`, `aaa`")
                )
              }
          }
        }
    }
  }
}

class GlutenCSVv2Suite extends GlutenCSVSuite {

  import testImplicits._
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
      .set(GlutenConfig.NATIVE_ARROW_READER_ENABLED.key, "true")

  override def testNameBlackList: Seq[String] = Seq(
    // overwritten with different test
    "test for FAILFAST parsing mode",
    "SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern"
  )

  testGluten("test for FAILFAST parsing mode") {
    Seq(false, true).foreach {
      multiLine =>
        val exception = intercept[SparkException] {
          spark.read
            .format("csv")
            .option("multiLine", multiLine)
            .options(Map("header" -> "true", "mode" -> "failfast"))
            .load(testFile(carsFile))
            .collect()
        }

        assert(exception.getCause.isInstanceOf[GlutenException])
        assert(
          exception.getMessage.contains(
            "[MALFORMED_RECORD_IN_PARSING] Malformed records are detected in record parsing: " +
              "[2015,Chevy,Volt,null,null]"))
    }
  }

  testGluten("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern") {
    withTempPath {
      path =>
        Seq("1,2020011,2020011", "2,20201203,20201203")
          .toDF()
          .repartition(1)
          .write
          .text(path.getAbsolutePath)
        val schema = new StructType()
          .add("id", IntegerType)
          .add("date", DateType)
          .add("ts", TimestampType)
        val output = spark.read
          .schema(schema)
          .option("dateFormat", "yyyyMMdd")
          .option("timestampFormat", "yyyyMMdd")
          .csv(path.getAbsolutePath)

        def check(mode: String, res: Seq[Row]): Unit = {
          withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> mode) {
            checkAnswer(output, res)
          }
        }

        check(
          "legacy",
          Seq(
            Row(1, Date.valueOf("2020-01-01"), Timestamp.valueOf("2020-01-01 00:00:00")),
            Row(2, Date.valueOf("2020-12-03"), Timestamp.valueOf("2020-12-03 00:00:00"))
          )
        )

        check(
          "corrected",
          Seq(
            Row(1, null, null),
            Row(2, Date.valueOf("2020-12-03"), Timestamp.valueOf("2020-12-03 00:00:00"))
          )
        )

        val err = intercept[TestFailedException] {
          check("exception", Nil)
        }
        assert(err.message.get.contains("org.apache.spark.SparkUpgradeException"))
    }
  }
}

class GlutenCSVLegacyTimeParserSuite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY, "legacy")
}
