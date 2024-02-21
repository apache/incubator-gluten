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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, StructType, TimestampType}

import org.scalatest.exceptions.TestFailedException

import java.sql.{Date, Timestamp}

class GlutenCSVSuite extends CSVSuite with GlutenSQLTestsBaseTrait {

  /** Returns full path to the given file in the resource folder */
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }
}

class GlutenCSVv1Suite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "csv")
}

class GlutenCSVv2Suite extends GlutenCSVSuite {

  import testImplicits._
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test(GLUTEN_TEST + "SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern") {
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
