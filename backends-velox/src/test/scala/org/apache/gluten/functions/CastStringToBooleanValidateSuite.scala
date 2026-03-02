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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer, WholeStageTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.Logger

import java.util.UUID

class CastStringToBooleanValidateSuite extends FunctionsValidateSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "false") // ANSI mode OFF
      .set("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel", "3")
  }

  private def uniqueTableName(prefix: String): String =
    s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"

  private def withStringValuesTable(tablePrefix: String, values: Seq[String])(
      f: String => Unit): Unit = {
    val tableName = uniqueTableName(tablePrefix)
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (str_col STRING) USING parquet")
      val rows = values.map(value => s"($value)").mkString(", ")
      sql(s"INSERT INTO $tableName VALUES $rows")
      f(tableName)
    }
  }

  private def withLogLevels[T](level: Level, loggerNames: Seq[String])(f: => T): T = {
    val loggers = loggerNames.map(name => LogManager.getLogger(name).asInstanceOf[Logger])
    val previousLevels = loggers.map(_.getLevel)
    loggers.foreach(_.setLevel(level))
    try {
      f
    } finally {
      loggers.zip(previousLevels).foreach {
        case (logger, previousLevel) => logger.setLevel(previousLevel)
      }
    }
  }

  private def substraitPlanJson(df: DataFrame): String = {
    val planJson = df.queryExecution.executedPlan
      .collectFirst { case stage: WholeStageTransformer => stage.substraitPlanJson }
    assert(
      planJson.nonEmpty,
      s"Expected WholeStageTransformer in plan: ${df.queryExecution.executedPlan}")
    planJson.get
  }

  private def assertUsesGlutenStringToBooleanCast(df: DataFrame): Unit = {
    val planJson = substraitPlanJson(df)
    assert(
      planJson.contains("\"cast\""),
      s"Expected cast in Substrait plan: ${df.queryExecution.executedPlan}")
  }

  test("cast invalid string to boolean returns null in non-ANSI mode") {
    val invalidStrings = Seq("'invalid'", "'2'", "'maybe'", "''", "' '", "'yes '", "' no'")

    withStringValuesTable("test_cast_bool_invalid_non_ansi", invalidStrings) {
      tableName =>
        runQueryAndCompare(s"SELECT str_col, CAST(str_col AS BOOLEAN) FROM $tableName") {
          df =>
            checkGlutenPlan[ProjectExecTransformer](df)
            assertUsesGlutenStringToBooleanCast(df)
        }
    }
  }

  test("cast valid strings to boolean in non-ANSI mode") {
    val validStrings = Seq("'true'", "'false'", "'1'", "'0'")
    withStringValuesTable("test_cast_bool_valid_non_ansi", validStrings) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast string column with mixed valid and invalid values") {
    val tableName = uniqueTableName("test_cast_bool_mixed_non_ansi")
    withTable(tableName) {
      sql("""
        CREATE TABLE %s (str_col STRING)
        USING parquet
      """.format(tableName))

      sql("""
        INSERT INTO %s VALUES
        ('true'), ('invalid'), ('false'), ('maybe'), ('1'), ('2'), ('0'), (NULL)
      """.format(tableName))

      runQueryAndCompare(
        s"SELECT str_col, CAST(str_col AS BOOLEAN) FROM $tableName"
      ) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("cast string to boolean with whitespace in non-ANSI mode") {
    val stringsWithWhitespace = Seq(
      "' true'",
      "'false '",
      "' 1 '",
      "' yes'",
      "'no '"
    )

    withStringValuesTable("test_cast_bool_whitespace_non_ansi", stringsWithWhitespace) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast string to boolean in WHERE clause with invalid values") {
    val tableName = uniqueTableName("test_cast_bool_where_non_ansi")
    withTable(tableName) {
      sql("""
        CREATE TABLE %s (id INT, str_col STRING)
        USING parquet
      """.format(tableName))

      sql("""
        INSERT INTO %s VALUES
        (1, 'true'), (2, 'invalid'), (3, 'false'), (4, 'maybe'), (5, '1'), (6, '0')
      """.format(tableName))

      // In non-ANSI mode, invalid strings cast to null, so they won't match
      runQueryAndCompare(
        s"""SELECT id, str_col FROM $tableName
           |WHERE CAST(str_col AS BOOLEAN) IS NOT NULL""".stripMargin
      ) {
        checkGlutenPlan[FilterExecTransformer]
      }
    }
  }

  test("cast all valid boolean string variations") {
    val allValidStrings = Seq(
      "'t'",
      "'T'",
      "'true'",
      "'TRUE'",
      "'True'",
      "'TrUe'",
      "'f'",
      "'F'",
      "'false'",
      "'FALSE'",
      "'False'",
      "'FaLsE'",
      "'y'",
      "'Y'",
      "'yes'",
      "'YES'",
      "'Yes'",
      "'YeS'",
      "'n'",
      "'N'",
      "'no'",
      "'NO'",
      "'No'",
      "'nO'",
      "'1'",
      "'0'"
    )

    withStringValuesTable("test_cast_bool_all_valid_non_ansi", allValidStrings) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast null string to boolean in non-ANSI mode") {
    withStringValuesTable("test_cast_bool_null_non_ansi", Seq("NULL")) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast empty and whitespace strings") {
    val emptyAndWhitespace = Seq("''", "' '", "'  '", "'\\t'", "'\\n'")

    withStringValuesTable("test_cast_bool_empty_non_ansi", emptyAndWhitespace) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  override protected def afterAll(): Unit = {
    withLogLevels(Level.ERROR, Seq(this.getClass.getName)) {
      super.afterAll()
    }
  }
}
