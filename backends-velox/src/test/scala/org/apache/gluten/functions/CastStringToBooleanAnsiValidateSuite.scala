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
import org.apache.gluten.execution.{FilterExecTransformer, GlutenQueryComparisonTest, ProjectExecTransformer, WholeStageTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.Logger

import java.util.UUID

class CastStringToBooleanAnsiValidateSuite extends FunctionsValidateSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
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

  private def assertUsesAnsiStringToBooleanCast(df: DataFrame): Unit = {
    val planJson = substraitPlanJson(df)
    assert(
      planJson.contains("spark_cast_string_to_boolean_ansi"),
      s"Expected ANSI string-to-boolean cast in plan: ${df.queryExecution.executedPlan}")
  }

  test("cast valid true strings to boolean") {
    val validTrueStrings =
      Seq("'t'", "'true'", "'y'", "'yes'", "'1'", "'T'", "'TRUE'", "'Y'", "'YES'")

    withStringValuesTable("test_cast_bool_true_ansi", validTrueStrings) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast valid false strings to boolean") {
    val validFalseStrings =
      Seq("'f'", "'false'", "'n'", "'no'", "'0'", "'F'", "'FALSE'", "'N'", "'NO'")

    withStringValuesTable("test_cast_bool_false_ansi", validFalseStrings) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast invalid string to boolean follows vanilla Spark behavior in ANSI mode") {
    val invalidStrings = Seq("'invalid'", "'2'", "'maybe'", "''", "' '", "'yes '", "' no'")

    withLogLevels(
      Level.OFF,
      Seq(
        "org.apache.spark.executor.Executor",
        "org.apache.spark.scheduler.TaskSetManager",
        "org.apache.spark.task.TaskResources")) {
      invalidStrings.foreach {
        str =>
          withStringValuesTable("test_cast_bool_invalid_ansi", Seq(str)) {
            tableName =>
              val query = s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName"
              val vanillaThrew =
                try {
                  withSQLConf(vanillaSparkConfs(): _*) {
                    sql(query).collect()
                  }
                  false
                } catch {
                  case _: Exception => true
                }

              if (vanillaThrew) {
                val df = sql(query)
                GlutenQueryComparisonTest.checkFallBack(df, noFallback = true)
                checkGlutenPlan[ProjectExecTransformer](df)
                assertUsesAnsiStringToBooleanCast(df)
                intercept[Exception] {
                  df.collect()
                }
              } else {
                runQueryAndCompare(query) {
                  checkGlutenPlan[ProjectExecTransformer]
                }
              }
          }
      }
    }
  }

  test("cast null string to boolean") {
    withStringValuesTable("test_cast_bool_null_ansi", Seq("NULL")) {
      tableName =>
        runQueryAndCompare(s"SELECT CAST(str_col AS BOOLEAN) FROM $tableName") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast string column to boolean with valid values") {
    val tableName = uniqueTableName("test_cast_bool_valid")
    withTable(tableName) {
      sql("""
        CREATE TABLE %s (str_col STRING)
        USING parquet
      """.format(tableName))

      sql("""
        INSERT INTO %s VALUES
        ('true'), ('false'), ('1'), ('0'), ('yes'), ('no'), ('t'), ('f')
      """.format(tableName))

      runQueryAndCompare(
        s"SELECT str_col, CAST(str_col AS BOOLEAN) FROM $tableName"
      ) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("cast string column to boolean with mixed valid and null values") {
    val tableName = uniqueTableName("test_cast_bool_mixed")
    withTable(tableName) {
      sql("""
        CREATE TABLE %s (str_col STRING)
        USING parquet
      """.format(tableName))

      sql("""
        INSERT INTO %s VALUES
        ('true'), (NULL), ('false'), (NULL), ('1'), ('0')
      """.format(tableName))

      runQueryAndCompare(
        s"SELECT str_col, CAST(str_col AS BOOLEAN) FROM $tableName"
      ) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("cast string to boolean in WHERE clause") {
    val tableName = uniqueTableName("test_cast_bool_where")
    withTable(tableName) {
      sql("""
        CREATE TABLE %s (id INT, str_col STRING)
        USING parquet
      """.format(tableName))

      sql("""
        INSERT INTO %s VALUES
        (1, 'true'), (2, 'false'), (3, '1'), (4, '0'), (5, 'yes'), (6, 'no')
      """.format(tableName))

      runQueryAndCompare(
        s"SELECT id, str_col FROM $tableName WHERE CAST(str_col AS BOOLEAN) = true"
      ) {
        checkGlutenPlan[FilterExecTransformer]
      }
    }
  }

  test("cast string to boolean with case variations") {
    val caseVariations = Seq(
      "'TrUe'",
      "'FaLsE'",
      "'YeS'",
      "'No'",
      "'T'",
      "'F'",
      "'Y'",
      "'N'"
    )

    withStringValuesTable("test_cast_bool_case_ansi", caseVariations) {
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
