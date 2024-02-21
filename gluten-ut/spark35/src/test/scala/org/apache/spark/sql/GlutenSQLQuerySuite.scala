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

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.internal.SQLConf

class GlutenSQLQuerySuite extends SQLQuerySuite with GlutenSQLTestsTrait {
  import testImplicits._

  test(GlutenTestConstants.GLUTEN_TEST + "SPARK-28156: self-join should not miss cached view") {
    withTable("table1") {
      withView("table1_vw") {
        withTempView("cachedview") {
          val df = Seq.tabulate(5)(x => (x, x + 1, x + 2, x + 3)).toDF("a", "b", "c", "d")
          df.write.mode("overwrite").format("parquet").saveAsTable("table1")
          sql("drop view if exists table1_vw")
          sql("create view table1_vw as select * from table1")

          val cachedView = sql("select a, b, c, d from table1_vw")

          cachedView.createOrReplaceTempView("cachedview")
          cachedView.persist()

          val queryDf = sql(s"""select leftside.a, leftside.b
                               |from cachedview leftside
                               |join cachedview rightside
                               |on leftside.a = rightside.a
          """.stripMargin)

          val inMemoryTableScan = collect(queryDf.queryExecution.executedPlan) {
            case i: InMemoryTableScanExec => i
          }
          assert(inMemoryTableScan.size == 2)
          checkAnswer(queryDf, Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Row(3, 4) :: Row(4, 5) :: Nil)
        }
      }
    }

  }

  // Velox throw exception : An unsupported nested encoding was found.
  ignore(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-33338: GROUP BY using literal map should not fail") {
    withTable("t") {
      withTempDir {
        dir =>
          sql(
            s"CREATE TABLE t USING PARQUET LOCATION '${dir.toURI}' AS SELECT map('k1', 'v1') m," +
              s" 'k1' k")
          Seq(
            "SELECT map('k1', 'v1')[k] FROM t GROUP BY 1",
            "SELECT map('k1', 'v1')[k] FROM t GROUP BY map('k1', 'v1')[k]",
            "SELECT map('k1', 'v1')[k] a FROM t GROUP BY a"
          ).foreach(statement => checkAnswer(sql(statement), Row("v1")))
      }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-33593: Vector reader got incorrect data with binary partition value") {
    Seq("false").foreach(
      value => {
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> value) {
          withTable("t1") {
            sql("""CREATE TABLE t1(name STRING, id BINARY, part BINARY)
                  |USING PARQUET PARTITIONED BY (part)""".stripMargin)
            sql("INSERT INTO t1 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
            checkAnswer(
              sql("SELECT name, cast(id as string), cast(part as string) FROM t1"),
              Row("a", "Spark SQL", "Spark SQL"))
          }
        }

        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
          withTable("t2") {
            sql("""CREATE TABLE t2(name STRING, id BINARY, part BINARY)
                  |USING PARQUET PARTITIONED BY (part)""".stripMargin)
            sql("INSERT INTO t2 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
            checkAnswer(
              sql("SELECT name, cast(id as string), cast(part as string) FROM t2"),
              Row("a", "Spark SQL", "Spark SQL"))
          }
        }
      })
  }

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar") {
    withTempView("df") {
      Seq("m@ca").toDF("s").createOrReplaceTempView("df")

      val e = intercept[SparkException] {
        sql("SELECT s LIKE 'm%@ca' ESCAPE '%' FROM df").collect()
      }
      assert(
        e.getMessage.contains(
          "Escape character must be followed by '%', '_' or the escape character itself"))

      checkAnswer(sql("SELECT s LIKE 'm@@ca' ESCAPE '@' FROM df"), Row(true))
    }
  }

  test(GlutenTestConstants.GLUTEN_TEST + "the escape character is not allowed to end with") {
    withTempView("df") {
      Seq("jialiuping").toDF("a").createOrReplaceTempView("df")

      val e = intercept[SparkException] {
        sql("SELECT a LIKE 'jialiuping%' ESCAPE '%' FROM df").collect()
      }
      assert(
        e.getMessage.contains(
          "Escape character must be followed by '%', '_' or the escape character itself"))
    }
  }
}
