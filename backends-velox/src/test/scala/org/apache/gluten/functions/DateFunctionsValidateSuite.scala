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

import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types.Decimal

import java.sql.Timestamp

class DateFunctionsValidateSuiteRasOff extends DateFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "false")
  }
}

class DateFunctionsValidateSuiteRasOn extends DateFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "true")
  }
}

abstract class DateFunctionsValidateSuite extends FunctionsValidateSuite {
  disableFallbackCheck

  import testImplicits._

  test("Test date_add function") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), 1: Integer),
          (java.sql.Date.valueOf("2022-03-12"), 2: Integer),
          (java.sql.Date.valueOf("2022-03-13"), 3: Integer),
          (java.sql.Date.valueOf("2022-03-14"), 4: Integer),
          (java.sql.Date.valueOf("2022-03-15"), 5: Integer),
          (java.sql.Date.valueOf("2022-03-16"), 6: Integer)
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT date_add(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test date_diff function") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), java.sql.Date.valueOf("2022-02-11")),
          (java.sql.Date.valueOf("2022-03-12"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-09-13"), java.sql.Date.valueOf("2022-05-12")),
          (java.sql.Date.valueOf("2022-07-14"), java.sql.Date.valueOf("2022-03-12")),
          (java.sql.Date.valueOf("2022-06-15"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-05-16"), java.sql.Date.valueOf("2022-06-12"))
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT datediff(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test date_format function") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2024-08-22 10:10:10.010")
        val t2 = Timestamp.valueOf("2014-12-31 00:00:00.012")
        val t3 = Timestamp.valueOf("1968-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("c0").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("t")
        runQueryAndCompare("SELECT date_format(c0, 'yyyy') FROM t") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test date_part & extract & weekofyear function") {
    withTempPath {
      path =>
        Seq(
          java.sql.Date.valueOf("2008-02-20"),
          java.sql.Date.valueOf("2022-01-01")
        )
          .toDF("dt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("t")
        runQueryAndCompare("select weekofyear(dt) from t") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
        runQueryAndCompare(
          "SELECT date_part('yearofweek', dt), extract(yearofweek from dt)" +
            " from t") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test date_trunc function") {
    withTempPath {
      path =>
        Seq(
          Timestamp.valueOf("2015-07-22 10:01:40.123456")
        )
          .toDF("dt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("t")
        runQueryAndCompare("""
                             |SELECT
                             |  date_trunc('yy',dt) as t1,
                             |  date_trunc('yyyy', dt) as t2,
                             |  date_trunc('year', dt) as t3,
                             |  date_trunc('quarter', dt) as t4,
                             |  date_trunc('mon', dt) as t5,
                             |  date_trunc('month', dt) as t6,
                             |  date_trunc('mm', dt) as t7,
                             |  date_trunc('week', dt) as t8,
                             |  date_trunc('dd', dt) as t9,
                             |  date_trunc('day', dt) as t10,
                             |  date_trunc('hour', dt) as t11,
                             |  date_trunc('minute', dt) as t12,
                             |  date_trunc('second', dt) as t13,
                             |  date_trunc('millisecond', dt) as t14,
                             |  date_trunc('microsecond', dt) as t15
                             |FROM t
                             |""".stripMargin) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test from_utc_timestamp function") {
    withTempPath {
      path =>
        Seq(
          (Timestamp.valueOf("2015-07-24 00:00:00"), "America/Los_Angeles"),
          (Timestamp.valueOf("2015-07-25 00:00:00"), "America/Los_Angeles")
        ).toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT from_utc_timestamp(a, \"America/Los_Angeles\") from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
        runQueryAndCompare("SELECT from_utc_timestamp(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test make_date function") {
    runQueryAndCompare(
      "select make_date(2025, 2, 7), make_date(2024, 11, null), make_date(2024, 11, 50)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test make_timestamp function") {
    withTempPath {
      path =>
        // w/o timezone.
        Seq(
          (2017, 7, 11, 6, 30, Decimal(45678000, 18, 6)),
          (1, 1, 1, 1, 1, Decimal(1, 18, 6)),
          (1, 1, 1, 1, 1, null)
        )
          .toDF("year", "month", "day", "hour", "min", "sec")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_timestamp_tbl1")

        runQueryAndCompare(
          "select make_timestamp(year, month, day, hour, min, sec) from make_timestamp_tbl1") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        // w/ timezone.
        Seq(
          (2017, 7, 11, 6, 30, Decimal(45678000, 18, 6), "CET"),
          (1, 1, 1, 1, 1, Decimal(1, 18, 6), null),
          (1, 1, 1, 1, 1, null, "CST")
        )
          .toDF("year", "month", "day", "hour", "min", "sec", "timezone")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_timestamp_tbl2")

        runQueryAndCompare("""
                             |select make_timestamp(year, month, day, hour, min, sec, timezone)
                             |from make_timestamp_tbl2
                             |""".stripMargin) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test make_ym_interval function") {
    runQueryAndCompare("select make_ym_interval(1, 1)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("select make_ym_interval(1)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("select make_ym_interval()") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[(Integer, Integer)]((1, 0), (-1, 1), (null, 1), (1, null))
          .toDF("year", "month")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_ym_interval_tbl")

        runQueryAndCompare("select make_ym_interval(year, month) from make_ym_interval_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select make_ym_interval(year) from make_ym_interval_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test timestamp_micros function") {
    runQueryAndCompare("select timestamp_micros(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test timestamp_millis function") {
    runQueryAndCompare("select timestamp_millis(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test to_utc_timestamp function") {
    withTempPath {
      path =>
        Seq(
          (Timestamp.valueOf("2015-07-24 00:00:00"), "America/Los_Angeles"),
          (Timestamp.valueOf("2015-07-25 00:00:00"), "America/Los_Angeles")
        ).toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT to_utc_timestamp(a, \"America/Los_Angeles\") from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
        runQueryAndCompare("SELECT to_utc_timestamp(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test unix_date function") {
    withTempPath {
      path =>
        Seq(
          java.sql.Date.valueOf("1970-01-01"),
          java.sql.Date.valueOf("1969-12-31"),
          java.sql.Date.valueOf("2022-09-13")
        )
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT unix_date(a) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test unix_micros function") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2015-07-22 10:00:00.012")
        val t2 = Timestamp.valueOf("2014-12-31 23:59:59.012")
        val t3 = Timestamp.valueOf("2014-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("time")
        runQueryAndCompare("select unix_micros(t) from time") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test unix_millis function") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2015-07-22 10:00:00.012")
        val t2 = Timestamp.valueOf("2014-12-31 23:59:59.012")
        val t3 = Timestamp.valueOf("2014-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("time")
        runQueryAndCompare("select unix_millis(t) from time") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test unix_seconds function") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2024-08-22 10:10:10.010")
        val t2 = Timestamp.valueOf("2014-12-31 00:00:00.012")
        val t3 = Timestamp.valueOf("1968-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select unix_seconds(t) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("PreciseTimestampConversion") {
    withTempPath {
      path =>
        spark
          .sql(
            "select * from VALUES ('A1', TIMESTAMP'2021-01-01 00:00:00'), " +
              "('A1', TIMESTAMP'2021-01-01 00:04:30'), ('A1', TIMESTAMP'2021-01-01 00:06:00'), " +
              "('A2', TIMESTAMP'2021-01-01 00:01:00') AS tab(a, b)")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("string_timestamp")

        runQueryAndCompare(
          "SELECT a, window.start, window.end, count(*) as cnt FROM" +
            " string_timestamp GROUP by a, window(b, '5 minutes') ORDER BY a, start;") {
          df =>
            val executedPlan = getExecutedPlan(df)
            assert(
              executedPlan.exists(plan => plan.isInstanceOf[ProjectExecTransformer]),
              s"Expect ProjectExecTransformer exists " +
                s"in executedPlan:\n ${executedPlan.last}"
            )
            assert(
              !executedPlan.exists(plan => plan.isInstanceOf[ProjectExec]),
              s"Expect ProjectExec doesn't exist " +
                s"in executedPlan:\n ${executedPlan.last}"
            )
        }
    }
  }
}
