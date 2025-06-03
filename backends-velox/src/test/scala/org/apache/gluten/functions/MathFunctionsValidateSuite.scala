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

import org.apache.gluten.execution.{BatchScanExecTransformer, ProjectExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class MathFunctionsValidateSuiteRasOff extends MathFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "false")
  }
}

class MathFunctionsValidateSuiteRasOn extends MathFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "true")
  }
}

abstract class MathFunctionsValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  test("abs") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("acos") {
    runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("asin") {
    runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("atan") {
    runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("atan2") {
    runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bin") {
    val df = runQueryAndCompare("SELECT bin(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("ceil") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("ceiling") {
    runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("cos") {
    runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("cosh") {
    runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("degrees") {
    runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("exp") {
    val df = runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("factorial") {
    withTable("factorial_input") {
      sql("CREATE TABLE factorial_input(id INT) USING parquet")
      sql("""
            |INSERT INTO factorial_input VALUES
            |(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
            |""".stripMargin)

      val query =
        """
          |SELECT
          |  id,
          |  factorial(id)
          |FROM factorial_input
          |""".stripMargin

      val expectedResults = Seq(
        Row(0, 1L),
        Row(1, 1L),
        Row(2, 2L),
        Row(3, 6L),
        Row(4, 24L),
        Row(5, 120L),
        Row(6, 720L),
        Row(7, 5040L),
        Row(8, 40320L),
        Row(9, 362880L),
        Row(10, 3628800L)
      )

      runSql(query) {
        df =>
          checkGlutenOperatorMatch[ProjectExecTransformer](df)
          val result = df.collect()
          assert(result.length == expectedResults.length)
          assert(result === expectedResults)
      }
    }
  }

  test("floor") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("greatest") {
    runQueryAndCompare(
      "SELECT greatest(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT greatest(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("hex") {
    runQueryAndCompare("SELECT hex(l_partkey), hex(l_shipmode) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("least") {
    runQueryAndCompare(
      "SELECT least(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT least(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("log") {
    runQueryAndCompare("SELECT log(10, l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("log10") {
    runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("negative") {
    runQueryAndCompare("select negative(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("pmod") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("power") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("rand") {
    runQueryAndCompare(
      """SELECT rand() from lineitem limit 100""".stripMargin,
      compareResult = false) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("rint") {
    withTempPath {
      path =>
        Seq(1.2, 1.5, 1.9).toDF("d").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("double")
        runQueryAndCompare("select rint(d) from double") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("round") {
    runQueryAndCompare(
      "SELECT round(cast(l_orderkey as int), 2)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("""
                         |select round(l_quantity, 2) from lineitem;
                         |""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    // Scale > 0 should return same value as input on integral values
    compareResultsAgainstVanillaSpark("select round(78, 1)", true, { _ => })
    // Scale < 0 should round down even on integral values
    compareResultsAgainstVanillaSpark("select round(44, -1)", true, { _ => })
  }

  test("shiftleft") {
    runQueryAndCompare("SELECT shiftleft(int_field1, 1) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("try_add") {
    runQueryAndCompare(
      "select try_add(cast(l_orderkey as int), 1), try_add(cast(l_orderkey as int), 2147483647)" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("try_divide") {
    runQueryAndCompare(
      "select try_divide(cast(l_orderkey as int), 0) from lineitem",
      noFallBack = false) {
      _ => // Spark would always cast inputs to double for this function.
    }
  }

  testWithMinSparkVersion("try_multiply", "3.3") {
    runQueryAndCompare(
      "select try_multiply(2147483647, cast(l_orderkey as int)), " +
        "try_multiply(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("try_subtract", "3.3") {
    runQueryAndCompare(
      "select try_subtract(2147483647, cast(l_orderkey as int)), " +
        "try_subtract(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("unhex") {
    runQueryAndCompare("SELECT unhex(hex(l_shipmode)) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("width_bucket", "3.4") {
    withTempPath {
      path =>
        Seq[(Integer, Integer, Integer, Integer)](
          (2, 0, 4, 3)
        )
          .toDF("val1", "val2", "val3", "val4")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("SELECT width_bucket(val1, val2, val3, val4) from tbl") {
          checkGlutenOperatorMatch[BatchScanExecTransformer]
        }
    }
  }

  test("sqrt") {
    val df = runQueryAndCompare("SELECT sqrt(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }
}
