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

  test("Test abs function") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test acos function") {
    runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test asin function") {
    runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test atan function") {
    runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test atan2 function datatab") {
    runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test bin function") {
    val df = runQueryAndCompare("SELECT bin(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test ceil function") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test ceiling function") {
    runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cos function") {
    runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cosh function") {
    runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test degrees function") {
    runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test exp function") {
    val df = runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test factorial function") {
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

  test("Test floor function") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test greatest function") {
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

  test("Test hex function") {
    runQueryAndCompare("SELECT hex(l_partkey), hex(l_shipmode) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test least function") {
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

  test("Test log function") {
    runQueryAndCompare("SELECT log(10, l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test log10 function") {
    runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test negative function") {
    runQueryAndCompare("select negative(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test pmod function") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test power function") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test rand function") {
    runQueryAndCompare(
      """SELECT rand() from lineitem limit 100""".stripMargin,
      compareResult = false) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test rint function") {
    withTempPath {
      path =>
        Seq(1.2, 1.5, 1.9).toDF("d").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("double")
        runQueryAndCompare("select rint(d) from double") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test round function") {
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

  test("Test shiftleft function") {
    runQueryAndCompare("SELECT shiftleft(int_field1, 1) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test try_add function") {
    runQueryAndCompare(
      "select try_add(cast(l_orderkey as int), 1), try_add(cast(l_orderkey as int), 2147483647)" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test try_divide function") {
    runQueryAndCompare(
      "select try_divide(cast(l_orderkey as int), 0) from lineitem",
      noFallBack = false) {
      _ => // Spark would always cast inputs to double for this function.
    }
  }

  testWithMinSparkVersion("Test try_multiply function", "3.3") {
    runQueryAndCompare(
      "select try_multiply(2147483647, cast(l_orderkey as int)), " +
        "try_multiply(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("Test try_subtract function", "3.3") {
    runQueryAndCompare(
      "select try_subtract(2147483647, cast(l_orderkey as int)), " +
        "try_subtract(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test unhex function") {
    runQueryAndCompare("SELECT unhex(hex(l_shipmode)) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("Test width_bucket function", "3.4") {
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

  test("Test sqrt function") {
    val df = runQueryAndCompare("SELECT sqrt(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }
}
