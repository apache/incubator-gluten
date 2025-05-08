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
import org.apache.gluten.execution.{BatchScanExecTransformer, FilterExecTransformer, ProjectExecTransformer}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.execution.ProjectExec

class ScalarFunctionsValidateSuiteRasOff extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "false")
  }
}

class ScalarFunctionsValidateSuiteRasOn extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "true")
  }
}

abstract class ScalarFunctionsValidateSuite extends FunctionsValidateSuite {
  disableFallbackCheck

  import testImplicits._

  // Test "SELECT ..." without a from clause.
  test("isnull function") {
    runQueryAndCompare("SELECT isnull(1)")(checkGlutenOperatorMatch[ProjectExecTransformer])
  }

  test("Test bit_count function") {
    runQueryAndCompare("SELECT bit_count(l_partkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test bit_get and getbit function") {
    runQueryAndCompare("SELECT bit_get(l_partkey, 0) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT getbit(l_partkey, 0) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("Test array_append function - INT", "3.4") {
    withTempPath {
      path =>
        Seq[(Array[Int], Int)](
          (Array(2, 1), 0),
          (Array(1), 1),
          (Array(), 0),
          (Array(1, 2, null.asInstanceOf[Int]), 1),
          (Array(null.asInstanceOf[Int]), 1),
          (Array(null.asInstanceOf[Int]), null.asInstanceOf[Int]),
          (Array(), null.asInstanceOf[Int]),
          (null.asInstanceOf[Array[Int]], 1)
        )
          .toDF("arr", "num")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, num, array_append(arr, num) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("Test array_append function - STRING", "3.4") {
    withTempPath {
      path =>
        Seq[(Array[String], String)](
          (Array("a", "b"), "c"),
          (Array("a"), "b"),
          (Array(), "a"),
          (Array("a", "b", null.asInstanceOf[String]), "c"),
          (Array(null.asInstanceOf[String]), "a"),
          (Array(null.asInstanceOf[String]), null.asInstanceOf[String]),
          (Array(), null.asInstanceOf[String])
        )
          .toDF("arr", "txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, txt, array_append(arr, txt) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("Test array_prepend function", "3.5") {
    withTempPath {
      path =>
        Seq[(Array[String], String)](
          (Array("a", "b"), "c"),
          (Array("a"), "b"),
          (Array(), "a"),
          (Array("a", "b", null.asInstanceOf[String]), "c"),
          (Array(null.asInstanceOf[String]), "a"),
          (Array(null.asInstanceOf[String]), null.asInstanceOf[String]),
          (Array(), null.asInstanceOf[String])
        )
          .toDF("arr", "txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, txt, array_prepend(arr, txt) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("Test array_compact function", "3.4") {
    withTempPath {
      path =>
        Seq[Array[String]](
          Array("a", "b"),
          Array(),
          Array("a", "b", null.asInstanceOf[String]),
          Array(null.asInstanceOf[String])
        )
          .toDF("arr")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select arr, array_compact(arr) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("null input for array_size", "3.3") {
    withTempPath {
      path =>
        Seq[(Array[Int])](
          (null.asInstanceOf[Array[Int]])
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select array_size(txt) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("chr function") {
    val df = runQueryAndCompare("SELECT chr(l_orderkey + 64) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("Test hash function") {
    runQueryAndCompare("SELECT hash(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test shiftright function") {
    val df = runQueryAndCompare("SELECT shiftright(int_field1, 1) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("array_aggregate") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](
          Seq(1, 9, 8, 7),
          Seq(5, null, 8, 9, 7, 2),
          Seq.empty,
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select aggregate(i, 0, (acc, x) -> acc + x," +
            " acc -> acc * 3) as v from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        Seq(
          (1, Array[Int](1, 2, 3)),
          (5, Array[Int](4, 5, 6))
        )
          .toDF("x", "ys")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select aggregate(ys, 0, (y, a) -> y + a + x) as v from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Support HOUR function") {
    withTable("t1") {
      sql("create table t1 (c1 int, c2 timestamp) USING PARQUET")
      sql("INSERT INTO t1 VALUES(1, NOW())")
      runQueryAndCompare("SELECT c1, HOUR(c2) FROM t1 LIMIT 1")(df => checkFallbackOperators(df, 0))
    }
  }

  test("map extract - getmapvalue") {
    withTempPath {
      path =>
        Seq(
          Map[Int, Int](1 -> 100, 2 -> 200),
          Map[Int, Int](),
          Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select i[\"1\"] from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_entries function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_entries(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_keys function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_keys(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("test map_contains_key function", "3.3") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_contains_key(i, 1) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_values function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_values(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map_zip_with") {
    withTempPath {
      path =>
        Seq((Map("a" -> 1, "b" -> 2), Map("a" -> 2, "b" -> 3)))
          .toDF("m1", "m2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare(
          "select map_zip_with(m1, m2, (k, v1, v2) -> k == v1 + v2) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map_concat") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")
        runQueryAndCompare("select map_concat(m, map('c', 4)) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map_filter") {
    withTempPath {
      path =>
        Seq((Map("a" -> 1, "b" -> 2, "c" -> 3)))
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_filter(m, (k, v) -> k != 'b') from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test transform_keys function") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_keys(m, (k, v) -> upper(k)) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test transform_values function") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_values(m, (k, v) -> v + 1) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("zip_with") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(9001, 9002, 9003), Seq(4, 5, 6)),
          (Seq(1, 2), Seq(3, 4)),
          (Seq.empty, Seq.empty),
          (null, null)
        ).toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select zip_with(val1, val2, (x, y) -> x + y) from array_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test isnan function") {
    runQueryAndCompare(
      "SELECT isnan(l_orderkey), isnan(cast('NaN' as double)), isnan(0.0F/0.0F)" +
        " from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test conditional function") {
    Seq("nanvl", "nullif", "nvl").foreach {
      func =>
        runQueryAndCompare(s"""
                              |SELECT
                              | $func(cast('nan' as float), 1f),
                              | $func(l_orderkey, cast('null' as double)),
                              | $func(cast('null' as double), l_orderkey),
                              | $func(l_orderkey, l_orderkey / 0.0d),
                              | $func(cast('nan' as float), l_orderkey)
                              | from lineitem limit 1
                              |""".stripMargin) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test nvl2 function") {
    Seq("null", "l_orderkey").foreach {
      expr =>
        runQueryAndCompare(s"""
                              |SELECT
                              | nvl2($expr, cast('nan' as float), 1f),
                              | nvl2($expr, l_orderkey, cast('null' as double)),
                              | nvl2($expr, cast('null' as double), l_orderkey),
                              | nvl2($expr, l_orderkey, l_orderkey / 0.0d),
                              | nvl2($expr, cast('nan' as float), l_orderkey)
                              | from lineitem limit 1
                              |""".stripMargin) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  // FIXME: Ignored: https://github.com/apache/incubator-gluten/issues/7600.
  ignore("Test monotonically_increasintestg_id function") {
    runQueryAndCompare("""SELECT monotonically_increasing_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test sequence function optimized by Spark constant folding") {
    withSQLConf(("spark.sql.optimizer.excludedRules", NullPropagation.ruleName)) {
      runQueryAndCompare("""SELECT sequence(1, 5), l_orderkey
                           | from lineitem limit 100""".stripMargin) {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("Test raise_error, assert_true function") {
    runQueryAndCompare("""SELECT assert_true(l_orderkey >= 1), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    val e = intercept[SparkException] {
      sql("""SELECT assert_true(l_orderkey >= 100), l_orderkey from
            | lineitem limit 100""".stripMargin).collect()
    }
    assert(e.getCause.isInstanceOf[RuntimeException])
    assert(e.getMessage.contains("l_orderkey"))
  }

  test("Test E function") {
    runQueryAndCompare("""SELECT E() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT E(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Pi function") {
    runQueryAndCompare("""SELECT Pi() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT Pi(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test version function") {
    runQueryAndCompare("""SELECT version() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT version(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test sum/count function") {
    withTempPath {
      path =>
        Seq[(Integer, Integer)](
          (2, 2)
        )
          .toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("SELECT sum(val1),count(val2) from tbl") {
          checkGlutenOperatorMatch[BatchScanExecTransformer]
        }
    }
  }

  test("Test spark_partition_id function") {
    runQueryAndCompare("""SELECT spark_partition_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT spark_partition_id()
                         |from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("Test url_decode function", "3.4") {
    withTempPath {
      path =>
        Seq("https%3A%2F%2Fspark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_decode(a) from url_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("Test url_encode function", "3.4") {
    withTempPath {
      path =>
        Seq("https://spark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_encode(a) from url_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("soundex") {
    runQueryAndCompare("select soundex(c_comment) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test uuid function") {
    runQueryAndCompare("""SELECT uuid() from lineitem limit 100""".stripMargin, false) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("regexp_replace") {
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something', 3) FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("mask", "3.4") {
    runQueryAndCompare("SELECT mask(c_comment) FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o', '*') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bit_length") {
    runQueryAndCompare(
      "select bit_length(c_comment), bit_length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_and") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) & cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) & cast(l_partkey as int), l_orderkey & l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_not") {
    runQueryAndCompare(
      "select ~(cast(l_orderkey as tinyint)), ~(cast(l_orderkey as int)), ~l_orderkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_or") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) | cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) | cast(l_partkey as int), l_orderkey | l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_xor") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) ^ cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) ^ cast(l_partkey as int), l_orderkey ^ l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test array filter") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select filter(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select filter(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test concat with array") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select concat(value, array(1)) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

    }
  }

  test("test array transform") {
    withTable("t") {
      sql("create table t (arr ARRAY<INT>) using parquet")
      sql("insert into t values(array(1, 2, 3, null))")
      runQueryAndCompare("select transform(arr, x -> x + 1) from t") {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("test array forall") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select forall(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select forall(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test array exists") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select exists(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select exists(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("arrays_zip") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(1, 2, 3), Seq(3, 4)),
          (Seq(5, null), Seq(null, 1, 2)))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_zip(v1, v2) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test shuffle") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select shuffle(value) from array_tbl;", false) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test flatten nested array") {
    withTempPath {
      path =>
        Seq[Seq[Seq[Integer]]](
          Seq(Seq(1, 2), Seq(4, 5)),
          null,
          Seq(null, Seq(1, 2)),
          Seq(null, null),
          Seq(Seq(1, 2, null), Seq(null, null), Seq(3, 4), Seq.empty))
          .toDF("arrays")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select flatten(arrays) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("get", "3.4") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select get(value, 0), get(value, 1), get(value, 2), get(value, 3) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("length") {
    runQueryAndCompare(
      "select length(c_comment), length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arrays_overlap") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])]((Seq(1, 2, 3), Seq(3, 4)), (Seq(5, null), Seq()))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_overlap(v1, v2) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("levenshtein") {
    runQueryAndCompare("select levenshtein(c_comment, c_address) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithMinSparkVersion("levenshtein with limit", "3.5") {
    runQueryAndCompare("select levenshtein(c_comment, c_address, 3) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test substring_index") {
    withTempView("substring_index_table") {
      withTempPath {
        path =>
          Seq[(String, String, Int)](
            ("www.apache.org", ".", 3),
            ("www.apache.org", ".", 2),
            ("www.apache.org", ".", 1),
            ("www.apache.org", ".", 0),
            ("www.apache.org", ".", -1),
            ("www.apache.org", ".", -2),
            ("www.apache.org", ".", -3),
            ("www.apache.org", "", 1),
            ("www.apache.org", "#", 1),
            ("www||apache||org", "||", 2),
            ("www||apache||org", "||", -2),
            ("", ".", 1),
            ("||||||", "|||", 3),
            ("||||||", "|||", -4)
          )
            .toDF("str", "delim", "count")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("substring_index_table")
          runQueryAndCompare(
            """
              |select substring_index(str, delim, count) from substring_index_table
              |""".stripMargin
          ) {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("repeat") {
    runQueryAndCompare("select repeat(c_comment, 5) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("concat_ws") {
    runQueryAndCompare("SELECT concat_ws('~~', c_comment, c_address) FROM customer LIMIT 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[Seq[String]](Seq("ab", null, "cd", "", "ef"), Seq(null, "x", "", "y"), Seq.empty, null)
          .toDF("col")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("SELECT concat_ws('~~', col, 'end') AS res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test input_file_name function") {
    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from
                         | (select l_orderkey from lineitem
                         | union all
                         | select o_orderkey as l_orderkey from orders)
                         | limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq(1, 2, 3).toDF("a").write.json(path.getCanonicalPath)
        spark.read.json(path.getCanonicalPath).createOrReplaceTempView("json_table")
        val sql =
          """
            |SELECT input_file_name(), a
            |FROM
            |(SELECT a FROM json_table
            |UNION ALL
            |SELECT l_orderkey as a FROM lineitem)
            |LIMIT 100
            |""".stripMargin
        compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }

    // Collapse project if scan is fallback and the outer project is cheap or fallback.
    Seq("true", "false").foreach {
      flag =>
        withSQLConf(
          GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> flag,
          GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
          runQueryAndCompare("SELECT l_orderkey, input_file_name() as name FROM lineitem") {
            df =>
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case f: ProjectExecTransformer => f }.size == 0)
              assert(collect(plan) { case f: ProjectExec => f }.size == 1)
          }
        }
    }
  }

  testWithMinSparkVersion("array insert", "3.4") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        Seq("true", "false").foreach {
          legacyNegativeIndex =>
            withSQLConf("spark.sql.legacy.negativeIndexInArrayInsert" -> legacyNegativeIndex) {
              runQueryAndCompare("""
                                   |select
                                   |  array_insert(value, 1, 0), array_insert(value, 10, 0),
                                   |  array_insert(value, -1, 0), array_insert(value, -10, 0)
                                   |from array_tbl
                                   |""".stripMargin) {
                checkGlutenOperatorMatch[ProjectExecTransformer]
              }
            }
        }
    }
  }

  test("test internal function: AtLeastNNonNulls") {
    // AtLeastNNonNulls is called by drop DataFrameNafunction
    withTempPath {
      path =>
        val input = Seq[(String, java.lang.Integer, java.lang.Double)](
          ("Bob", 16, 176.5),
          ("Alice", null, 164.3),
          ("David", 60, null),
          ("Nina", 25, Double.NaN),
          ("Amy", null, null),
          (null, null, null)
        ).toDF("name", "age", "height")
        val rows = input.collect()
        input.write.parquet(path.getCanonicalPath)

        val df = spark.read.parquet(path.getCanonicalPath).na.drop(2, Seq("age", "height"))
        checkAnswer(df, rows(0) :: Nil)
        checkGlutenOperatorMatch[FilterExecTransformer](df)
    }
  }

  testWithMinSparkVersion("Test try_cast", "3.4") {
    withTempView("try_cast_table") {
      withTempPath {
        path =>
          Seq[(String)](("123456"), ("000A1234"))
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("try_cast_table")
          runQueryAndCompare("select try_cast(str as bigint) from try_cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
          runQueryAndCompare("select try_cast(str as double) from try_cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("Test cast") {
    withTempView("cast_table") {
      withTempPath {
        path =>
          Seq[(String)](("123456"), ("000A1234"))
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("cast_table")
          runQueryAndCompare("select cast(str as bigint) from cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(str as double) from cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  testWithMinSparkVersion("equal_null", "3.4") {
    Seq[(Integer, Integer)]().toDF("a", "b")
    withTempPath {
      path =>
        Seq[(Integer, Integer)](
          (null, 8),
          (8, 8)
        )
          .toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select equal_null(val1, val2) from tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }
}
