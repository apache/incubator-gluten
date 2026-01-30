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
package org.apache.gluten.execution

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.extension.columnar.validator.FallbackInjects

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

abstract class VeloxAggregateFunctionsSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.MERGE_TWO_PHASES_ENABLED.key, "false")
  }

  test("count") {
    runQueryAndCompare("select count(*) from lineitem where l_partkey in (1552, 674, 1062)") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select count(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("avg") {
    runQueryAndCompare("select avg(l_partkey) from lineitem where l_partkey < 1000") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select avg(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    // Test the situation that precision + 4 of input decimal value exceeds 38.
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(36, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("sum") {
    runQueryAndCompare("select sum(l_partkey) from lineitem where l_partkey < 2000") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select sum(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("select sum(cast (l_quantity as DECIMAL(22, 2))) from lineitem") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }

    // Test the situation that precision + 4 of input decimal value exceeds 38.
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(36, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("min and max") {
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey) from lineitem where l_partkey < 2000") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("min_by/max_by") {
    withSQLConf(("spark.sql.leafNodeDefaultParallelism", "2")) {
      runQueryAndCompare(
        "select min_by(a, b), max_by(a, b) from " +
          "values (5, 6), (null, 11), (null, 5) test(a, b)") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("groupby") {
    val df = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("group sets") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey ") { _ => }
  }

  test("stddev_samp") {
    runQueryAndCompare("""
                         |select stddev_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, stddev_samp(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select stddev_samp(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("stddev_pop") {
    runQueryAndCompare("""
                         |select stddev_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, stddev_pop(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select stddev_pop(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("var_samp") {
    runQueryAndCompare("""
                         |select var_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, var_samp(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select var_samp(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("var_pop") {
    runQueryAndCompare("""
                         |select var_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, var_pop(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select var_pop(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("bit_and bit_or bit_xor") {
    val bitAggs = Seq("bit_and", "bit_or", "bit_xor")
    for (func <- bitAggs) {
      runQueryAndCompare(s"""
                            |select $func(l_linenumber) from lineitem
                            |group by l_orderkey;
                            |""".stripMargin) {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
      runQueryAndCompare(s"select $func(l_linenumber), count(distinct l_partkey) from lineitem") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }
    }
  }

  test("corr covar_pop covar_samp") {
    runQueryAndCompare("""
                         |select corr(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select corr(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select covar_pop(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_pop(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select covar_samp(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_samp(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("regr_r2") {
    runQueryAndCompare("""
                         |select regr_r2(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_r2(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  testWithMinSparkVersion("regr_slope", "3.4") {
    runQueryAndCompare("""
                         |select regr_slope(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_slope(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  testWithMinSparkVersion("regr_intercept", "3.4") {
    runQueryAndCompare("""
                         |select regr_intercept(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_intercept(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  testWithMinSparkVersion("regr_sxy regr_sxx regr_syy", "3.4") {
    runQueryAndCompare("""
                         |select regr_sxy(l_quantity, l_tax) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_sxy(l_quantity, l_tax), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select regr_sxx(l_quantity, l_tax) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_sxx(l_quantity, l_tax), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select regr_syy(l_quantity, l_tax) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select regr_syy(l_quantity, l_tax), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("first") {
    runQueryAndCompare(s"""
                          |select first(l_linenumber), first(l_linenumber, true) from lineitem;
                          |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select first_value(l_linenumber), first_value(l_linenumber, true) from lineitem
         |group by l_orderkey;
         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select first(l_linenumber), first(l_linenumber, true), count(distinct l_partkey)
         |from lineitem
         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("last") {
    runQueryAndCompare(s"""
                          |select last(l_linenumber), last(l_linenumber, true) from lineitem;
                          |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select last_value(l_linenumber), last_value(l_linenumber, true)
                          |from lineitem
                          |group by l_orderkey;
                          |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select last(l_linenumber), last(l_linenumber, true), count(distinct l_partkey)
         |from lineitem
         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("approx_count_distinct") {
    runQueryAndCompare(
      """
        |select approx_count_distinct(l_shipmode), approx_count_distinct(l_discount) from lineitem;
        |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select approx_count_distinct(l_discount), count(distinct l_orderkey) from lineitem") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2024-08-22 10:10:10.010")
        val t2 = Timestamp.valueOf("2014-12-31 00:00:00.012")
        val t3 = Timestamp.valueOf("1968-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select approx_count_distinct(t) from view") {
          checkGlutenPlan[HashAggregateExecTransformer]
        }
    }
  }

  test("max_by") {
    runQueryAndCompare(s"""
                          |select max_by(l_linenumber, l_comment) from lineitem;
                          |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select max_by(distinct l_linenumber, l_comment)
                          |from lineitem
                          |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("min_by") {
    runQueryAndCompare(s"""
                          |select min_by(l_linenumber, l_comment) from lineitem;
                          |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select min_by(distinct l_linenumber, l_comment)
                          |from lineitem
                          |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("distinct functions") {
    runQueryAndCompare("SELECT sum(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT sum(DISTINCT l_partkey), count(*), sum(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT avg(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT avg(DISTINCT l_partkey), count(*), avg(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT count(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT count(DISTINCT l_partkey), count(*), count(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT stddev_samp(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT stddev_samp(DISTINCT l_partkey), count(*), " +
        "stddev_samp(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT stddev_pop(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT stddev_pop(DISTINCT l_partkey), count(*), " +
        "stddev_pop(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT var_samp(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT var_samp(DISTINCT l_partkey), count(*), " +
        "var_samp(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT var_pop(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT var_pop(DISTINCT l_partkey), count(*), " +
        "var_pop(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT corr(DISTINCT l_partkey, l_suppkey)," +
        "corr(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT corr(DISTINCT l_partkey, l_suppkey)," +
        "count(*), corr(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
        "covar_pop(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
        "count(*), covar_pop(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
        "covar_samp(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
        "count(*), covar_samp(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT collect_list(DISTINCT n_name), count(*), collect_list(n_name) FROM nation") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test collect_set") {
    runQueryAndCompare("SELECT array_sort(collect_set(l_partkey)) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 2)
        }
    }

    runQueryAndCompare(
      """
        |SELECT array_sort(collect_set(l_suppkey)), array_sort(collect_set(l_partkey))
        |FROM lineitem
        |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 2)
        }
    }

    runQueryAndCompare(
      "SELECT count(distinct l_suppkey), array_sort(collect_set(l_partkey)) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test collect_set/collect_list with null") {
    import testImplicits._

    withTempView("collect_tmp") {
      Seq((1, null), (1, "a"), (2, null), (3, null), (3, null), (4, "b"))
        .toDF("c1", "c2")
        .createOrReplaceTempView("collect_tmp")

      // basic test
      runQueryAndCompare("SELECT collect_set(c2), collect_list(c2) FROM collect_tmp GROUP BY c1") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }

      // test pre project and post project
      runQueryAndCompare("""
                           |SELECT
                           |size(collect_set(if(c2 = 'a', 'x', 'y'))) as x,
                           |size(collect_list(if(c2 = 'a', 'x', 'y'))) as y
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }

      // test distinct
      runQueryAndCompare(
        "SELECT collect_set(c2), collect_list(distinct c2) FROM collect_tmp GROUP BY c1") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }

      // test distinct + pre project and post project
      runQueryAndCompare("""
                           |SELECT
                           |size(collect_set(if(c2 = 'a', 'x', 'y'))),
                           |size(collect_list(distinct if(c2 = 'a', 'x', 'y')))
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }

      // test cast array to string
      runQueryAndCompare("""
                           |SELECT
                           |cast(collect_set(c2) as string),
                           |cast(collect_list(c2) as string)
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }
    }
  }

  // Used for testing aggregate fallback
  sealed trait FallbackMode
  case object Offload extends FallbackMode
  case object FallbackPartial extends FallbackMode
  case object FallbackFinal extends FallbackMode
  case object FallbackAll extends FallbackMode

  List(Offload, FallbackPartial, FallbackFinal, FallbackAll).foreach {
    mode =>
      test(s"test fallback collect_set/collect_list with null, $mode") {
        mode match {
          case Offload => doTest()
          case FallbackPartial =>
            FallbackInjects.fallbackOn {
              case agg: BaseAggregateExec =>
                agg.aggregateExpressions.exists(_.mode == Partial)
            } {
              doTest()
            }
          case FallbackFinal =>
            FallbackInjects.fallbackOn {
              case agg: BaseAggregateExec =>
                agg.aggregateExpressions.exists(_.mode == Final)
            } {
              doTest()
            }
          case FallbackAll =>
            FallbackInjects.fallbackOn { case _: BaseAggregateExec => true } {
              doTest()
            }
        }

        def doTest(): Unit = {
          withTempView("collect_tmp") {
            Seq((1, null), (1, "a"), (2, null), (3, null), (3, null), (4, "b"))
              .toDF("c1", "c2")
              .createOrReplaceTempView("collect_tmp")

            // basic test
            runQueryAndCompare(
              "SELECT collect_set(c2), collect_list(c2) FROM collect_tmp GROUP BY c1") { _ => }

            // test pre project and post project
            runQueryAndCompare("""
                                 |SELECT
                                 |size(collect_set(if(c2 = 'a', 'x', 'y'))) as x,
                                 |size(collect_list(if(c2 = 'a', 'x', 'y'))) as y
                                 |FROM collect_tmp GROUP BY c1
                                 |""".stripMargin) { _ => }

            // test distinct
            runQueryAndCompare(
              "SELECT collect_set(c2), collect_list(distinct c2) FROM collect_tmp GROUP BY c1") {
              _ =>
            }

            // test distinct + pre project and post project
            runQueryAndCompare("""
                                 |SELECT
                                 |size(collect_set(if(c2 = 'a', 'x', 'y'))),
                                 |size(collect_list(distinct if(c2 = 'a', 'x', 'y')))
                                 |FROM collect_tmp GROUP BY c1
                                 |""".stripMargin) { _ => }

            // test cast array to string
            runQueryAndCompare("""
                                 |SELECT
                                 |cast(collect_set(c2) as string),
                                 |cast(collect_list(c2) as string)
                                 |FROM collect_tmp GROUP BY c1
                                 |""".stripMargin) { _ => }
          }
        }
      }
  }

  test("count(1)") {
    runQueryAndCompare(
      """
        |select count(1) from (select * from values(1,2) as data(a,b) group by a,b union all
        |select * from values(2,3),(3,4) as data(c,d) group by c,d);
        |""".stripMargin) {
      df =>
        assert(
          getExecutedPlan(df).count(plan => plan.isInstanceOf[HashAggregateExecTransformer]) >= 2)
    }
  }

  test("bind reference failed when subquery in agg expressions") {
    runQueryAndCompare("""
                         |select sum(if(c > (select sum(a) from values (1), (-1) AS tab(a)), 1, -1))
                         |from values (5), (-10), (15) AS tab(c);
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))

    runQueryAndCompare("""
                         |select sum(if(c > (select sum(a) from values (1), (-1) AS tab(a)), 1, -1))
                         |from values (1L, 5), (1L, -10), (2L, 15) AS tab(sum, c) group by sum;
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))
  }

  test("collect_list null inputs") {
    runQueryAndCompare("""
                         |select collect_list(a) from values (1), (-1), (null) AS tab(a)
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))
  }

  test("collect_list with different primitive types") {
    // Test with different primitive types to ensure comprehensive coverage

    // Boolean type
    withTempView("bool_tmp") {
      Seq((1, true), (1, false), (2, true), (2, false))
        .toDF("key", "val")
        .createOrReplaceTempView("bool_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM bool_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Byte (TinyInt) type
    withTempView("byte_tmp") {
      Seq((1, 1.toByte), (1, 2.toByte), (2, 127.toByte), (2, (-128).toByte))
        .toDF("key", "val")
        .createOrReplaceTempView("byte_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM byte_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Short type
    withTempView("short_tmp") {
      Seq((1, 100.toShort), (1, 200.toShort), (2, 32767.toShort), (2, (-32768).toShort))
        .toDF("key", "val")
        .createOrReplaceTempView("short_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM short_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Integer type
    withTempView("int_tmp") {
      Seq((1, 100), (1, 200), (2, Int.MaxValue), (2, Int.MinValue))
        .toDF("key", "val")
        .createOrReplaceTempView("int_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM int_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Long type
    withTempView("long_tmp") {
      Seq((1, 100L), (1, 200L), (2, Long.MaxValue), (2, Long.MinValue))
        .toDF("key", "val")
        .createOrReplaceTempView("long_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM long_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Float type
    withTempView("float_tmp") {
      Seq((1, 1.5f), (1, 2.5f), (2, Float.MaxValue), (2, Float.MinValue))
        .toDF("key", "val")
        .createOrReplaceTempView("float_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM float_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Double type
    withTempView("double_tmp") {
      Seq((1, 1.5d), (1, 2.5d), (2, Double.MaxValue), (2, Double.MinValue))
        .toDF("key", "val")
        .createOrReplaceTempView("double_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM double_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Decimal type
    withTempView("decimal_tmp") {
      val data = Seq(
        (1, BigDecimal("123.45")),
        (1, BigDecimal("678.90")),
        (2, BigDecimal("999.99")),
        (2, BigDecimal("-999.99"))
      )
      data.toDF("key", "val").createOrReplaceTempView("decimal_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM decimal_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // String type
    withTempView("string_tmp") {
      Seq((1, "hello"), (1, "world"), (2, "foo"), (2, "bar"), (2, ""))
        .toDF("key", "val")
        .createOrReplaceTempView("string_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM string_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Date type
    withTempView("date_tmp") {
      Seq(
        (1, Date.valueOf("2023-01-01")),
        (1, Date.valueOf("2023-06-15")),
        (2, Date.valueOf("2024-12-31")),
        (2, Date.valueOf("2020-01-01"))
      ).toDF("key", "val").createOrReplaceTempView("date_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM date_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Timestamp type
    withTempView("ts_tmp") {
      Seq(
        (1, Timestamp.valueOf("2023-01-01 10:00:00")),
        (1, Timestamp.valueOf("2023-06-15 15:30:00")),
        (2, Timestamp.valueOf("2024-12-31 23:59:59")),
        (2, Timestamp.valueOf("2020-01-01 00:00:00"))
      ).toDF("key", "val").createOrReplaceTempView("ts_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM ts_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Binary type
    withTempView("binary_tmp") {
      Seq(
        (1, Array[Byte](1, 2, 3)),
        (1, Array[Byte](4, 5, 6)),
        (2, Array[Byte](7, 8, 9)),
        (2, Array[Byte](10, 11, 12))
      ).toDF("key", "val").createOrReplaceTempView("binary_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM binary_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Mixed nulls across all groups - use SQL literal to avoid Seq type inference issues
    withTempView("null_mix_tmp") {
      spark
        .sql("""
               |SELECT * FROM VALUES
               |(1, CAST(NULL AS INT)),
               |(1, CAST(NULL AS INT)),
               |(2, 100),
               |(2, CAST(NULL AS INT)),
               |(3, CAST(NULL AS INT))
               |AS null_mix_tmp(key, val)
               |""".stripMargin)
        .createOrReplaceTempView("null_mix_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM null_mix_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("collect_list with nested complex types") {
    import org.apache.spark.sql.Row

    // Array of integers
    withTempView("array_tmp") {
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(
            Seq(
              Row(1, Seq(1, 2, 3)),
              Row(1, Seq(4, 5)),
              Row(2, Seq(6, 7, 8, 9)),
              Row(2, null)
            )),
          StructType(
            Seq(
              StructField("key", IntegerType),
              StructField("val", ArrayType(IntegerType), nullable = true)
            ))
        )
        .createOrReplaceTempView("array_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM array_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Struct type
    withTempView("struct_tmp") {
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(
            Seq(
              Row(1, Row(100, "a")),
              Row(1, Row(200, "b")),
              Row(2, Row(300, "c")),
              Row(2, null)
            )),
          StructType(
            Seq(
              StructField("key", IntegerType),
              StructField(
                "val",
                StructType(
                  Seq(
                    StructField("id", IntegerType),
                    StructField("name", StringType)
                  )),
                nullable = true)
            ))
        )
        .createOrReplaceTempView("struct_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM struct_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Map type
    withTempView("map_tmp") {
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(
            Seq(
              Row(1, Map("a" -> 1, "b" -> 2)),
              Row(1, Map("c" -> 3)),
              Row(2, Map("d" -> 4, "e" -> 5, "f" -> 6)),
              Row(2, null)
            )),
          StructType(
            Seq(
              StructField("key", IntegerType),
              StructField("val", MapType(StringType, IntegerType), nullable = true)
            ))
        )
        .createOrReplaceTempView("map_tmp")
      runQueryAndCompare("SELECT key, collect_list(val) FROM map_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("collect_list with large result sets") {
    // Test collect_list with larger number of elements
    withTempView("large_tmp") {
      val data = (1 to 1000).map(i => (i % 10, i))
      data.toDF("key", "val").createOrReplaceTempView("large_tmp")
      runQueryAndCompare("SELECT key, size(collect_list(val)) FROM large_tmp GROUP BY key") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("collect_list global aggregation without group by") {
    // Test global aggregation (no GROUP BY)
    withTempView("global_tmp") {
      Seq[java.lang.Integer](1, 2, 3, 4, 5, null)
        .toDF("val")
        .createOrReplaceTempView("global_tmp")
      runQueryAndCompare("SELECT collect_list(val) FROM global_tmp") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }

    // Empty table
    withTempView("empty_tmp") {
      spark
        .createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(StructField("key", IntegerType), StructField("val", IntegerType)))
        )
        .createOrReplaceTempView("empty_tmp")
      runQueryAndCompare("SELECT collect_list(val) FROM empty_tmp") {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("collect_list fallback for unsupported types") {
    // Test that collect_list properly falls back to Spark for unsupported types
    // TimestampNTZ (Timestamp without timezone) is not supported by Velox
    // TimestampNTZType is only available in Spark 3.4+
    if (isSparkVersionGE("3.4")) {
      withTempView("timestamp_ntz_tmp") {
        // Use SQL to create TimestampNTZ data to avoid compile-time dependency
        spark
          .sql("""
                 |SELECT * FROM VALUES
                 |  (1, TIMESTAMP_NTZ'2023-01-01 10:30:00'),
                 |  (1, TIMESTAMP_NTZ'2023-06-15 14:45:30'),
                 |  (2, TIMESTAMP_NTZ'2024-12-31 23:59:59'),
                 |  (2, NULL)
                 |AS timestamp_ntz_tmp(key, val)
                 |""".stripMargin)
          .createOrReplaceTempView("timestamp_ntz_tmp")

        // TimestampNTZ should trigger fallback - verify no native HashAggregateExecTransformer
        runQueryAndCompare(
          "SELECT key, collect_list(val) FROM timestamp_ntz_tmp GROUP BY key",
          noFallBack = false) {
          df =>
            assert(
              !getExecutedPlan(df).exists(_.isInstanceOf[HashAggregateExecTransformer]),
              "Expected fallback to Spark for TimestampNTZ type, but found native HashAggregate"
            )
        }
      }
    }
  }

  test("skewness") {
    runQueryAndCompare("""
                         |select skewness(l_partkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select skewness(l_partkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("kurtosis") {
    runQueryAndCompare("""
                         |select kurtosis(l_partkey) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select kurtosis(l_partkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("complex type with null") {
    val jsonStr = """{"txn":{"appId":"txnId","version":0,"lastUpdated":null}}"""
    val jsonSchema = StructType(
      Seq(
        StructField(
          "txn",
          StructType(
            Seq(
              StructField("appId", StringType, true),
              StructField("lastUpdated", LongType, true),
              StructField("version", LongType, true))),
          true)))
    spark.read.schema(jsonSchema).json(Seq(jsonStr).toDS).createOrReplaceTempView("t1")
    runQueryAndCompare("select collect_set(txn), min(txn), max(txn) from t1") {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
  }

  test("drop redundant partial sort which has pre-project when offload sortAgg") {
    // Spark 3.2 does not have this configuration, but it does not affect the test results.
    withSQLConf("spark.sql.test.forceApplySortAggregate" -> "true") {
      withTempView("t1") {
        Seq((-1, 2), (-1, 3), (2, 3), (3, 4), (-3, 5), (4, 5))
          .toDF("c1", "c2")
          .createOrReplaceTempView("t1")
        runQueryAndCompare("select c2, sum(if(c1<0,0,c1)) from t1 group by c2") {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[HashAggregateExecTransformer]
                  }) == 2)
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[SortExecTransformer]
                  }) == 0)
            }
        }
      }
    }
  }
}

class VeloxAggregateFunctionsDefaultSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Disable flush. This may cause spilling to happen on partial aggregations.
      .set(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "false")
  }

  test("flushable aggregate rule") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      runQueryAndCompare(VeloxAggregateFunctionsSuite.GROUP_SETS_TEST_SQL) {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            !executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }
}

class VeloxAggregateFunctionsFlushSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // To test flush behaviors, set low flush threshold to ensure flush happens.
      .set(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "true")
      .set(VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_PCT.key, "1")
      .set(VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_ROWS.key, "10")
  }

  test("flushable aggregate rule") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select distinct l_partkey from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate rule - agg input already distributed by keys") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare(
        "select * from (select distinct l_orderkey,l_partkey from lineitem) a" +
          " inner join (select l_orderkey from lineitem limit 10) b" +
          " on a.l_orderkey = b.l_orderkey limit 10") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate decimal sum") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select sum(l_quantity) from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate rule - double sum when floatingPointMode is strict") {
    withSQLConf(
      VeloxConfig.MAX_PARTIAL_AGGREGATION_MEMORY.key -> "100",
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key -> "false",
      GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2",
      VeloxConfig.FLOATING_POINT_MODE.key -> "strict"
    ) {
      withTempView("t1") {
        import testImplicits._
        Seq((24.621d, 1), (12.14d, 1), (0.169d, 1), (6.865d, 1), (1.879d, 1), (16.326d, 1))
          .toDF("c1", "c2")
          .createOrReplaceTempView("t1")
        runQueryAndCompare("select c2, cast(sum(c1) as bigint) from t1 group by c2") {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[RegularHashAggregateExecTransformer]
                  }) == 2)
            }
        }
      }
    }
  }

  test("flushable aggregate rule - double sum when floatingPointMode is loose") {
    withSQLConf(
      VeloxConfig.FLOATING_POINT_MODE.key -> "loose"
    ) {
      withTempView("t1") {
        import testImplicits._
        Seq((24.6d, 1), (12.1d, 1), (0.1d, 1), (6.8d, 1), (1.8d, 1), (16.3d, 1))
          .toDF("c1", "c2")
          .createOrReplaceTempView("t1")
        runQueryAndCompare("select c2, cast(sum(c1) as bigint) from t1 group by c2") {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[RegularHashAggregateExecTransformer]
                  }) == 1)
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[FlushableHashAggregateExecTransformer]
                  }) == 1)
            }
        }
      }
    }
  }
}

object VeloxAggregateFunctionsSuite {
  val GROUP_SETS_TEST_SQL: String =
    "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
      "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
      "order by l_orderkey, l_partkey "
}
