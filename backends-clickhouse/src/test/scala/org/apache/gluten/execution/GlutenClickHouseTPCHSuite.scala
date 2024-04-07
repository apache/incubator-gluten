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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, TestUtils}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.types.{DecimalType, StructType}

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCHSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 1)

        assert(scanExec(0).nodeName.startsWith("Scan mergetree"))

        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: SortExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(3) {
        df =>
          val shjBuildLeft = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 2)
      }
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) {
        df =>
          val bhjRes = df.queryExecution.executedPlan.collect {
            case bhj: BroadcastHashJoinExecTransformerBase => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(8) { df => }
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11) { df => }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df => }
  }

  test("TPCH Q14") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(14) { df => }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df => }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false) { df => }
  }

  test("TPCH Q17") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(17) { df => }
    }
  }

  test("TPCH Q18") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(18) { df => }
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df => }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df => }
  }

  test("TPCH Q21") {
    runTPCHQuery(21, noFallBack = false) { df => }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }

  test("test 'select count(*) from table'") {
    // currently, it can't support 'select count(*)' for non-partitioned tables.
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
  }

  test("test 'select count(*)'") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |where l_quantity < 24
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 275436L)
  }

  test("test 'select global/local limit'") {
    val result = runSql("""
                          |select * from (
                          | select * from lineitem limit 10
                          |) where l_suppkey != 0 limit 100;
                          |""".stripMargin) { _ => }
    assert(result.size == 10)
  }

  test("test 'function explode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, explode(array(l_returnflag, l_linestatus)),
                          |  l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'function posexplode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, posexplode(array(l_returnflag, l_linestatus)),
                          |  l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'lateral view explode(array)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, l_suppkey, col1, col2 from lineitem
                          |  lateral view explode(array(l_returnflag, l_linestatus)) as col1
                          |  lateral view explode(array(l_shipmode, l_comment)) as col2)
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 2402288L)
  }

  test("test 'lateral view posexplode(array)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey, l_suppkey, pos1, col1, pos2, col2 from lineitem
               |  lateral view posexplode(array(l_returnflag, l_linestatus)) as pos1, col1
               |  lateral view posexplode(array(l_shipmode, l_comment)) as pos2, col2)
               |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 2402288L)
  }

  test("test 'function explode(map)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey,
                          |    explode(map('returnflag', l_returnflag, 'linestatus', l_linestatus)),
                          |    l_suppkey from lineitem);
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'function posexplode(map)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey,
               |    posexplode(map('returnflag', l_returnflag, 'linestatus', l_linestatus)),
               |    l_suppkey from lineitem);
               |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'lateral view explode(map)'") {
    val result = runSql("""
                          |select count(*) from (
                          |  select l_orderkey, l_suppkey, k1, v1, k2, v2 from lineitem
                          |  lateral view
                          |    explode(map('returnflag', l_returnflag, 'linestatus', l_linestatus))
                          |    as k1, v1
                          |  lateral view
                          |    explode(map('orderkey', l_orderkey, 'partkey', l_partkey))
                          |    as k2, v2
                          |)
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 2402288L)
  }

  test("test 'lateral view posexplode(map)'") {
    val result =
      runSql("""
               |select count(*) from (
               |  select l_orderkey, l_suppkey, p1, k1, v1, p2, k2, v2 from lineitem
               |  lateral view
               |    posexplode(map('returnflag', l_returnflag, 'linestatus', l_linestatus))
               |    as p1, k1, v1
               |  lateral view
               |    posexplode(map('orderkey', l_orderkey, 'partkey', l_partkey))
               |    as p2, k2, v2
               |)
               |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 2402288L)
  }

  test("test 'select count(1)'") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |where l_quantity < 20
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 227302L)
  }

  test("test 'select count(1)' with empty columns to read") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select count(*)' with empty columns to read") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select sum(2)' with empty columns to read") {
    val result = runSql("""
                          |select sum(2) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'select 1' with empty columns to read") {
    val result = runSql("""
                          |select 1 from lineitem limit 2
                          |""".stripMargin) { _ => }
    assert(result.size == 2)
    assert(result(0).getInt(0) == 1 && result(1).getInt(0) == 1)
  }

  test("test 'order by'") {
    val result = runSql("""
                          |select l_suppkey from lineitem
                          |where l_orderkey < 3 order by l_partkey / 2
                          |""".stripMargin) { _ => }
    assert(result.size == 7)
    val expected =
      Seq(Row(465.0), Row(67.0), Row(160.0), Row(371.0), Row(732.0), Row(138.0), Row(785.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("test 'order by' two keys") {
    runSql("""
             |select n_nationkey, n_name, n_regionkey from nation
             |order by n_name, n_regionkey + 1
             |""".stripMargin) {
      df =>
        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: SortExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
        val result = df.take(3)
        val expected =
          Seq(Row(0, "ALGERIA", 0), Row(1, "ARGENTINA", 1), Row(2, "BRAZIL", 1))
        TestUtils.compareAnswers(result, expected)
    }
  }

  test("test 'order by limit'") {
    runSql("""
             |select n_nationkey from nation order by n_nationkey limit 5
             |""".stripMargin) {
      df =>
        val sortExec = df.queryExecution.executedPlan.collect {
          case sortExec: TakeOrderedAndProjectExecTransformer => sortExec
        }
        assert(sortExec.size == 1)
        val result = df.collect()
        val expectedResult = Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
        TestUtils.compareAnswers(result, expectedResult)
    }
  }

  test("test 'function space'") {
    val result = runSql("""
                          | select
                          | space(3),
                          | space(0),
                          | space(NULL),
                          | space(3/3.00f)
                          | from lineitem limit 1
                          |""".stripMargin) { _ => }
    assert(result(0).getString(0).equals("   "))
    assert(result(0).getString(1).equals(""))
    assert(result(0).getString(2) == null)
    assert(result(0).getString(3).equals(" "))
  }

  test("test 'ISSUE https://github.com/Kyligence/ClickHouse/issues/225'") {
    val result = runSql("""
                          |SELECT
                          |cast(1.11 as decimal(20, 3)),
                          |cast(1.123456789 as decimal(20,9)),
                          |cast(123456789.123456789 as decimal(30,9)),
                          |cast(1.12345678901234567890123456789 as decimal(38,29)),
                          |cast(123456789.123456789012345678901234567 as decimal(38,27)),
                          |cast(123456789.123456789012345678901234567 as decimal(38,28)) + 0.1,
                          |array(cast(123456789.123456789012345678901234567 as decimal(38,27)))
                          |FROM lineitem
                          |WHERE l_shipdate <= date'1998-09-02' - interval 1 day limit 1
                          |""".stripMargin) { _ => }
    assert(result.length == 1)
    val expectedResult = Seq(
      Row(
        new java.math.BigDecimal("1.110"),
        new java.math.BigDecimal("1.123456789"),
        new java.math.BigDecimal("123456789.123456789"),
        new java.math.BigDecimal("1.12345678901234567890123456789"),
        new java.math.BigDecimal("123456789.123456789012345678901234567"),
        new java.math.BigDecimal("123456789.223456789012345678901234567"),
        Seq(new java.math.BigDecimal("123456789.123456789012345678901234567"))
      ))
    TestUtils.compareAnswers(result, expectedResult)
  }

  test("test decimal128") {
    val struct = Row(new java.math.BigDecimal("123456789.123456789012345678901234567"))
    val data = sparkContext.parallelize(
      Seq(
        Row(new java.math.BigDecimal("123456789.123456789012345678901234566"), struct)
      ))

    val schema = new StructType()
      .add("a", DecimalType(38, 27))
      .add(
        "b",
        new StructType()
          .add("b1", DecimalType(38, 27)))

    val df2 = spark.createDataFrame(data, schema)
    TestUtils.compareAnswers(df2.select("b").collect(), Seq(Row(struct)))
    TestUtils.compareAnswers(
      df2.select("a").collect(),
      Seq(Row(new java.math.BigDecimal("123456789.123456789012345678901234566"))))
  }

  test("test 'sum/count/max/min from empty table'") {
    spark.sql(
      """
        | create table test_tbl(id bigint, name string) using parquet;
        |""".stripMargin
    )
    val sql1 = "select count(1), sum(id), max(id), min(id), 'abc' as x from test_tbl"
    val sql2 =
      "select count(1) as cnt, sum(id) as sum, max(id) as max, min(id) as min from test_tbl"
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
    spark.sql("drop table test_tbl")
  }

  test("test 'function json_tuple'") {
    val result = runSql(
      """
        | select
        | json_tuple('{"hello":"world", "hello1":"world1", "hello2":["a","b"]}', 'hello', 'hello1','hello2', 'hello3')
        | from lineitem where l_linenumber = 3 and l_orderkey < 3 limit 1
        |""".stripMargin) { _ => }
    assert(result(0).getString(0).equals("world"))
    assert(result(0).getString(1).equals("world1"))
    assert(result(0).getString(2).equals("[\"a\",\"b\"]"))
    assert(result(0).isNullAt(3))
  }

  test("GLUTEN-3271: Bug fix arrayElement from split") {
    val table_create_sql =
      """
        | create table test_tbl_3271(id bigint, data string) using parquet;
        |""".stripMargin
    val table_drop_sql = "drop table test_tbl_3271";
    val data_insert_sql = "insert into test_tbl_3271 values(1, 'ab')"
    val select_sql_1 = "select id, split(data, ',')[1] from test_tbl_3271 where id = 1"
    val select_sql_2 = "select id, element_at(split(data, ','), 2) from test_tbl_3271 where id = 1"
    val select_sql_3 = "select id, element_at(map(id, data), 1) from test_tbl_3271 where id = 1"
    spark.sql(table_create_sql);
    spark.sql(data_insert_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_2, true, { _ => })
    compareResultsAgainstVanillaSpark(select_sql_3, true, { _ => })

    spark.sql(table_drop_sql)
  }
}
// scalastyle:off line.size.limit
