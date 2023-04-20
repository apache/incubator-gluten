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
package io.glutenproject.execution

import io.glutenproject.extension.GlutenPlan

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.functions.{col, rand, when}

import java.io.File

class GlutenClickHouseTPCHParquetSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
  }

  override protected val createNullableTables = true

  override protected def createTPCHNullableTables(): Unit = {

    // first process the parquet data to:
    // 1. make every column nullable in schema (optional rather than required)
    // 2. salt some null values randomly
    val saltedTablesPath = tablesPath + "-salted"
    withSQLConf(vanillaSparkConfs(): _*) {
      Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
        .map(
          tableName => {
            val originTablePath = tablesPath + "/" + tableName
            val df = spark.read.parquet(originTablePath)
            var salted_df: Option[DataFrame] = None
            for (c <- df.schema) {
              salted_df = Some((salted_df match {
                case Some(x) => x
                case None => df
              }).withColumn(c.name, when(rand() < 0.1, null).otherwise(col(c.name))))
            }

            val currentSaltedTablePath = saltedTablesPath + "/" + tableName
            val file = new File(currentSaltedTablePath)
            if (file.exists()) {
              file.delete()
            }
            salted_df.get.write.parquet(currentSaltedTablePath)
          })
    }

    val customerData = saltedTablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING PARQUET LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = saltedTablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS lineitem (
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string)
                 | USING PARQUET LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = saltedTablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = saltedTablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING PARQUET LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = saltedTablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING PARQUET LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = saltedTablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING PARQUET LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = saltedTablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING PARQUET LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = saltedTablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING PARQUET LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.size == 8)
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => true
        }
        assert(scanExec.size == 1)
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
            case shj: ShuffledHashJoinExecTransformer if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 1)
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
            case bhj: BroadcastHashJoinExecTransformer => bhj
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
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
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
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(14) { df => }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df => }
  }

  // see issue https://github.com/Kyligence/ClickHouse/issues/93
  ignore("TPCH Q16") {
    runTPCHQuery(16) { df => }
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

  test("test 'function pmod'") {
    val df = runQueryAndCompare(
      "select pmod(-10, id+10) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function ascii'") {
    val df = runQueryAndCompare(
      "select ascii(cast(id as String)) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function rand'") {
    runSql("select rand(), rand(1), rand(null) from range(10)")(
      checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'function date_add/date_sub/datediff'") {
    val df = runQueryAndCompare(
      "select l_shipdate, l_commitdate, " +
        "date_add(l_shipdate, 1), date_add(l_shipdate, -1), " +
        "date_sub(l_shipdate, 1), date_sub(l_shipdate, -1), " +
        "datediff(l_shipdate, l_commitdate), datediff(l_commitdate, l_shipdate) " +
        "from lineitem order by l_shipdate, l_commitdate limit 1"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'function remainder'") {
    val df = runQueryAndCompare(
      "select l_orderkey, l_partkey, l_orderkey % l_partkey, l_partkey % l_orderkey " +
        "from lineitem order by l_orderkey desc, l_partkey desc limit 1"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("coalesce") {
    var df = runQueryAndCompare(
      "select l_orderkey, coalesce(l_comment, 'default_val') " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), l_comment, 'default_val') " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), cast(null as string), l_comment) " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(cast(null as string), cast(null as string), 1, 2) " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, " +
        "coalesce(cast(null as string), cast(null as string), cast(null as string)) "
        + "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 5)
  }

  test("test 'function from_unixtime'") {
    val df = runQueryAndCompare(
      "select l_orderkey, from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss'), " +
        "from_unixtime(l_orderkey, 'yyyy-MM-dd') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  ignore("test 'aggregate function collect_list'") {
    val df = runQueryAndCompare(
      "select l_orderkey,from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function regexp_replace'") {
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1') " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 1) " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'function to_unix_timestamp/unix_timestamp'") {
    runQueryAndCompare(
      "select to_unix_timestamp(concat(cast(l_shipdate as String), ' 00:00:00')) " +
        "from lineitem order by l_shipdate limit 10;")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select unix_timestamp(concat(cast(l_shipdate as String), ' 00:00:00')) " +
        "from lineitem order by l_shipdate limit 10;")(checkOperatorMatch[ProjectExecTransformer])
  }

  // see issue https://github.com/Kyligence/ClickHouse/issues/93
  ignore("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }

  test("window row_number") {
    val sql =
      """
        |select row_number() over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum 1") {
    val sql =
      """
        |select sum(n_nationkey + 1) over (partition by n_regionkey order by n_nationkey)
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum 2") {
    val sql =
      """
        |select sum(n_nationkey + 1) over (partition by n_regionkey order by n_name)
        |from nation
        |order by n_regionkey, n_name
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window sum const") {
    val sql =
      """
        |select n_regionkey, sum(2) over (partition by n_regionkey)
        |from nation
        |order by n_regionkey
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[WindowExecTransformer])
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window max") {
    val sql =
      """
        |select max(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window min") {
    val sql =
      """
        |select min(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window avg") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window offset preceding") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 3
        |preceding and current row) from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window offset following") {
    val sql =
      """
        |select avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between
        |current row and 3 following) as x from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window range") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        |  sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range
        |  between unbounded preceding and current row) as n_sum
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_sum
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("windows") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        | rank() over (partition by n_regionkey order by n_nationkey) as n_rank,
        | sum(n_nationkey) over (partition by n_regionkey order by n_nationkey) as n_sum
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_rank
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window rank") {
    val sql =
      """
        |select n_nationkey, n_name, n_regionkey,
        | rank() over (partition by n_regionkey order by n_nationkey) as n_rank
        |from nation
        |order by n_regionkey,n_nationkey,n_name,n_rank
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead with default value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, 1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin

    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lag") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lag(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lag with default value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lag(n_nationkey, 1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window dense_rank") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | dense_rank(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_rank
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window first value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | first_value(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey)
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  ignore("window last value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | last_value(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey)
        |from nation
        |order by n_regionkey, n_nationkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with rollup") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(l_shipmode) as n from lineitem
        |group by l_shipdate, l_shipmode with rollup
        |order by l_shipdate, l_shipmode, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with cube") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(l_tax) as n from lineitem
        |group by l_shipdate, l_shipmode with cube
        |order by l_shipdate, l_shipmode, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("group with sets") {
    val sql =
      """
        |select l_shipdate, l_shipmode, count(1) as cnt from lineitem
        |group by grouping sets (l_shipdate, l_shipmode, (l_shipdate, l_shipmode))
        |order by l_shipdate, l_shipmode, cnt
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test 'position/locate'") {
    runQueryAndCompare(
      """
        |select position('D', l_shipinstruct, 0), position('', l_shipinstruct, 0),
        |position('I', l_shipinstruct, 5), position('IN', l_shipinstruct),
        |position('', l_shipinstruct), locate(l_returnflag, l_shipinstruct),
        |position(l_returnflag in l_shipinstruct), position('bar', 'foobarbar'),
        |position(l_returnflag, 'TENSTNTEST', 4), position('bar', 'foobarbar', 5),
        |position(l_returnflag, l_shipinstruct, l_linenumber + 11),
        |position(null, l_shipinstruct),
        |position(l_returnflag, null),
        |position(l_returnflag, l_shipinstruct, null),
        |position(l_returnflag, l_shipinstruct, 0),
        |position(l_returnflag, null, 0),
        |position(null, l_shipinstruct, 0),
        |position(null, null, 0),
        |position(l_returnflag, null, null),
        |position(null, l_shipinstruct, null),
        |position(null, null, null)
        |from lineitem
        |""".stripMargin
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test stddev_samp 1") {
    val sql =
      """
        |select stddev_samp(n_nationkey), stddev_samp(n_regionkey) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test stddev_samp 2") {
    val sql =
      """
        |select stddev_samp(l_orderkey), stddev_samp(l_quantity) from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("isNaN") {
    val sql =
      """
        |select isNaN(l_shipinstruct), isNaN(l_partkey), isNaN(l_discount)
        |from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test 'sequence'") {
    runQueryAndCompare(
      "select sequence(id, id+10), sequence(id+10, id), sequence(id, id+10, 3), " +
        "sequence(id+10, id, -3) from range(1)")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("Bug-398 collec_list failure") {
    val sql =
      """
        |select n_regionkey, collect_list(if(n_regionkey=0, n_name, null)) as t from nation group by n_regionkey
        |order by n_regionkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, df => {})
  }

  test("Test 'spark.gluten.enabled' false") {
    withSQLConf(("spark.gluten.enabled", "false")) {
      runTPCHQuery(2) {
        df =>
          val glutenPlans = df.queryExecution.executedPlan.collect {
            case glutenPlan: GlutenPlan => glutenPlan
          }
          assert(glutenPlans.isEmpty)
      }
    }
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String = tpchQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    // super.runTPCHQuery(queryNum, tpchQueries, queriesResults, compareResult)(customCheck)
    compareTPCHQueryAgainstVanillaSpark(queryNum, tpchQueries, customCheck)
  }
}
