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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.functions.{col, rand, when}
import org.apache.spark.sql.internal.SQLConf

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

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
      .set("spark.gluten.supported.scala.udfs", "my_add")
      .set("spark.gluten.supported.hive.udfs", "my_add")
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
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
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

  test("GLUTEN-2115: Fix wrong number of records shuffle written") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.adaptive.enabled", "true")
    ) {
      compareResultsAgainstVanillaSpark(
        """
          |select
          |    l_shipdate_grp l_shipdate,
          |    (lead(count(distinct l_suppkey), -1) over (order by l_shipdate_grp)) cc
          |from
          |    (select l_suppkey, EXTRACT(year from `l_shipdate`)  l_shipdate_grp from lineitem) t
          |group by l_shipdate_grp
          |order by l_shipdate_grp desc
          |limit 20
          |""".stripMargin,
        compareResult = true,
        _ => {}
      )
    }
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
    runQueryAndCompare(
      "select l_shipdate, l_commitdate, " +
        "date_add(l_shipdate, 1), date_add(l_shipdate, -1), " +
        "date_sub(l_shipdate, 1), date_sub(l_shipdate, -1), " +
        "datediff(l_shipdate, l_commitdate), datediff(l_commitdate, l_shipdate) " +
        "from lineitem order by l_shipdate, l_commitdate limit 1"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'function remainder'") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, l_orderkey % l_partkey, l_partkey % l_orderkey " +
        "from lineitem order by l_orderkey desc, l_partkey desc limit 1"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test positive/negative") {
    runQueryAndCompare(
      "select +n_nationkey, positive(n_nationkey), -n_nationkey, negative(n_nationkey) from nation"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  // TODO: enable when supports interval type
  ignore("test positive/negative with interval type") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select +interval 1 day, positive(interval 1 day), -interval 1 day, negative(interval 1 day)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_intersect") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select a from (select array_intersect(split(n_comment, ' '), split(n_comment, ' ')) as arr " +
          "from nation) lateral view explode(arr) as a order by a"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select a from (select array_intersect(array(null,1,2,3,null), array(3,5,1,null,null)) as arr) " +
          "lateral view explode(arr) as a order by a",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_intersect(array(null,1,2,3,null), cast(null as array<int>))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select a from (select array_intersect(array(array(1,2),array(3,4)), array(array(1,2),array(3,4))) as arr) " +
          "lateral view explode(arr) as a order by a",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_position") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_position(split(n_comment, ' '), 'final') from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_position(array(1,2,3,null), 1), array_position(array(1,2,3,null), null)," +
          "array_position(array(1,2,3,null), 5), array_position(array(1,2,3), 5), " +
          "array_position(array(1,2,3), 2), array_position(cast(null as array<int>), 1)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_contains") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_contains(split(n_comment, ' '), 'final') from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_contains(array(1,2,3,null), 1), array_contains(array(1,2,3,null), " +
          "cast(null as int)), array_contains(array(1,2,3,null), 5), array_contains(array(1,2,3), 5)," +
          "array_contains(array(1,2,3), 2), array_contains(cast(null as array<int>), 1)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test sort_array") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select sort_array(split(n_comment, ' ')) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select sort_array(split(n_comment, ' '), false) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select sort_array(array(1,3,2,null)), sort_array(array(1,2,3,null),false)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test coalesce") {
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

  test("test 'aggregate function collect_list'") {
    val df = runQueryAndCompare(
      "select l_orderkey,from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test find_in_set") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select find_in_set(null, 'a'), find_in_set('a', null), " +
          "find_in_set('a', 'a,b'), find_in_set('a', 'ab,ab')",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test 'function regexp_replace'") {
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1') " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 1) " +
        "from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("regexp_extract") {
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])') " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 0) " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("lpad") {
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80) " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80, '??') " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("rpad") {
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80) " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80, '??') " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test elt") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select elt(2, n_comment, n_regionkey) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
      runQueryAndCompare(
        "select elt(null, 'a', 'b'), elt(0, 'a', 'b'), elt(1, 'a', 'b'), elt(3, 'a', 'b')",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_max") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_max(split(n_comment, ' ')) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
      runQueryAndCompare(
        "select array_max(null), array_max(array(null)), array_max(array(1, 2, 3, null)), " +
          "array_max(array(1.0, 2.0, 3.0, null)), array_max(array('z', 't', 'abc'))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_min") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_min(split(n_comment, ' ')) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
      runQueryAndCompare(
        "select array_min(null), array_min(array(null)), array_min(array(1, 2, 3, null)), " +
          "array_min(array(1.0, 2.0, 3.0, null)), array_min(array('z', 't', 'abc'))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test slice function") {
    val sql =
      """
        |select slice(arr, 1, 5), slice(arr, 1, 100), slice(arr, -2, 5), slice(arr, 1, n_nationkey),
        |slice(null, 1, 2), slice(arr, null, 2), slice(arr, 1, null)
        |from (select split(n_comment, ' ') as arr, n_nationkey from nation) t
        |""".stripMargin
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test slice function with unexpected arguments") {
    def checkException(sql: String, expectedErrMsg: String): Unit = {
      val errMsg = intercept[SparkException] {
        spark.sql(sql).collect()
      }.getMessage

      if (errMsg == null) {
        fail(s"Expected null error message, but `$errMsg` found")
      } else if (!errMsg.contains(expectedErrMsg)) {
        fail(s"Expected error message is `$expectedErrMsg`, but `$errMsg` found")
      }
    }

    checkException(
      "select slice(split(n_comment, ' '), n_regionkey, 5) from nation",
      "Unexpected value for start")
    checkException(
      "select slice(split(n_comment, ' '), 1, -5) from nation",
      "Unexpected value for length")
  }

  test("test array_distinct") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_distinct(split(n_comment, ' ')) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_distinct(array(1,2,1,2,3)), array_distinct(array(null,1,null,1,2,null,3)), " +
          "array_distinct(array(array(1,null,2), array(1,null,2))), array_distinct(null), array_distinct(array(null))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test array_union") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select array_union(split(n_comment, ' '), reverse(split(n_comment, ' '))) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select array_union(array(1,2,1,2,3), array(2,4,2,3,5)), " +
          "array_union(array(null,1,null,1,2,null,3), array(1,null,2,null,3,null,4)), " +
          "array_union(array(array(1,null,2), array(2,null,3)), array(array(2,null,3), array(1,null,2))), " +
          "array_union(array(null), array(null)), " +
          "array_union(cast(null as array<int>), cast(null as array<int>))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test 'function regexp_extract_all'") {
    runQueryAndCompare(
      "select l_orderkey, regexp_extract_all(l_comment, '([a-z])', 1) " +
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

  test("test literals") {
    val query = """
      SELECT
        CAST(NULL AS BOOLEAN) AS boolean_literal,
        CAST(1 AS TINYINT) AS tinyint_literal,
        CAST(2 AS SMALLINT) AS smallint_literal,
        CAST(3 AS INTEGER) AS integer_literal,
        CAST(4 AS BIGINT) AS bigint_literal,
        CAST(5.5 AS FLOAT) AS float_literal,
        CAST(6.6 AS DOUBLE) AS double_literal,
        CAST('7' AS STRING) AS string_literal,
        DATE '2022-01-01' AS date_literal,
        TIMESTAMP '2022-01-01 10:00:00' AS timestamp_literal,
        CAST(X'48656C6C6F' AS BINARY) AS binary_literal,
        ARRAY(1, 2, 3, 4) AS array_literal,
        MAP("a", 1, "b", 2) AS map_literal,
        STRUCT("hello", 123) AS struct_literal,
        ARRAY() as empty_array_literal,
        MAP() as empty_map_literal,
        ARRAY(1, NULL, 3) as array_with_null_literal,
        MAP(1, 2, CAST(3 as SHORT), null) as map_with_null_literal
      from range(10)"""
    runQueryAndCompare(query)(checkOperatorMatch[ProjectExecTransformer])
  }

  // see issue https://github.com/Kyligence/ClickHouse/issues/93
  ignore("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }

  test("window row_number") {
    val sql =
      """
        |select row_number() over (partition by n_regionkey order by n_nationkey) as num from nation
        |order by n_regionkey, n_nationkey, num
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
        |order by n_regionkey, n_nationkey, x
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
        |order by n_regionkey, n_nationkey, n_lead
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead / lag with negative offset") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, -3, 2) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead1,
        | lead(n_nationkey, 1) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead2,
        | lag(n_nationkey, -1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag1,
        | lag(n_nationkey, 2) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lag2
        |from nation
        |order by n_regionkey, n_nationkey, n_lead1, n_lead2, n_lag1, n_lag2
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lead with default value") {
    val sql =
      """
        |select n_regionkey, n_nationkey,
        | lead(n_nationkey, 1, 3) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) as n_lead
        |from nation
        |order by n_regionkey, n_nationkey, n_lead
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
        |order by n_regionkey, n_nationkey, n_lag
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window lag with null value") {
    val sql =
      """
        |select n_regionkey,
        | lag(count(distinct n_nationkey), -1) OVER (ORDER BY n_regionkey) as n_lag
        |from nation
        |group by n_regionkey
        |order by n_regionkey, n_lag
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

  test("window first value with nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   first_value(n_nationkey) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window first value ignore nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   first_value(n_nationkey, true) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window last value with nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   last_value(n_nationkey) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window last value ignore nulls") {
    val sql =
      """
        | select n_regionkey, n_nationkey,
        |   last_value(n_nationkey, true) over (partition by n_regionkey order by n_nationkey)
        | from
        |   (
        |     select n_regionkey, if(n_nationkey = 1, null, n_nationkey) as n_nationkey from  nation
        |   ) as t
        | order by n_regionkey, n_nationkey
      """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("window bug #2586") {
    val sql =
      """
        | select row_number() over (partition by n_regionkey, id  order by n_nationkey) as num from (
        |   select n_regionkey, 'x' as id , n_nationkey from nation
        | ) order by n_regionkey, id, n_nationkey, num
      """.stripMargin
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

  test("expand with nullable type not match") {
    val sql =
      """
        |select a, n_regionkey, n_nationkey from
        |(select nvl(n_name, "aaaa") as a, n_regionkey, n_nationkey from nation)
        |group by n_regionkey, n_nationkey
        |grouping sets((a, n_regionkey, n_nationkey),(a, n_regionkey), (a))
        |order by a, n_regionkey, n_nationkey
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ExpandExecTransformer])
  }

  test("expand col result") {
    val sql =
      """
        |select n_regionkey, n_nationkey, count(1) as cnt from nation
        |group by n_regionkey, n_nationkey with rollup
        |order by n_regionkey, n_nationkey, cnt
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ExpandExecTransformer])
  }

  test("expand with not nullable") {
    val sql =
      """
        |select a,b, sum(c) from
        |(select nvl(n_nationkey, 0) as c, nvl(n_name, '') as b, nvl(n_nationkey, 0) as a from nation)
        |group by a,b with rollup
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ExpandExecTransformer])
  }

  test("expand with function expr") {
    val sql =
      """
        |select
        | n_name,
        | count(distinct n_regionkey) as col1,
        | count(distinct concat(n_regionkey, n_nationkey)) as col2
        |from nation
        |group by n_name
        |order by n_name, col1, col2
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ExpandExecTransformer])
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

  test("test stddev") {
    val sql =
      """
        |select stddev(l_orderkey), stddev(l_quantity) from lineitem
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

  test("GLUTEN-2491: sequence with null value as argument") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select sequence(null, 1), sequence(1, null), sequence(1, 3, null), sequence(1, 5)," +
          "sequence(5, 1), sequence(1, 5, 2), sequence(5, 1, -2)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select sequence(n_nationkey, n_nationkey+10), sequence(n_nationkey, n_nationkey+10, 2) " +
          "from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("Bug-398 collect_list failure") {
    val sql =
      """
        |select n_regionkey, collect_list(if(n_regionkey=0, n_name, null)) as t from nation group by n_regionkey
        |order by n_regionkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, df => {})
  }

  test("collect_set") {
    val sql =
      """
        |select a, b from (
        |select n_regionkey as a, collect_set(if(n_regionkey=0, n_name, null)) as set from nation group by n_regionkey)
        |lateral view explode(set) as b
        |order by a, b
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[CHHashAggregateExecTransformer])
  }

  test("collect_set should return empty set") {
    runQueryAndCompare(
      "select collect_set(if(n_regionkey != -1, null, n_regionkey)) from nation"
    )(checkOperatorMatch[CHHashAggregateExecTransformer])
  }

  test("Test 'spark.gluten.enabled' false") {
    withSQLConf(("spark.gluten.enabled", "false")) {
      runTPCHQuery(2, noFallBack = false) {
        df =>
          val glutenPlans = df.queryExecution.executedPlan.collect {
            case glutenPlan: GlutenPlan => glutenPlan
          }
          assert(glutenPlans.isEmpty)
      }
    }
  }

  test("test 'cast null value'") {
    val sql = "select cast(x as double), cast(x as float), cast(x as string), cast(x as binary)," +
      "cast(x as long), cast(x as int), cast(x as short), cast(x as byte), cast(x as boolean)," +
      "cast(x as date), cast(x as timestamp), cast(x as decimal(10, 2)) from " +
      "(select cast(null as string) as x from range(10) union all " +
      "select cast(id as string) as x from range(2))"
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'max(NULL)/min(NULL) from table'") {
    val sql =
      """
        |select
        | l_linenumber, max(NULL), min(NULL)
        | from lineitem where l_linenumber = 3 and l_orderkey < 3
        | group by l_linenumber limit 1
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test 'dayofweek/weekday'") {
    val sql = "select l_orderkey, l_shipdate, weekday(l_shipdate), dayofweek(l_shipdate) " +
      "from lineitem limit 10"
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'to_date/to_timestamp'") {
    val sql = "select to_date(concat('2022-01-0', cast(id+1 as String)), 'yyyy-MM-dd')," +
      "to_timestamp(concat('2022-01-01 10:30:0', cast(id+1 as String)), 'yyyy-MM-dd HH:mm:ss') " +
      "from range(9)"
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'btrim/ltrim/rtrim/trim'") {
    runQueryAndCompare(
      "select l_comment, btrim(l_comment), btrim(l_comment, 'abcd') " +
        "from lineitem limit 10")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, ltrim(l_comment), ltrim('abcd', l_comment) " +
        "from lineitem limit 10")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, rtrim(l_comment), rtrim('abcd', l_comment) " +
        "from lineitem limit 10")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      "select l_comment, trim(l_comment), trim('abcd' from l_comment), " +
        "trim(BOTH 'abcd' from l_comment), trim(LEADING 'abcd' from l_comment), " +
        "trim(TRAILING 'abcd' from l_comment) from lineitem limit 10"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("bit_and/bit_or/bit_xor") {
    runQueryAndCompare(
      "select bit_and(n_regionkey), bit_or(n_regionkey), bit_xor(n_regionkey) from nation") {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select bit_and(l_partkey), bit_or(l_suppkey), bit_xor(l_orderkey) from lineitem") {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
  }

  test("test 'EqualNullSafe'") {
    runQueryAndCompare("select l_linenumber <=> l_orderkey, l_linenumber <=> null from lineitem") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test posexplode issue: https://github.com/oap-project/gluten/issues/1767") {
    spark.sql(
      """
        | create table test_tbl(id bigint, data map<string, string>) using parquet;
        |""".stripMargin
    )

    spark.sql("INSERT INTO test_tbl values(1, map('k', 'v'))")
    val sql = """
                | select id from test_tbl lateral view
                | posexplode(split(data['k'], ',')) tx as a, b""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test posexplode issue: https://github.com/oap-project/gluten/issues/2492") {
    val sql = "select posexplode(split(n_comment, ' ')) from nation where n_comment is null"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("test posexplode issue: https://github.com/oap-project/gluten/issues/2454") {
    val sqls = Seq(
      "select explode(array(id, id+1)) from range(10)",
      "select explode(map(id, id+1, id+2, id+3)) from range(10)",
      "select posexplode(array(id, id+1)) from range(10)",
      "select posexplode(map(id, id+1, id+2, id+3)) from range(10)"
    )

    for (sql <- sqls) {
      runQueryAndCompare(sql)(checkOperatorMatch[GenerateExecTransformer])
    }
  }

  test("test 'scala udf'") {
    spark.udf.register("my_add", (x: Long, y: Long) => x + y)
    runQueryAndCompare("select my_add(id, id+1) from range(10)")(
      checkOperatorMatch[ProjectExecTransformer])
  }

  ignore("test 'hive udf'") {
    val jarPath = "backends-clickhouse/src/test/resources/udfs/hive-test-udfs.jar"
    val jarUrl = s"file://${System.getProperty("user.dir")}/$jarPath"
    spark.sql(
      s"CREATE FUNCTION my_add as " +
        "'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2' USING JAR '$jarUrl'")
    runQueryAndCompare("select my_add(id, id+1) from range(10)")(
      checkOperatorMatch[ProjectExecTransformer])
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String = tpchQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    val confName = "spark.gluten.sql.columnar.backend.ch." +
      "runtime_settings.query_plan_enable_optimizations"
    withSQLConf((confName, "true")) {
      compareTPCHQueryAgainstVanillaSpark(queryNum, tpchQueries, customCheck, noFallBack)
    }
    withSQLConf((confName, "false")) {
      compareTPCHQueryAgainstVanillaSpark(queryNum, tpchQueries, customCheck, noFallBack)
    }
  }

  test("test 'ColumnarToRowExec should not be used'") {
    withSQLConf(
      "spark.gluten.sql.columnar.filescan" -> "false",
      "spark.gluten.sql.columnar.filter" -> "false"
    ) {
      runQueryAndCompare(
        "select l_shipdate from lineitem where l_shipdate = '1996-05-07'",
        noFallBack = false) {
        df => getExecutedPlan(df).count(plan => plan.isInstanceOf[ColumnarToRowExec]) == 0
      }
    }
  }

  test("GLUTEN-2104: test size function") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName) {
      runQueryAndCompare(
        "select size(null), size(split(l_shipinstruct, ' ')) from lineitem"
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("GLUTEN-1822: test reverse/concat") {
    val sql =
      """
        |select reverse(split(n_comment, ' ')), reverse(n_comment),
        |concat(split(n_comment, ' ')), concat(n_comment), concat(n_comment, n_name),
        |concat(split(n_comment, ' '), split(n_name, ' '))
        |from nation
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("GLUTEN-1620: fix 'attribute binding failed.' when executing hash agg without aqe") {
    val sql =
      """
        |SELECT *
        |    FROM (
        |      SELECT t1.O_ORDERSTATUS, t4.ACTIVECUSTOMERS / t1.ACTIVECUSTOMERS AS REPEATPURCHASERATE
        |      FROM (
        |         SELECT o_orderstatus AS O_ORDERSTATUS, COUNT(1) AS ACTIVECUSTOMERS
        |         FROM orders
        |         GROUP BY o_orderstatus
        |      ) t1
        |         INNER JOIN (
        |            SELECT o_orderstatus AS O_ORDERSTATUS, MAX(o_totalprice) AS ACTIVECUSTOMERS
        |                FROM orders
        |                GROUP BY o_orderstatus
        |         ) t4
        |         ON t1.O_ORDERSTATUS = t4.O_ORDERSTATUS
        |    ) t5
        |      INNER JOIN (
        |         SELECT t8.O_ORDERSTATUS, t9.ACTIVECUSTOMERS / t8.ACTIVECUSTOMERS AS REPEATPURCHASERATE
        |            FROM (
        |                SELECT o_orderstatus AS O_ORDERSTATUS, COUNT(1) AS ACTIVECUSTOMERS
        |                FROM orders
        |                GROUP BY o_orderstatus
        |            ) t8
        |                INNER JOIN (
        |                    SELECT o_orderstatus AS O_ORDERSTATUS, MAX(o_totalprice) AS ACTIVECUSTOMERS
        |                    FROM orders
        |                    GROUP BY o_orderstatus
        |                ) t9
        |                ON t8.O_ORDERSTATUS = t9.O_ORDERSTATUS
        |            ) t12
        |      ON t5.O_ORDERSTATUS = t12.O_ORDERSTATUS
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { df => })
  }

  test("GLUTEN-1848: Fix execute subquery repeatedly issue with ReusedSubquery") {
    val sql =
      """
        |SELECT
        |    s_suppkey,
        |    s_name,
        |    s_address,
        |    s_phone,
        |    total_revenue,
        |    total_revenue_1
        |FROM
        |    supplier,
        |    (
        |        SELECT
        |            l_suppkey AS supplier_no,
        |            sum(l_extendedprice * (1 - l_discount)) AS total_revenue,
        |            sum(l_extendedprice * l_discount) AS total_revenue_1
        |        FROM
        |            lineitem
        |        WHERE
        |            l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |        GROUP BY
        |            supplier_no) revenue0
        |WHERE
        |    s_suppkey = supplier_no
        |    AND total_revenue = (
        |        SELECT
        |            max(total_revenue)
        |        FROM (
        |            SELECT
        |                l_suppkey AS supplier_no,
        |                sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |            FROM
        |                lineitem
        |            WHERE
        |                l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |            GROUP BY
        |                supplier_no) revenue1)
        |    AND total_revenue_1 < (
        |        SELECT
        |            max(total_revenue)
        |        FROM (
        |            SELECT
        |                l_suppkey AS supplier_no,
        |                sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |            FROM
        |                lineitem
        |            WHERE
        |                l_shipdate >= date'1996-01-01' AND l_shipdate < date'1996-01-01' + interval 3 month
        |            GROUP BY
        |                supplier_no) revenue1)
        |ORDER BY
        |    s_suppkey;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val subqueriesId = df.queryExecution.executedPlan.collectWithSubqueries {
            case s: SubqueryExec => s.id
            case rs: ReusedSubqueryExec => rs.child.id
          }
          assert(subqueriesId.distinct.size == 1)
      }
    )
  }

  test("GLUTEN-1875: UnionExecTransformer for BroadcastRelation") {
    val sql =
      """
        |select /*+ BROADCAST(t2) */ t1.l_orderkey, t1.l_partkey, t2.o_custkey
        |from lineitem t1
        |join (
        |  select o_orderkey, o_custkey from orders
        |  union all
        |  select  o_orderkey, o_custkey from orders) t2
        |on t1.l_orderkey = cast(t2.o_orderkey as int)
        |order by t1.l_orderkey, t1.l_partkey, t2.o_custkey
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2198: Fix wrong schema when there is no aggregate function") {
    val sql =
      """
        |select b, a
        |from
        |  (
        |    select l_shipdate as a, l_returnflag as b from lineitem
        |    union
        |    select o_orderdate as a, o_orderstatus as b from orders
        |  )
        |group by b, a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    val sql1 =
      """
        |select b, a, sum(c), avg(c)
        |from
        |  (
        |    select l_shipdate as a, l_returnflag as b, l_quantity as c from lineitem
        |    union
        |    select o_orderdate as a, o_orderstatus as b, o_totalprice as c from orders
        |  )
        |group by b, a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        |select t1.o_orderkey, o_shippriority, sss from (
        |(
        |  select o_orderkey,o_shippriority from orders
        |) t1
        |left join
        |(
        | select c_custkey custkey, c_nationkey, sum(c_acctbal) as sss
        | from customer
        | group by 1,2
        |) t2
        |on t1.o_orderkey=t2.custkey and t1.o_shippriority=t2.c_nationkey
        |)
        |order by t1.o_orderkey desc, o_shippriority
        |limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 =
      """
        |select t1.o_orderkey, o_shippriority from (
        |(
        |  select o_orderkey,o_shippriority from orders
        |) t1
        |left join
        |(
        | select c_custkey custkey, c_nationkey
        | from customer
        | group by 1,2
        |) t2
        |on t1.o_orderkey=t2.custkey and t1.o_shippriority=t2.c_nationkey
        |)
        |order by t1.o_orderkey desc, o_shippriority
        |limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })
  }

  test("GLUTEN-2079: aggregate function with filter") {
    val sql =
      """
        | select
        |  count(distinct(a)), count(distinct(b)), count(distinct(c))
        | from
        |  values (1, null,2), (2,2,4), (3,2,4) as data(a,b,c)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-1956: fix error conversion of Float32 in CHColumnToSparkRow") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName) {
      runQueryAndCompare(
        "select struct(1.0f), array(2.0f), map('a', 3.0f) from range(1)"
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("GLUTEN-1790 count multi cols") {
    val sql1 =
      """
        | select count(n_regionkey, n_nationkey) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        | select count(a, b) from values(1,null),(2, 2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })

    val sql3 =
      """
        | select count(a, b) from values(null,1),(2, 2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => })

    val sql4 =
      """
        | select count(n_regionkey, n_name) from nation
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => })
  }

  test("GLUTEN-2028: struct as join key") {
    val tables = Seq("struct_1", "struct_2")
    tables.foreach {
      table =>
        spark.sql(s"create table $table (info struct<a:int, b:int>) using parquet")
        spark.sql(s"insert overwrite $table values (named_struct('a', 1, 'b', 2))")
    }
    val hints = Seq("BROADCAST(t2)", "SHUFFLE_MERGE(t2), SHUFFLE_HASH(t2)")
    hints.foreach(
      hint =>
        compareResultsAgainstVanillaSpark(
          s"select /*+ $hint */ t1.info from struct_1 t1 join struct_2 t2 on t1.info = t2.info",
          true,
          { _ => }))
  }

  test("GLUTEN-2005: Json_tuple return cause data loss") {
    spark.sql(
      """
        | create table test_2005(tuple_data struct<a:string, b:string>, id bigint, json_data string, name string) using parquet;
        |""".stripMargin
    )
    spark.sql(
      "insert into test_2005 values(struct('a', 'b'), 1, '{\"a\":\"b\", \"c\":\"d\"}', 'gluten')")
    val sql =
      """
        | select tuple_data, json_tuple(json_data, 'a', 'c'), name from test_2005
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2060 null count") {
    val sql =
      """
        |select
        | count(a),count(b), count(1), count(distinct(a)), count(distinct(b))
        |from
        | values (1, null), (2,2) as data(a,b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2221 empty hash aggregate exec") {
    val sql1 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => }, false)

    val sql2 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 left join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => }, false)

    val sql3 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, true, { _ => }, false)

    val sql4 =
      """
        | select count(*) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, true, { _ => }, false)

    val sql5 =
      """
        | select count(*) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(6,5),(7,5),(8,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql5, true, { _ => }, false)
  }

  test("GLUTEN-1874 not null in one stream") {
    val sql =
      """
        |select n_regionkey from (
        | select *, row_number() over (partition by n_regionkey order by is_new) as rank from(
        |   select n_regionkey, 0 as is_new from nation where n_regionkey is not null
        |   union all
        |   select n_regionkey, 1 as is_new from (
        |     select n_regionkey,
        |       row_number() over (partition by n_regionkey order by n_nationkey) as rn from nation
        |   ) t0 where rn = 1
        | ) t1
        |) t2 where rank = 1
    """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-1874 not null in both streams") {
    val sql =
      """
        |select n_regionkey from (
        | select *, row_number() over (partition by n_regionkey order by is_new) as rank from(
        |   select n_regionkey, 0 as is_new from nation where n_regionkey is not null
        |   union all
        |   select n_regionkey, 1 as is_new from (
        |     select n_regionkey,
        |       row_number() over (partition by n_regionkey order by n_nationkey) as rn
        |     from nation where n_regionkey is not null
        |   ) t0 where rn = 1
        | ) t1
        |) t2 where rank = 1
    """.stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-2095: test cast(string as binary)") {
    runQueryAndCompare(
      "select cast(n_nationkey as binary), cast(n_comment as binary) from nation"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("var_samp") {
    runQueryAndCompare("""
                         |select var_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey % 5, var_samp(l_quantity) from lineitem
                         |group by l_orderkey % 5;
                         |""".stripMargin) {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
  }

  test("var_pop") {
    runQueryAndCompare("""
                         |select var_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey % 5, var_pop(l_quantity) from lineitem
                         |group by l_orderkey % 5;
                         |""".stripMargin) {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }
  }

  test("corr") {
    runQueryAndCompare("""
                         |select corr(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[CHHashAggregateExecTransformer]
    }

    runQueryAndCompare(
      "select corr(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[CHHashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test concat_ws") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select concat_ws(null), concat_ws('-'), concat_ws('-', null), concat_ws('-', null, null), " +
          "concat_ws(null, 'a'), concat_ws('-', 'a'), concat_ws('-', 'a', null), " +
          "concat_ws('-', 'a', null, 'b', 'c', null, array(null), array('d', null), array('f', 'g'))",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])

      runQueryAndCompare(
        "select concat_ws('-', n_comment, " +
          "array(if(n_regionkey=0, null, cast(n_regionkey as string)))) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("GLUTEN-2422 range bound with nan/inf") {
    val sql =
      """
        |select a from values (1.0), (2.1), (null), (cast('NaN' as double)), (cast('inf' as double)),
        | (cast('-inf' as double)) as data(a) order by a asc nulls last
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[SortExecTransformer])
  }

  test("GLUTEN-2639: log1p") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select log1p(n_regionkey), log1p(-1.0), log1p(-2.0) from nation"
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("GLUTEN-2243 empty projection") {
    val sql =
      """
        | select count(1) from(
        |   select b,c from values(1,2),(1,2) as data(b,c) group by b,c
        |   union all
        |   select a, b from values (1,2),(1,2),(2,3) as data(a,b) group by a, b
        | )
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("Gluten-2430 hash partition column not found") {
    val sql =
      """
        |
        | select a.l_shipdate,
        |    a.l_partkey,
        |    b.l_shipmode,
        |    if(c.l_suppkey is not null, 'new', 'old') as usertype,
        |    a.uid
        |from (
        |        select l_shipdate,
        |            l_partkey,
        |            l_suppkey as uid
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |        group by l_shipdate,
        |            l_partkey,
        |            l_suppkey
        |    ) a
        |    join (
        |        select l_shipdate,
        |            l_suppkey as uid,
        |            l_shipmode
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |    ) b on a.l_shipdate = b.l_shipdate
        |    and a.uid = b.uid
        |    left join (
        |        select l_shipdate,
        |            l_suppkey
        |        from lineitem
        |        where l_shipdate = '2023-03-07'
        |        group by l_shipdate,
        |            l_suppkey
        |    ) c on a.l_shipdate = c.l_shipdate
        |    and a.uid = c.l_suppkey
        |limit 100;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("cast date issue-2474") {
    spark.sql(
      """
        | create table test_date (pid BIGINT) using parquet;
        |""".stripMargin
    )
    spark.sql(
      """
        | insert into test_date values (6927737632337729200), (6927744564414944949)
        |""".stripMargin
    )

    val sql1 =
      """
        | select
        |   pid,
        |   from_unixtime(bigint(pid / 4294967296),'yyyy-mm-dd') < date_sub('2023-01-01', 1) as c
        | from test_date
        | order by pid
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    val sql2 =
      """
        | select pid
        | from test_date
        | where from_unixtime(bigint(pid / 4294967296),'yyyy-mm-dd') < date_sub('2023-01-01', 1)
        | order by pid
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
  }

  test("test-conv-function") {
    {
      val sql =
        """
          | select conv(a, 2, 10) from(
          |   select a from values('100'),('1010') as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
    {
      val sql =
        """
          | select conv(a, 200, 10) from(
          |   select a from values('100'),('1010') as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
    {
      val sql =
        """
          | select conv(a, 16, 10) from(
          |   select a from values(10),(20) as data(a))
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-3105: test json output format") {
    val sql =
      """
        |select to_json(struct(cast(id as string), id, 1.1, 1.1f, 1.1d)) from range(3)
        |""".stripMargin
    // cast('nan' as double) output 'NaN' in Spark, 'nan' in CH
    // cast('inf' as double) output 'Infinity' in Spark, 'inf' in CH
    // ignore them temporarily
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("Test plan json non-empty") {
    spark.sparkContext.setLogLevel("WARN")
    val df1 = spark
      .sql("""
             | select * from lineitem limit 1
             | """.stripMargin)
    val executedPlan1 = df1.queryExecution.executedPlan
    val lastStageTransformer1 = executedPlan1.find(_.isInstanceOf[WholeStageTransformer])
    executedPlan1.execute()
    assert(lastStageTransformer1.get.asInstanceOf[WholeStageTransformer].getPlanJson.isEmpty)

    spark.sparkContext.setLogLevel("DEBUG")
    val df2 = spark
      .sql("""
             | select * from lineitem limit 1
             | """.stripMargin)
    val executedPlan2 = df2.queryExecution.executedPlan
    val lastStageTransformer2 = executedPlan2.find(_.isInstanceOf[WholeStageTransformer])
    executedPlan2.execute()
    assert(lastStageTransformer2.get.asInstanceOf[WholeStageTransformer].getPlanJson.nonEmpty)
    spark.sparkContext.setLogLevel(logLevel)
  }
}
// scalastyle:on line.size.limit
