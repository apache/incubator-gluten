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

import io.glutenproject.GlutenConfig
import io.glutenproject.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.{GenerateExec, RDDScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

import scala.collection.JavaConverters

class TestOperator extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
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
  }

  test("simple_select") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("select_part_column") {
    val df = runQueryAndCompare("select l_shipdate, l_orderkey from lineitem limit 1") {
      df => { assert(df.schema.fields.length == 2) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("select_as") {
    val df = runQueryAndCompare("select l_shipdate as my_col from lineitem limit 1") {
      df => { assert(df.schema.fieldNames(0).equals("my_col")) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("where") {
    val df = runQueryAndCompare("select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    checkLengthAndPlan(df, 59288)
  }

  test("is_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_comment is null") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val df = runQueryAndCompare("select * from temp_test_is_null where col1 is null") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("is_not_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    checkLengthAndPlan(df, 6)
  }

  test("and pushdown") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey = 1") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("in") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("in_or") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_or_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("in_not") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 60141)
  }

  test("coalesce") {
    var df = runQueryAndCompare(
      "select l_orderkey, coalesce(l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, l_comment) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, 1, 2) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, null) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
  }

  test("groupby") {
    val df = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("group sets") {
    val result = runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey ") { _ => }
  }

  test("orderby") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("orderby expression") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("window expression") {
    def assertWindowOffloaded: DataFrame => Unit = {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[WindowExecTransformer]
              }) > 0)
        }
    }

    Seq("sort", "streaming").foreach {
      windowType =>
        withSQLConf("spark.gluten.sql.columnar.backend.velox.window.type" -> windowType) {
          runQueryAndCompare(
            "select row_number() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select dense_rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select percent_rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select cume_dist() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) IGNORE NULLS over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select sum(l_partkey + 1) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select max(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select min(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }

          // Test same partition/ordering keys.
          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_suppkey) from lineitem ") {
            assertWindowOffloaded
          }

          // Test overlapping partition/ordering keys.
          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_suppkey, l_orderkey) from lineitem ") {
            assertWindowOffloaded
          }
        }
    }
  }

  test("chr function") {
    val df = runQueryAndCompare(
      "SELECT chr(l_orderkey + 64) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("bin function") {
    val df = runQueryAndCompare(
      "SELECT bin(l_orderkey) " +
        "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("abs function") {
    val df = runQueryAndCompare(
      "SELECT abs(l_orderkey) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("ceil function") {
    val df = runQueryAndCompare(
      "SELECT ceil(cast(l_orderkey as long)) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("floor function") {
    val df = runQueryAndCompare(
      "SELECT floor(cast(l_orderkey as long)) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("exp function") {
    val df = spark.sql("SELECT exp(l_orderkey) from lineitem limit 1")
    checkLengthAndPlan(df, 1)
  }

  test("power function") {
    val df = runQueryAndCompare(
      "SELECT power(l_orderkey, 2.0) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("pmod function") {
    val df = runQueryAndCompare(
      "SELECT pmod(cast(l_orderkey as int), 3) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("round function") {
    val df = runQueryAndCompare(
      "SELECT round(cast(l_orderkey as int), 2)" +
        "from lineitem limit 1")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("greatest function") {
    val df = runQueryAndCompare(
      "SELECT greatest(l_orderkey, l_orderkey)" +
        "from lineitem limit 1")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("least function") {
    val df = runQueryAndCompare(
      "SELECT least(l_orderkey, l_orderkey)" +
        "from lineitem limit 1")(checkOperatorMatch[ProjectExecTransformer])
  }

  // Test "SELECT ..." without a from clause.
  test("isnull function") {
    runQueryAndCompare("SELECT isnull(1)") { _ => }
  }

  test("df.count()") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("union_all two tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(
            plan => plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
        }
    }
  }

  test("union_all three tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         | union all
                         | (select o_orderkey as orderkey from orders limit 100)
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(
            plan => plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
        }
    }
  }

  test("union two tables") {
    val df = runQueryAndCompare("""
                                  |select count(orderkey) from (
                                  | select l_orderkey as orderkey from lineitem
                                  | union
                                  | select o_orderkey as orderkey from orders
                                  |);
                                  |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(
            plan => plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
        }
    }
  }

  test("test 'select global/local limit'") {
    runQueryAndCompare("""
                         |select * from (
                         | select * from lineitem limit 10
                         |) where l_suppkey != 0 limit 100;
                         |""".stripMargin) {
      checkOperatorMatch[LimitTransformer]
    }
  }

  test("round") {
    runQueryAndCompare("""
                         |select round(l_quantity, 2) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bool scan") {
    withTempPath {
      path =>
        Seq(true, false, true, true, false, false)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT a from view") {
          checkOperatorMatch[BatchScanExecTransformer]
        }
    }
  }

  test("decimal abs") {
    runQueryAndCompare("""
                         |select abs(cast (l_quantity * (-1.0) as decimal(12, 2))),
                         |abs(cast (l_quantity * (-1.0) as decimal(22, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        Seq(-3099.270000, -3018.367500, -2833.887500, -1304.180000, -1263.289167, -1480.093333)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT abs(cast (a as decimal(19, 6))) from view") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("nested decimal arithmetics") {
    runQueryAndCompare("""
                         |SELECT
                         |  l_orderkey,
                         |  SUM(
                         |    (l_extendedprice * (1 - l_discount)) +
                         |    (l_extendedprice * (1 - l_discount) * 0.05)
                         |  ) AS total_revenue_with_tax
                         |FROM
                         |  lineitem
                         |GROUP BY
                         |  l_orderkey
                         |ORDER BY
                         |  l_orderkey
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
  }

  test("Cast double to decimal") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df
      .select($"DecimalCol".cast(DecimalType(38, 33)))
      .select(col("DecimalCol"))
      .agg(avg($"DecimalCol"))
    // Double precision loss:
    // https://github.com/facebookincubator/velox/pull/6051#issuecomment-1731028215.
    // assert(result.collect()(0).get(0).toString.equals("0.0345678900000000000000000000000000000"))
    assert((result.collect()(0).get(0).toString.toDouble - d).abs < 0.00000000001)
    checkOperatorMatch[HashAggregateExecTransformer](result)
  }

  test("orc scan") {
    val df = spark.read
      .format("orc")
      .load("../cpp/velox/benchmarks/data/bm_lineitem/orc/lineitem.orc")
    df.createOrReplaceTempView("lineitem_orc")
    runQueryAndCompare("select l_orderkey from lineitem_orc") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[BatchScanExecTransformer]
              }) == 1)
        }
    }
  }

  test("test OneRowRelation") {
    val df = sql("SELECT 1")
    checkAnswer(df, Row(1))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[RDDScanExec]).isDefined)
    assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
    assert(plan.find(_.isInstanceOf[RowToVeloxColumnarExec]).isDefined)
  }

  test("equal null safe") {
    runQueryAndCompare("""
                         |select l_quantity <=> 1000 from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test overlay function") {
    runQueryAndCompare("""
                         |select overlay(l_shipdate placing '_' from 0) from lineitem limit 1;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Improve the local sort ensure requirements") {
    withSQLConf(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false") {
      withTable("t1", "t2") {
        sql("""
              |create table t1 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100)
              |""".stripMargin)
        sql("""
              |create table t2 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
              |""".stripMargin)

        runQueryAndCompare(
          """
            |select * from (select c1, max(c2) from t1 group by c1)t1
            |join t2 on t1.c1 = t2.c1 and t1.c1 > conv(t2.c1, 2, 10);
            |""".stripMargin
        ) {
          checkOperatorMatch[HashAggregateExecTransformer]
        }
      }
    }
  }

  test("Fix Generate fail when required child output is not same with child output") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      runQueryAndCompare("SELECT c1, explode(array(c2)) FROM t") {
        checkOperatorMatch[GenerateExecTransformer]
      }

      runQueryAndCompare("SELECT c1, explode(c3) FROM (SELECT c1, array(c2) as c3 FROM t)") {
        checkOperatorMatch[GenerateExecTransformer]
      }
    }
  }

  test("Validation should fail if unsupported expression is used for Generate.") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      // Add a simple UDF to generate the unsupported case
      val intToArrayFunc = udf((s: Int) => Array(s))
      spark.udf.register("intToArray", intToArrayFunc)

      // Testing unsupported case
      runQueryAndCompare("SELECT explode(intToArray(c1)) from t;") {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }

      // Testing unsupported case in case when
      runQueryAndCompare("""
                           |SELECT explode(case when size(intToArray(c1)) > 0
                           |then array(c1) else array(c2) end) from t;
                           |""".stripMargin) {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }
    }
  }

  test("Support get native plan tree string, Velox single aggregation") {
    runQueryAndCompare("select l_partkey + 1, count(*) from lineitem group by l_partkey + 1") {
      df =>
        val wholeStageTransformers = collect(df.queryExecution.executedPlan) {
          case w: WholeStageTransformer => w
        }
        val nativePlanString = wholeStageTransformers.head.nativePlanString()
        assert(nativePlanString.contains("Aggregation[SINGLE"))
        assert(nativePlanString.contains("TableScan"))
    }
  }

  test("Support StreamingAggregate if child output ordering is satisfied") {
    withTable("t") {
      spark
        .range(10000)
        .selectExpr(s"id % 999 as c1", "id as c2")
        .write
        .saveAsTable("t")

      withSQLConf(
        GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "true",
        GlutenConfig.COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
      ) {
        val query =
          """
            |SELECT c1, count(*), sum(c2) FROM (
            |SELECT t1.c1, t2.c2 FROM t t1 JOIN t t2 ON t1.c1 = t2.c1
            |)
            |GROUP BY c1
            |""".stripMargin
        runQueryAndCompare(query) {
          df =>
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[SortMergeJoinExecTransformer]).isDefined)
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
        }
      }
    }
  }

  test("Verify parquet field name with special character") {
    withTable("t") {

      // https://github.com/apache/spark/pull/35229 Spark remove parquet field name check after 3.2
      if (!SparkShimLoader.getSparkVersion.startsWith("3.2")) {
        sql("create table t using parquet as select sum(l_partkey) from lineitem")
        runQueryAndCompare("select * from t") {
          checkOperatorMatch[FileSourceScanExecTransformer]
        }
      } else {
        val msg = intercept[AnalysisException] {
          sql("create table t using parquet as select sum(l_partkey) from lineitem")
        }.message
        assert(msg.contains("contains invalid character"))
      }
    }
  }

  test("test explode function") {
    runQueryAndCompare("""
                         |SELECT explode(array(1, 2, 3));
                         |""".stripMargin) {
      checkOperatorMatch[GenerateExecTransformer]
    }
    runQueryAndCompare("""
                         |SELECT explode(map(1, 'a', 2, 'b'));
                         |""".stripMargin) {
      checkOperatorMatch[GenerateExecTransformer]
    }
    runQueryAndCompare(
      """
        |SELECT explode(array(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd'), map(5, 'e', 6, 'f')));
        |""".stripMargin) {
      checkOperatorMatch[GenerateExecTransformer]
    }
    runQueryAndCompare("""
                         |SELECT explode(map(1, array(1, 2), 2, array(3, 4)));
                         |""".stripMargin) {
      checkOperatorMatch[GenerateExecTransformer]
    }
  }

  test("Support bool type filter in scan") {
    withTable("t") {
      sql("create table t (id int, b boolean) using parquet")
      sql("insert into t values (1, true), (2, false), (3, null)")
      runQueryAndCompare("select * from t where b = true") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b = false") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b is NULL") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
    }
  }

  test("test cross join with equi join conditions") {
    withTable("t1", "t2") {
      sql("""
            |create table t1 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100)
            |""".stripMargin)
      sql("""
            |create table t2 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
            |""".stripMargin)

      runQueryAndCompare(
        """
          |select * from t1 cross join t2 on t1.c1 = t2.c1;
          |""".stripMargin
      ) {
        checkOperatorMatch[ShuffledHashJoinExecTransformer]
      }

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1MB") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkOperatorMatch[GlutenBroadcastHashJoinExecTransformer]
        }
      }

      withSQLConf("spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkOperatorMatch[SortMergeJoinExecTransformer]
        }
      }
    }
  }

  test("knownfloatingpointnormalized") {
    runQueryAndCompare("""
                         |select avg(l_quantity) from lineitem
                         |group by cast(l_orderkey as double);
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
  }
}
