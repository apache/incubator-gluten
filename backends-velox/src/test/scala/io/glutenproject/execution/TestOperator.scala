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

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{Decimal, DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import scala.collection.JavaConverters

import org.apache.spark.sql.functions.{avg, col}

class TestOperator extends WholeStageTransformerSuite {

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
    val df = runQueryAndCompare("select l_shipdate, l_orderkey from lineitem limit 1") { df =>
      { assert(df.schema.fields.length == 2) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("select_as") {
    val df = runQueryAndCompare("select l_shipdate as my_col from lineitem limit 1") { df =>
      { assert(df.schema.fieldNames(0).equals("my_col")) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("where") {
    val df = runQueryAndCompare(
      "select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    checkLengthAndPlan(df, 59288)
  }

  test("is_null") {
    val df = runQueryAndCompare("select l_orderkey from lineitem " +
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
    val df = runQueryAndCompare("select l_orderkey from lineitem " +
      "where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
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
    var df = runQueryAndCompare("select l_orderkey, coalesce(l_comment, 'default_val') " +
      "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare("select l_orderkey, coalesce(null, l_comment, 'default_val') " +
      "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare("select l_orderkey, coalesce(null, null, l_comment) " +
      "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare("select l_orderkey, coalesce(null, null, 1, 2) " +
      "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare("select l_orderkey, coalesce(null, null, null) " +
      "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
  }

  test("count") {
    val df = runQueryAndCompare(
      "select count(*) from lineitem where l_partkey in (1552, 674, 1062)") {
      checkOperatorMatch[GlutenHashAggregateExecTransformer] }
    runQueryAndCompare(
      "select count(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("avg") {
    val df = runQueryAndCompare(
      "select avg(l_partkey) from lineitem where l_partkey < 1000") {
      checkOperatorMatch[GlutenHashAggregateExecTransformer] }
    runQueryAndCompare(
      "select avg(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("sum") {
    runQueryAndCompare(
      "select sum(l_partkey) from lineitem where l_partkey < 2000") {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select sum(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(22, 2))) from lineitem") {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("min and max") {
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey) from lineitem where l_partkey < 2000") {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
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
    runQueryAndCompare(
      "select row_number() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select rank() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

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
      "select sum(l_partkey + 1) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") { _ => }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select min(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select avg(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

  }

  test("chr function") {
    val df = runQueryAndCompare("SELECT chr(l_orderkey + 64) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("abs function") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("ceil function") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("floor function") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("exp function") {
    val df = spark.sql("SELECT exp(l_orderkey) from lineitem limit 1")
    checkLengthAndPlan(df, 1)
  }

  test("power function") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2.0) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("pmod function") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("round function") {
    val df = runQueryAndCompare("SELECT round(cast(l_orderkey as int), 2)" +
      "from lineitem limit 1") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("greatest function") {
    val df = runQueryAndCompare("SELECT greatest(l_orderkey, l_orderkey)" +
      "from lineitem limit 1" ) { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("least function") {
    val df = runQueryAndCompare("SELECT least(l_orderkey, l_orderkey)" +
      "from lineitem limit 1" ) { checkOperatorMatch[ProjectExecTransformer] }
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
    runQueryAndCompare(
      """
        |select count(orderkey) from (
        | select l_orderkey as orderkey from lineitem
        | union all
        | select o_orderkey as orderkey from orders
        |);
        |""".stripMargin) { df => {
      getExecutedPlan(df).exists(plan =>
        plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
    }}
  }

  test("union_all three tables") {
    runQueryAndCompare(
      """
        |select count(orderkey) from (
        | select l_orderkey as orderkey from lineitem
        | union all
        | select o_orderkey as orderkey from orders
        | union all
        | (select o_orderkey as orderkey from orders limit 100)
        |);
        |""".stripMargin) { df => {
      getExecutedPlan(df).exists(plan =>
        plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
    }}
  }

  test("union two tables") {
      val df = runQueryAndCompare(
        """
          |select count(orderkey) from (
          | select l_orderkey as orderkey from lineitem
          | union
          | select o_orderkey as orderkey from orders
          |);
          |""".stripMargin) { df => {
          getExecutedPlan(df).exists(plan =>
            plan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
        }
      }
  }

  test("test 'select global/local limit'") {
    runQueryAndCompare(
      """
        |select * from (
        | select * from lineitem limit 10
        |) where l_suppkey != 0 limit 100;
        |""".stripMargin) {
      checkOperatorMatch[LimitTransformer]
    }
  }

  test("stddev_samp") {
    runQueryAndCompare(
      """
        |select stddev_samp(l_quantity) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      """
        |select l_orderkey, stddev_samp(l_quantity) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select stddev_samp(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("round") {
    runQueryAndCompare(
      """
        |select round(l_quantity, 2) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("stddev_pop") {
    runQueryAndCompare(
      """
        |select stddev_pop(l_quantity) from lineitem;
      |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      """
        |select l_orderkey, stddev_pop(l_quantity) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select stddev_pop(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("var_samp") {
    runQueryAndCompare(
      """
        |select var_samp(l_quantity) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      """
        |select l_orderkey, var_samp(l_quantity) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select var_samp(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("var_pop") {
    runQueryAndCompare(
      """
        |select var_pop(l_quantity) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      """
        |select l_orderkey, var_pop(l_quantity) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select var_pop(l_quantity), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("bit_and and bit_or") {
    runQueryAndCompare(
      """
        |select bit_and(l_linenumber) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select bit_and(l_linenumber), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      """
        |select bit_or(l_linenumber) from lineitem
        |group by l_orderkey;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select bit_or(l_linenumber), count(distinct l_partkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("bool scan") {
    withTempPath { path =>
      Seq(true, false, true, true, false, false)
        .toDF("a").write.parquet(path.getCanonicalPath)
      spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
      runQueryAndCompare("SELECT a from view") {
        checkOperatorMatch[BatchScanExecTransformer]
      }
    }
  }

  test("decimal abs") {
    runQueryAndCompare(
      """
        |select abs(cast (l_quantity * (-1.0) as decimal(12, 2))),
        |abs(cast (l_quantity * (-1.0) as decimal(22, 2))),
        |abs(cast (l_quantity as decimal(12, 2))),
        |abs(cast (l_quantity as decimal(12, 2))) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
    withTempPath { path =>
      Seq(-3099.270000, -3018.367500, -2833.887500, -1304.180000, -1263.289167, -1480.093333)
        .toDF("a").write.parquet(path.getCanonicalPath)
      spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
      runQueryAndCompare("SELECT abs(cast (a as decimal(19, 6))) from view") {
        checkOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("corr covar_pop covar_samp") {
    runQueryAndCompare(
      """
        |select corr(l_partkey, l_suppkey) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select corr(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      """
        |select covar_pop(l_partkey, l_suppkey) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_pop(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      """
        |select covar_samp(l_partkey, l_suppkey) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[GlutenHashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_samp(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }

  test("Cast double to decimal") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df.select($"DecimalCol" cast DecimalType(38, 33))
      .select(col("DecimalCol")).agg(avg($"DecimalCol"))
    assert(result.collect()(0).get(0).toString.equals("0.0345678900000000000000000000000000000"))
    checkOperatorMatch[GlutenHashAggregateExecTransformer](result)
  }

  test("distinct functions") {
    runQueryAndCompare("SELECT sum(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT sum(DISTINCT l_partkey), count(*), sum(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT avg(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT avg(DISTINCT l_partkey), count(*), avg(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT count(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT count(DISTINCT l_partkey), count(*), count(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT stddev_samp(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT stddev_samp(DISTINCT l_partkey), count(*), " +
        "stddev_samp(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT stddev_pop(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT stddev_pop(DISTINCT l_partkey), count(*), " +
        "stddev_pop(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT var_samp(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT var_samp(DISTINCT l_partkey), count(*), " +
        "var_samp(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT var_pop(DISTINCT l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare(
      "SELECT var_pop(DISTINCT l_partkey), count(*), " +
        "var_pop(l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT corr(DISTINCT l_partkey, l_suppkey)," +
      "corr(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT corr(DISTINCT l_partkey, l_suppkey)," +
      "count(*), corr(l_suppkey, l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
      "covar_pop(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
      "count(*), covar_pop(l_suppkey, l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
      "covar_samp(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
    runQueryAndCompare("SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
      "count(*), covar_samp(l_suppkey, l_partkey) FROM lineitem") { df => {
      assert(getExecutedPlan(df).count(plan => {
        plan.isInstanceOf[GlutenHashAggregateExecTransformer]}) == 4)
    }}
  }
}
