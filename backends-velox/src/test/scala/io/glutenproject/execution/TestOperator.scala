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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters

class TestOperator extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

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
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
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

  test("test_where") {
    val df = runQueryAndCompare(
      "select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    checkLengthAndPlan(df, 59288)
  }

  test("test_is_null") {
    val df = runQueryAndCompare("select l_orderkey from lineitem " +
      "where l_comment is null") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("test_is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val df = runQueryAndCompare("select * from temp_test_is_null where col1 is null") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("test_is_not_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    checkLengthAndPlan(df, 6)
  }

  test("test_and pushdown") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey = 1") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("test_in") {
    val df = runQueryAndCompare("select l_orderkey from lineitem " +
      "where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    checkLengthAndPlan(df, 122)
  }

  test("test_in_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("test_in_or") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("test_in_or_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("test_in_not") {
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

  ignore("test_count") {
    val df = runQueryAndCompare("select count(*) from lineitem " +
      "where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    checkLengthAndPlan(df, 1)
  }

  ignore("test_avg") {
    val df = runQueryAndCompare("select avg(l_partkey) from lineitem " +
      "where l_partkey < 1000") { _ => }
    checkLengthAndPlan(df, 1)
  }

  ignore("test_sum") {
    val df = runQueryAndCompare("select sum(l_partkey) from lineitem " +
      "where l_partkey < 2000") { _ => }
    checkLengthAndPlan(df, 1)
  }

  ignore("test_groupby") {
    val df = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("test_group sets") {
    val result = runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey ") { _ => }
  }

  ignore("test_orderby") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    checkLengthAndPlan(df, 7)
  }

  ignore("test_orderby expression") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("test window expression") {
    runQueryAndCompare(
      "select row_number() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select rank() over" +
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

  test("Test chr function") {
    val df = runQueryAndCompare("SELECT chr(l_orderkey + 64) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test abs function") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test ceil function") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test floor function") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test Exp function") {
    val df = spark.sql("SELECT exp(l_orderkey) from lineitem limit 1")
    checkLengthAndPlan(df, 1)
  }

  test("Test Power function") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2.0) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test Pmod function") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  ignore("Test round function") {
    val df = runQueryAndCompare("SELECT round(cast(l_orderkey as int), 2)" +
      "from lineitem limit 1") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("Test greatest function") {
    val df = runQueryAndCompare("SELECT greatest(l_orderkey, l_orderkey)" +
      "from lineitem limit 1" ) { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("Test least function") {
    val df = runQueryAndCompare("SELECT least(l_orderkey, l_orderkey)" +
      "from lineitem limit 1" ) { checkOperatorMatch[ProjectExecTransformer] }
  }

  // Test "SELECT ..." without a from clause.
  test("Test isnull function") {
    val df = runQueryAndCompare("SELECT isnull(1)") { _ => }
  }

  // VeloxRuntimeError, wait to fix
  ignore("Test df.count()") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("test_union_all two tables") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val df = runQueryAndCompare(
        """
          |select count(orderkey) from (
          | select l_orderkey as orderkey from lineitem
          | union all
          | select o_orderkey as orderkey from orders
          |);
          |""".stripMargin) { _ => }
      assert(df.queryExecution.executedPlan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
    }
  }

  test("test_union_all three tables") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val df = runQueryAndCompare(
        """
          |select count(orderkey) from (
          | select l_orderkey as orderkey from lineitem
          | union all
          | select o_orderkey as orderkey from orders
          | union all
          | (select o_orderkey as orderkey from orders limit 100)
          |);
          |""".stripMargin) { _ => }
      assert(df.queryExecution.executedPlan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
    }
  }

  test("test_union two tables") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val df = runQueryAndCompare(
        """
          |select count(orderkey) from (
          | select l_orderkey as orderkey from lineitem
          | union
          | select o_orderkey as orderkey from orders
          |);
          |""".stripMargin) {
        _ =>
      }
      assert(df.queryExecution.executedPlan.find(_.isInstanceOf[UnionExecTransformer]).isDefined)
    }
  }

  test("test 'select global/local limit'") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      runQueryAndCompare(
        """
          |select * from (
          | select * from lineitem limit 10
          |) where l_suppkey != 0 limit 100;
          |""".stripMargin) {
        checkOperatorMatch[LimitTransformer]
      }
    }
  }

  test("stddev_samp") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
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
    }
  }

  test("round") {
    runQueryAndCompare(
      """
        |select round(l_quantity, 2) from lineitem;
        |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }
}
