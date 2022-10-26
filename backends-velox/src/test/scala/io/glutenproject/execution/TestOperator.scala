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
import org.apache.spark.sql.{Row, TestUtils}
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
    val result = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    assert(result.length == 1)
  }

  test("select_part_column") {
    val result = runQueryAndCompare("select l_shipdate, l_orderkey from lineitem limit 1") { df =>
      { assert(df.schema.fields.length == 2) }
    }
    assert(result.length == 1)
  }

  test("select_as") {
    val result = runQueryAndCompare("select l_shipdate as my_col from lineitem limit 1") { df =>
      { assert(df.schema.fieldNames(0).equals("my_col")) }
    }
    assert(result.length == 1)
  }

  test("test_where") {
    val result = runQueryAndCompare(
      "select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    assert(result.length == 59288)
  }

  test("test_is_null") {
    val result = runQueryAndCompare("select l_orderkey from lineitem " +
      "where l_comment is null") { _ => }
    assert(result.isEmpty)
  }

  test("test_is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val result = runQueryAndCompare("select * from temp_test_is_null where col1 is null") { _ => }
    assert(result.length == 2)
  }

  test("test_is_not_null") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    assert(result.length == 6)
  }

  // test result failed, wait to fix, filter on 2 column will cause problem
  ignore("test_and pushdown") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey = 1") { _ => }
    assert(result.isEmpty)
  }

  test("test_in") {
    val result = runQueryAndCompare("select l_orderkey from lineitem " +
      "where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    assert(result.length == 122)
  }

  // wait to fix, may same as before todo
  ignore("test_in_and") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    assert(result.length == 73)
  }

  test("test_in_or") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    assert(result.length == 122)
  }

  test("test_in_or_and") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    assert(result.length == 73)
  }

  test("test_in_not") {
    val result = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    assert(result.length == 60141)
  }

  test("coalesce") {
    var result = runQueryAndCompare("select l_orderkey, coalesce(l_comment, 'default_val') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runQueryAndCompare("select l_orderkey, coalesce(null, l_comment, 'default_val') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runQueryAndCompare("select l_orderkey, coalesce(null, null, l_comment) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runQueryAndCompare("select l_orderkey, coalesce(null, null, 1, 2) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runQueryAndCompare("select l_orderkey, coalesce(null, null, null) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("test_count") {
    val result = runQueryAndCompare("select count(*) from lineitem " +
      "where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    val expected = Seq(Row(122))
    assert(result.length == 1)
    TestUtils.compareAnswers(result, expected)
  }

  test("test_avg") {
    val result = runQueryAndCompare("select avg(l_partkey) from lineitem " +
      "where l_partkey < 1000") { _ => }
    assert(result.length == 1)
  }

  test("test_sum") {
    val result = runQueryAndCompare("select sum(l_partkey) from lineitem " +
      "where l_partkey < 2000") { _ => }
    assert(result.length == 1)
  }

  test("test_groupby") {
    val result = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    assert(result.length == 2)
    val expected = Seq(Row(1.0, 3283.0), Row(2.0, 1062.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("test_orderby") {
    val result = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    assert(result.length == 7)

    val expected =
      Seq(Row(48.0), Row(10.0), Row(23.0), Row(38.0), Row(75.0), Row(33.0), Row(93.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("test_orderby expression") {
    val result = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    assert(result.length == 7)
    val expected =
      Seq(Row(48.0), Row(10.0), Row(23.0), Row(38.0), Row(75.0), Row(33.0), Row(93.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("Test chr function") {
    val df = spark.sql("SELECT chr(l_orderkey + 64) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row("A"))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test abs function") {
    val df = spark.sql("SELECT abs(l_orderkey) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(1))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test ceil function") {
    val df = spark.sql("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(1))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test floor function") {
    val df = spark.sql("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(1))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test Exp function") {
    val df = spark.sql("SELECT exp(l_orderkey) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(2.718281828459045))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test Power function") {
    val df = spark.sql("SELECT power(l_orderkey, 2.0) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(1))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test Pmod function") {
    val df = spark.sql("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1")
    val result = df.collect()
    assert(result.length == 1)
    val expected = Seq(Row(1))
    TestUtils.compareAnswers(result, expected)
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  // VeloxRuntimeError, wait to fix
  ignore("Test isnull function") {
    val df = spark.sql("SELECT isnull(1)")
    df.show()
    df.explain(false)
  }

  // VeloxRuntimeError, wait to fix
  ignore("Test df.count()") {
    val df = spark.sql("select * from lineitem limit 1")
    df.count()
    df.explain(false)
  }
}
