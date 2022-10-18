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
  override protected val resourcePath: String = "/tpch-data-orc-velox"
  override protected val fileFormat: String = "orc"

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
    val result = runSql("select * from lineitem limit 1") { _ => }
    assert(result.length == 1)
  }

  test("select_part_column") {
    val result = runSql("select l_shipdate, l_orderkey from lineitem limit 1") { df =>
      { assert(df.schema.fields.length == 2) }
    }
    assert(result.length == 1)
  }

  test("select_as") {
    val result = runSql("select l_shipdate as my_col from lineitem limit 1") { df =>
      { assert(df.schema.fieldNames(0).equals("my_col")) }
    }
    assert(result.length == 1)
  }

  test("test_where") {
    val result = runSql("select * from lineitem where l_shipdate < 8500") { _ => }
    assert(result.length == 10057)
  }

  test("test_is_null") {
    val result = runSql("select l_orderkey from lineitem where l_comment is null") { _ => }
    assert(result.isEmpty)
  }

  test("test_is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val result = runSql("select * from temp_test_is_null where col1 is null") { _ => }
    assert(result.length == 2)
  }

  test("test_is_not_null") {
    val result = runSql(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    assert(result.length == 6)
  }

  // test result failed, wait to fix, filter on 2 column will cause problem
  ignore("test_and pushdown") {
    val result = runSql(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey = 1") { _ => }
    assert(result.isEmpty)
  }

  test("test_in") {
    val result = runSql("select l_orderkey from lineitem where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    assert(result.length == 122)
  }

  // wait to fix, may same as before todo
  ignore("test_in_and") {
    val result = runSql(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    assert(result.length == 73)
  }

  test("test_in_or") {
    val result = runSql(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    assert(result.length == 122)
  }

  test("test_in_or_and") {
    val result = runSql(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    assert(result.length == 73)
  }

  test("test_in_not") {
    val result = runSql(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    assert(result.length == 60141)
  }

  test("test_count") {
    val result = runSql("select count(*) from lineitem where l_partkey in (1552, 674, 1062)") {
      _ =>
    }
    val expected = Seq(Row(122))
    assert(result.length == 1)
    TestUtils.compareAnswers(result, expected)
  }

  test("test_avg") {
    val result = runSql("select avg(l_partkey) from lineitem where l_partkey < 1000") { _ => }
    assert(result.length == 1)
  }

  test("test_sum") {
    val result = runSql("select sum(l_partkey) from lineitem where l_partkey < 2000") { _ => }
    assert(result.length == 1)
  }

  test("test_groupby") {
    val result = runSql(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    assert(result.length == 2)
    val expected = Seq(Row(1.0, 3283.0), Row(2.0, 1062.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("test_orderby") {
    val result = runSql(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    assert(result.length == 7)

    val expected =
      Seq(Row(48.0), Row(10.0), Row(23.0), Row(38.0), Row(75.0), Row(33.0), Row(93.0))
    TestUtils.compareAnswers(result, expected)
  }

  test("test_orderby expression") {
    val result = runSql(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    assert(result.length == 7)

    val expected =
      Seq(Row(48.0), Row(10.0), Row(23.0), Row(38.0), Row(75.0), Row(33.0), Row(93.0))
    TestUtils.compareAnswers(result, expected)
  }
}
