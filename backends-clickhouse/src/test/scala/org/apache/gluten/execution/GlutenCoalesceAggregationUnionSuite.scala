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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.types._

import java.nio.file.Files

class GlutenCoalesceAggregationUnionSuite extends GlutenClickHouseWholeStageTransformerSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set(CHBackendSettings.GLUTEN_ENABLE_COALESCE_PROJECT_UNION, "true")
      .set(CHBackendSettings.GLUTEN_ENABLE_COALESCE_AGGREGATION_UNION, "true")
  }

  def createTestTable(tableName: String, data: DataFrame): Unit = {
    val tempFile = Files.createTempFile("", ".parquet").toFile
    tempFile.deleteOnExit()
    val tempFilePath = tempFile.getAbsolutePath
    data.coalesce(1).write.format("parquet").mode("overwrite").parquet(tempFilePath)
    spark.catalog.createTable(tableName, tempFilePath, "parquet")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val schema = StructType(
      Array(
        StructField("a", StringType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("x", StringType, nullable = true),
        StructField("y", IntegerType, nullable = true)
      ))
    val data1 = sparkContext.parallelize(
      Seq(
        Row("a", 1, null, 1),
        Row("a", 2, "a", 2),
        Row("a", 3, "b", 3),
        Row("a", 4, "c", 4),
        Row("b", 1, "d", 5),
        Row("b", 2, "e", 6),
        Row("b", 3, "f", 7),
        Row("b", 4, "g", null)
      ))

    val dataFrame1 = spark.createDataFrame(data1, schema)
    createTestTable("coalesce_union_t1", dataFrame1)
    createTestTable("coalesce_union_t2", dataFrame1)

    val data2 = sparkContext.parallelize(
      Seq(
        Row("a", 1, null, 1),
        Row("a", 2, "a", 2)
      ))
    val dataFrame2 = spark.createDataFrame(data2, schema)
    createTestTable("coalesce_union_t3", dataFrame2)
  }

  def checkNoUnion(df: DataFrame): Unit = {
    val unions = collectWithSubqueries(df.queryExecution.executedPlan) {
      case e: ColumnarUnionExec => e
    }
    assert(unions.isEmpty)
  }

  def checkHasUnion(df: DataFrame): Unit = {
    val unions = collectWithSubqueries(df.queryExecution.executedPlan) {
      case e: ColumnarUnionExec => e
    }
    assert(unions.size == 1)
  }

  test("coalesce aggregation union. case 1") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 2
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 2") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b > 1
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 3") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, 1 as t, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, 2 as t, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, t, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 4") {
    val sql =
      """
        |select * from (
        | select a, 1 as t, count(x) + sum(y) as n from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, 2 as t, count(x) + sum(y) as n from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, t, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 5") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 6") {
    val sql =
      """
        |select * from (
        | select y + 1 as y , count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by y + 1
        | union all
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by y + 1
        |) order by y, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 7") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x, sum(y + 1) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, count(x) as x, sum(y + 1) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 8") {
    val sql =
      """
        |select * from (
        | select a as a, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 9") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 2
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 3
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 10") {
    val sql =
      """
        |select * from (
        | select a as a, sum(y) as y from (
        |   select concat(a, "x") as a, y from coalesce_union_t1 where b % 3 = 0
        | ) group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 11") {
    val sql =
      """
        |select t1.a, t1.y, t2.x from (
        | select a as a, sum(y) as y from (
        |   select concat(a, "x") as a, y from coalesce_union_t1 where b % 3 = 0
        | ) group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) as t1
        |left join (
        | select a, x from coalesce_union_t2
        |) as t2
        |on t1.a = t2.a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 12") {
    val sql =
      """
        |select a, x, y from (
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        | union all
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("no coalesce aggregation union. case 1") {
    val sql =
      """
        |select * from (
        | select a, count(x) + 1 as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) + 1 as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 2") {
    val sql =
      """
        |select * from (
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by y
        | union all
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by y
        |) order by y, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 3") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(y) as x from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 4") {
    val sql =
      """
        |select * from (
        | select a, 1 as b, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a, 1
        | union all
        | select a, b, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, b, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 5") {
    val sql =
      """
        |select * from (
        | select a, b, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, b, count(x) as x from coalesce_union_t2 where b % 3 = 1
        |   group by a, b
        |) order by a, b, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 6") {
    val sql =
      """
        |select * from (
        | select a as k1, x as k2, count(y) as c from coalesce_union_t1 where b % 3 = 0
        |   group by a, x
        | union all
        | select x as k1, a as k2, count(y) as c from coalesce_union_t1 where b % 3 = 1
        |   group by a, x
        |) order by k1, k2, c
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 7") {
    val sql =
      """
        |select * from (
        | select a, count(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(y) as y from coalesce_union_t2
        |   group by a
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 8") {
    val sql =
      """
        |select a, x, y from (
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("coalesce project union. case 1") {

    val sql =
      """
        |select a, x, y from (
        | select a, x, y from coalesce_union_t1 where b % 2 = 0
        | union all
        | select a, x, y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce project union. case 2") {
    val sql =
      """
        |select a, x, y from (
        | select concat(a, 'x') as a , x, y from coalesce_union_t1 where b % 2 = 0
        | union all
        | select a, x, y + 2 as y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce project union. case 3") {
    val sql =
      """
        |select a, x, y from (
        | select concat(a, 'x') as a , x, y, 1 as t from coalesce_union_t1 where b % 2 = 0
        | union all
        | select a, x, y + 2 as y, 2 as t from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y, t
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce project union. case 4") {
    val sql =
      """
        |select a, x, y from (
        | select concat(a, 'x') as a , x, 1 as y from coalesce_union_t1 where b % 2 = 0
        | union all
        | select a, x, y + 2 as y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce project union. case 5") {
    val sql =
      """
        |select a, x, y from (
        | select a, x, y from (select a, x, y, b + 4 as b from coalesce_union_t1) where b % 2 = 0
        | union all
        | select a, x, y + 2 as y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce project union. case 6") {

    val sql =
      """
        |select a, x, y, t from (
        | select a, x, y, 1 as t from coalesce_union_t1 where b % 2 = 0
        | union all
        | select a, x, y, 2 as t from coalesce_union_t1 where b % 3 = 1
        | union all
        | select a, x, y, 3 as t from coalesce_union_t1 where b % 4 = 1
        | union all
        | select a, x, y, 4 as t from coalesce_union_t1 where b % 5 = 1
        |) order by a, x, y, t
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("no coalesce project union. case 1") {
    val sql =
      """
        |select a, x, y from (
        | select a, x, y from coalesce_union_t1
        | union all
        | select a, x, y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce project union. case 2") {
    val sql =
      """
        |select a, x, y from (
        | select a , x, y from coalesce_union_t2 where b % 2 = 0
        | union all
        | select a, x, y from coalesce_union_t1 where b % 3 = 1
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce project union. case 3") {
    val sql =
      """
        |select a from (
        |   select a from coalesce_union_t1 where b % 2 = 0
        |   union all
        |   select a from coalesce_union_t1 where a in (select a from coalesce_union_t3)
        |) order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("GLUTEN-9646: fix coalesce project union when has subquery") {
    val schema_fact = StructType(
      Array(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true)
      ))

    val schema_order = StructType(
      Array(
        StructField("c", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true)
      ))

    val data_fact = sparkContext.parallelize(
      Seq(
        Row(2, 1),
        Row(3, 2),
        Row(4, 3),
        Row(5, 4)
      ))

    val data_order = sparkContext.parallelize(
      Seq(
        Row(1, 1),
        Row(2, 2),
        Row(3, 3),
        Row(4, 4)
      ))

    val dataFrame1 = spark.createDataFrame(data_fact, schema_fact)
    val dataFrame2 = spark.createDataFrame(data_order, schema_order)
    createTestTable("fact", dataFrame1)
    createTestTable("order", dataFrame2)

    val sql =
      """
        |SELECT a
        |FROM fact
        |WHERE a =
        |    (SELECT sum(c) + 2
        |     FROM order
        |     WHERE order.b = fact.b
        |     GROUP BY order.b)
        |UNION ALL
        |SELECT a
        |FROM fact
        |WHERE a =
        |    (SELECT sum(c) + 1
        |     FROM order
        |     WHERE order.b = fact.b
        |     GROUP BY order.b)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkNoUnion)
  }
}
