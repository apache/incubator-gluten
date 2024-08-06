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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import org.apache.commons.io.FileUtils

import java.io.File

class GlutenClickhouseFunctionSuite extends GlutenClickHouseTPCHAbstractSuite {
  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  private var _hiveSpark: SparkSession = _
  override protected def spark: SparkSession = _hiveSpark

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1073741824")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.sql.warehouse.dir", warehouse)
      .setMaster("local[1]")
  }

  override protected def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
      _hiveSpark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .config(
          "javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
        .getOrCreate()
    }
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    FileUtils.copyDirectory(new File(rootPath + resourcePath), new File(tablesPath))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()

    try {
      super.afterAll()
    } finally {
      try {
        if (_hiveSpark != null) {
          try {
            _hiveSpark.sessionState.catalog.reset()
          } finally {
            _hiveSpark.stop()
            _hiveSpark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  test("test uuid - write and read") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      (GlutenConfig.GLUTEN_ENABLED.key, "true")) {

      spark.sql("drop table if exists uuid_test")
      spark.sql("create table if not exists uuid_test (id string) stored as parquet")

      val df = spark.sql("select regexp_replace(uuid(), '-', '') as id from range(1)")
      df.cache()
      df.write.insertInto("uuid_test")

      val df2 = spark.table("uuid_test")
      val diffCount = df.exceptAll(df2).count()
      assert(diffCount == 0)
    }
  }

  test("Support In list option contains non-foldable expression") {
    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey - 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey + 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("GLUTEN-5981 null value from get_json_object") {
    spark.sql("create table json_t1 (a string) using parquet")
    spark.sql("insert into json_t1 values ('{\"a\":null}')")
    runQueryAndCompare(
      """
        |SELECT get_json_object(a, '$.a') is null from json_t1
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
    spark.sql("drop table json_t1")
  }

  test("Fix arrayDistinct(Array(Nullable(Decimal))) core dump") {
    val create_sql =
      """
        |create table if not exists test(
        | dec array<decimal(10, 2)>
        |) using parquet
        |""".stripMargin
    val fill_sql =
      """
        |insert into test values(array(1, 2, null)), (array(null, 2,3, 5))
        |""".stripMargin
    val query_sql =
      """
        |select array_distinct(dec) from test;
        |""".stripMargin
    spark.sql(create_sql)
    spark.sql(fill_sql)
    compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    spark.sql("drop table test")
  }

  test("intersect all") {
    spark.sql("create table t1 (a int, b string) using parquet")
    spark.sql("insert into t1 values (1, '1'),(2, '2'),(3, '3'),(4, '4'),(5, '5'),(6, '6')")
    spark.sql("create table t2 (a int, b string) using parquet")
    spark.sql("insert into t2 values (4, '4'),(5, '5'),(6, '6'),(7, '7'),(8, '8'),(9, '9')")
    runQueryAndCompare(
      """
        |SELECT a,b FROM t1 INTERSECT ALL SELECT a,b FROM t2
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
    spark.sql("drop table t1")
    spark.sql("drop table t2")
  }

  test("array decimal32") {
    runQueryAndCompare(
      """
        |SELECT array(1.0, 2.0)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

}
