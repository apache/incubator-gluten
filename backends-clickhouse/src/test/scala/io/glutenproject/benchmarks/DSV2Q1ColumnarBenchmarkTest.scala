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

package io.glutenproject.benchmarks

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import io.glutenproject.GlutenConfig

import org.apache.spark.sql.SparkSession

object DSV2Q1ColumnarBenchmarkTest {

  val tableName = "lineitem_ch"

  def main(args: Array[String]): Unit = {

    val (
      parquetFilesPath,
      fileFormat,
      executedCnt,
      configed,
      sqlFilePath,
      stopFlagFile,
      createTable,
      metaRootPath) = if (args.length > 0) {
      (args(0), args(1), args(2).toInt, true, args(3), args(4), args(5).toBoolean, args(6))
    } else {
      val rootPath = this.getClass.getResource("/").getPath
      val resourcePath = rootPath + "../../../src/test/resources/"
      val dataPath = resourcePath + "/tpch-data/"
      val queryPath = resourcePath + "/queries/"
      (
        new File(dataPath).getAbsolutePath,
        "parquet",
        1,
        false,
        queryPath + "q01.sql",
        "",
        true,
        "/tmp/gluten-warehouse")
    }

    val (warehouse, metaStorePathAbsolute, hiveMetaStoreDB) = if (!metaRootPath.isEmpty) {
      (
        metaRootPath + "/spark-warehouse",
        metaRootPath + "/meta",
        metaRootPath + "/meta/metastore_db")
    } else {
      ("/tmp/spark-warehouse", "/tmp/meta", "/tmp/meta/metastore_db")
    }

    if (!warehouse.isEmpty) {
      val warehouseDir = new File(warehouse)
      if (!warehouseDir.exists()) {
        warehouseDir.mkdirs()
      }
      val hiveMetaStoreDBDir = new File(metaStorePathAbsolute)
      if (!hiveMetaStoreDBDir.exists()) {
        hiveMetaStoreDBDir.mkdirs()
      }
    }

    val sqlStr = Source.fromFile(new File(sqlFilePath), "UTF-8")

    val sessionBuilderTmp = SparkSession
      .builder()
      .appName("Gluten-Benchmark")

    val sessionBuilder = if (!configed) {
      val sessionBuilderTmp1 = sessionBuilderTmp
        .master("local[1]")
        .config("spark.driver.memory", "4G")
        .config("spark.driver.memoryOverhead", "6G")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.default.parallelism", 1)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.files.maxPartitionBytes", 1024 << 10 << 10) // default is 128M
        .config("spark.sql.files.minPartitionNum", "1")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.locality.wait", "0s")
        .config("spark.sql.sources.ignoreDataLocality", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        // .config("spark.sql.sources.useV1SourceList", "avro")
        .config("spark.memory.fraction", "0.3")
        .config("spark.memory.storageFraction", "0.3")
        // .config("spark.sql.parquet.columnarReaderBatchSize", "20000")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
        .config("spark.databricks.delta.maxSnapshotLineageLength", 20)
        .config("spark.databricks.delta.snapshotPartitions", 1)
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", 5)
        .config("spark.databricks.delta.stalenessLimit", 3600 * 1000)
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        // .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.gluten.sql.columnar.columnartorow", "false")
        .config(GlutenConfig.GLUTEN_LOAD_NATIVE, "true")
        .config(GlutenConfig.GLUTEN_LOAD_ARROW, "false")
        .config(GlutenConfig.GLUTEN_LIB_PATH, "/usr/local/clickhouse/lib/libch.so")
        .config("spark.gluten.sql.columnar.iterator", "true")
        // .config("spark.sql.planChangeLog.level", "info")
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "6442450944")
        .config("spark.io.compression.codec", "LZ4")

      if (!warehouse.isEmpty) {
        sessionBuilderTmp1
          .config("spark.sql.warehouse.dir", warehouse)
          .config(
            "javax.jdo.option.ConnectionURL",
            s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
          .enableHiveSupport()
      } else {
        sessionBuilderTmp1.enableHiveSupport()
      }
    } else {
      sessionBuilderTmp
    }

    val spark = sessionBuilder.getOrCreate()
    if (!configed) {
      // spark.sparkContext.setLogLevel("WARN")
    }

    val createTbl = true
    if (createTbl) {
      //      createClickHouseTables(spark, parquetFilesPath, fileFormat)
      createLocationClickHouseTable(spark)
    }
    val refreshTable = true
    if (refreshTable) {
      refreshClickHouseTable(spark)
    }
    //    selectClickHouseTable(spark, executedCnt, sqlStr.mkString)
    selectLocationClickHouseTable(spark, executedCnt, sqlStr.mkString)
    // scalastyle:off println
    System.out.println("waiting for finishing")
    if (stopFlagFile.isEmpty) {
      Thread.sleep(1800000)
    } else {
      while ((new File(stopFlagFile)).exists()) {
        Thread.sleep(1000)
      }
    }
    spark.stop()
    System.out.println("finished")
  }

  def createLocationClickHouseTable(spark: SparkSession): Unit = {
    spark.sql(s"""
         | USE default
         |""".stripMargin).show(100, false)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS ch_clickhouse")

    // Create a table
    // PARTITIONED BY (age)
    // engine='MergeTree' or engine='Parquet'
    spark.sql(s"""
         | CREATE TABLE IF NOT EXISTS ch_clickhouse (
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
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '/home/saber/Documents/data/mergetree'
         |""".stripMargin)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)
  }

  def refreshClickHouseTable(spark: SparkSession): Unit = {
    spark.sql(s"""
         | refresh table ${tableName}
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | desc formatted ${tableName}
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | refresh table ch_clickhouse
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | desc formatted ch_clickhouse
         |""".stripMargin).show(100, false)
  }

  def selectLocationClickHouseTable(spark: SparkSession, executedCnt: Int, sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    l_returnflag,
           |    l_linestatus,
           |    sum(l_quantity) as sum_qty,
           |    sum(l_extendedprice) as sum_base_price,
           |    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
           |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
           |    avg(l_quantity) as avg_qty,
           |    avg(l_extendedprice) as avg_price,
           |    avg(l_discount) as avg_disc,
           |    count(*) as count_order
           |FROM
           |    ch_clickhouse
           |WHERE
           |    l_shipdate <= date '1998-12-01' - interval '90' day
           |GROUP BY
           |    l_returnflag,
           |    l_linestatus
           |ORDER BY
           |    l_returnflag,
           |    l_linestatus;
           |""".stripMargin).show(200, false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.sql.enable.native.engine", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }

  def createClickHouseTables(
      spark: SparkSession,
      parquetFilesPath: String,
      fileFormat: String): Unit = {
    spark.sql("""
        | show databases
        |""".stripMargin).show(100, false)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)

    spark.sql(s"""
         | USE default
         |""".stripMargin).show(100, false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create a table
    println("Creating a table")
    // PARTITIONED BY (age)
    // engine='MergeTree' or engine='Parquet'
    spark.sql(s"""
         | CREATE TABLE IF NOT EXISTS $tableName (
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
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         |""".stripMargin)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)

    spark.sql(s"""
         | desc formatted ${tableName}
         |""".stripMargin).show(100, false)

  }

  def selectClickHouseTable(spark: SparkSession, executedCnt: Int, sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    l_returnflag,
           |    l_linestatus,
           |    sum(l_quantity) as sum_qty,
           |    sum(l_extendedprice) as sum_base_price,
           |    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
           |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
           |    avg(l_quantity) as avg_qty,
           |    avg(l_extendedprice) as avg_price,
           |    avg(l_discount) as avg_disc,
           |    count(*) as count_order
           |FROM
           |    ${tableName}
           |WHERE
           |    l_shipdate <= date '1998-12-01' - interval '90' day
           |GROUP BY
           |    l_returnflag,
           |    l_linestatus
           |ORDER BY
           |    l_returnflag,
           |    l_linestatus;
           |""".stripMargin).show(200, false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.sql.enable.native.engine", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }
}
