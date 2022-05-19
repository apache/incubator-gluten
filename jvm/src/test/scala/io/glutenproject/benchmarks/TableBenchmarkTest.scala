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

object TableBenchmarkTest {

  def main(args: Array[String]): Unit = {

    val (parquetFilesPath, fileFormat,
    executedCnt, configed, sqlFilePath, stopFlagFile,
    createTable, metaRootPath) = if (args.length > 0) {
      (args(0), args(1), args(2).toInt, true, args(3), args(4), args(5).toBoolean, args(6))
    } else {
      val rootPath = this.getClass.getResource("/").getPath
      val resourcePath = rootPath + "../../../src/test/resources/"
      val dataPath = resourcePath + "/tpch-data/"
      val queryPath = resourcePath + "/tpch-queries/"
      (new File(dataPath).getAbsolutePath, "parquet", 1, false, queryPath + "q06.sql", "", true,
      "")
    }

    val (warehouse, metaStorePathAbsolute, hiveMetaStoreDB) = if (!metaRootPath.isEmpty) {
      (metaRootPath + "/spark-warehouse", metaRootPath + "/meta",
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
        .master("local[3]")
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
        //.config("spark.sql.sources.useV1SourceList", "avro")
        .config("spark.memory.fraction", "0.3")
        .config("spark.memory.storageFraction", "0.3")
        //.config("spark.sql.parquet.columnarReaderBatchSize", "20000")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        //.config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        //.config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.gluten.sql.columnar.columnartorow", "false")
        .config(GlutenConfig.GLUTEN_LOAD_NATIVE, "true")
        .config(GlutenConfig.GLUTEN_LOAD_ARROW, "false")
        .config(GlutenConfig.GLUTEN_LIB_PATH,
          "/usr/local/clickhouse/lib/libch.so")
        .config("spark.gluten.sql.columnar.iterator", "false")
        .config("spark.gluten.sql.columnar.ch.mergetree.enabled", "true")
        .config("spark.gluten.sql.columnar.ch.mergetree.table.path",
          "data1/clickhouse-test/test-tpch10/")
        .config("spark.gluten.sql.columnar.ch.mergetree.database", "default")
        .config("spark.gluten.sql.columnar.ch.mergetree.table", "test")
        //.config("spark.sql.planChangeLog.level", "info")
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "6442450944")

      if (!warehouse.isEmpty) {
        sessionBuilderTmp1.config("spark.sql.warehouse.dir", warehouse)
          .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
          .enableHiveSupport()
      } else {
        sessionBuilderTmp1.enableHiveSupport()
      }
    } else {
      sessionBuilderTmp
    }

    val spark = sessionBuilder.getOrCreate()
    if (!configed) {
      spark.sparkContext.setLogLevel("WARN")
    }

    if (createTable) {
      createTables(spark, parquetFilesPath, fileFormat)
    }
    testSQL(spark, parquetFilesPath, fileFormat, executedCnt, sqlStr.mkString)

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

  def testSQL(spark: SparkSession, parquetFilesPath: String,
              fileFormat: String, executedCnt: Int,
              sql: String): Unit = {

    /*spark.sql(
      s"""
         | show databases;
         |""".stripMargin).show(100, false)
    spark.sql(
      s"""
         | show tables;
         |""".stripMargin).show(100, false)*/

    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(sql).show(200, false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    spark.conf.set("spark.gluten.sql.enable.native.engine", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }

  def createTables(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    val dataSourceMap = Map(
      "customer" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),

      "lineitem" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),

      "nation" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),

      "region" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),

      "orders" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),

      "part" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),

      "partsupp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),

      "supplier" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier"))

    val customerData = parquetFilesPath + "/customer"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
         | c_custkey    bigint,
         | c_name       string,
         | c_address    string,
         | c_nationkey  bigint,
         | c_phone      string,
         | c_acctbal    double,
         | c_mktsegment string,
         | c_comment    string)
         | STORED AS PARQUET LOCATION '${customerData}'
         |""".stripMargin).show(1, false)

    val lineitemData = parquetFilesPath + "/lineitem"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
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
         | STORED AS PARQUET LOCATION '${lineitemData}'
         |""".stripMargin).show(1, false)

    val nationData = parquetFilesPath + "/nation"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | STORED AS PARQUET LOCATION '${nationData}'
         |""".stripMargin).show(1, false)

    val regionData = parquetFilesPath + "/region"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS region (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | STORED AS PARQUET LOCATION '${regionData}'
         |""".stripMargin).show(1, false)

    val ordersData = parquetFilesPath + "/order"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
         | o_orderkey      bigint,
         | o_custkey       bigint,
         | o_orderstatus   string,
         | o_totalprice    double,
         | o_orderdate     date,
         | o_orderpriority string,
         | o_clerk         string,
         | o_shippriority  bigint,
         | o_comment       string)
         | STORED AS PARQUET LOCATION '${ordersData}'
         |""".stripMargin).show(1, false)

    val partData = parquetFilesPath + "/part"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS part (
         | p_partkey     bigint,
         | p_name        string,
         | p_mfgr        string,
         | p_brand       string,
         | p_type        string,
         | p_size        bigint,
         | p_container   string,
         | p_retailprice double,
         | p_comment     string)
         | STORED AS PARQUET LOCATION '${partData}'
         |""".stripMargin).show(1, false)

    val partsuppData = parquetFilesPath + "/partsupp"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | STORED AS PARQUET LOCATION '${partsuppData}'
         |""".stripMargin).show(1, false)

    val supplierData = parquetFilesPath + "/supplier"
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | STORED AS PARQUET LOCATION '${supplierData}'
         |""".stripMargin).show(1, false)

    /*spark.sql(
      s"""
         | show databases;
         |""".stripMargin).show(100, false)
    spark.sql(
      s"""
         | show tables;
         |""".stripMargin).show(100, false)
    dataSourceMap.foreach {
      case (key, value) => {
        println(s"----------------create table $key")
        spark.sql(
          s"""
             | desc $key;
             |""".stripMargin).show(100, false)
        spark.sql(
          s"""
             | select count(1) from $key;
             |""".stripMargin).show(10, false)
      }
    }*/
  }
}
