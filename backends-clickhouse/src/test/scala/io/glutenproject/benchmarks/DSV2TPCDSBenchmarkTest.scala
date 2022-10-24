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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog

// scalastyle:off
object DSV2TPCDSBenchmarkTest extends AdaptiveSparkPlanHelper {

  def main(args: Array[String]): Unit = {

    // val libPath = "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse-1/" +
    //   "cmake-build-release/utils/local-engine/libch.so"
    val libPath = "/usr/local/clickhouse/lib/libch.so"
    val thrdCnt = 12
    val shufflePartitions = 12
    val shuffleManager = "sort"
    // val shuffleManager = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
    val ioCompressionCodec = "SNAPPY"
    val columnarColumnToRow = "true"
    val useV2 = "false"
    val separateScanRDD = "true"
    val coalesceBatches = "true"
    val broadcastThreshold = "10MB" // 100KB  10KB
    val adaptiveEnabled = "true"
    val sparkLocalDir = "/data1/gazelle-jni-warehouse/spark_local_dirs"
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
      val resourcePath = rootPath + "../../../../jvm/src/test/resources/"
      val dataPath = resourcePath + "/tpch-data/"
      val queryPath = resourcePath + "/queries/"
      // (new File(dataPath).getAbsolutePath, "parquet", 1, false, queryPath + "q06.sql", "", true,
      // "/data1/gazelle-jni-warehouse")
      (
        "/data1/test_output/tpcds-data-sf10",
        "parquet",
        1,
        false,
        queryPath + "q01.sql",
        "",
        true,
        "/data1/gazelle-jni-warehouse")
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
      .appName("Gluten-TPCDS-Benchmark")

    val sessionBuilder = if (!configed) {
      val sessionBuilderTmp1 = sessionBuilderTmp
        .master(s"local[${thrdCnt}]")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.driver.memory", "30G")
        .config("spark.driver.memoryOverhead", "10G")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.default.parallelism", 1)
        .config("spark.sql.shuffle.partitions", shufflePartitions)
        .config("spark.sql.adaptive.enabled", adaptiveEnabled)
        .config("spark.sql.adaptive.logLevel", "DEBUG")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        // .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "")
        // .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "")
        .config("spark.sql.adaptive.fetchShuffleBlocksInBatch", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
        .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2")
        // .config("spark.sql.adaptive.optimizer.excludedRules", "")
        .config("spark.sql.files.maxPartitionBytes", 1024 << 10 << 10) // default is 128M
        .config("spark.sql.files.openCostInBytes", 1024 << 10 << 10) // default is 4M
        .config("spark.sql.files.minPartitionNum", "1")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.locality.wait", "0s")
        .config("spark.sql.sources.ignoreDataLocality", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        // .config("spark.sql.sources.useV1SourceList", "avro")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        // .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "128")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
        .config("spark.shuffle.manager", shuffleManager)
        .config("spark.shuffle.compress", "true")
        .config("spark.io.compression.codec", ioCompressionCodec)
        .config("spark.io.compression.zstd.bufferSize", "32k")
        .config("spark.io.compression.zstd.level", "1")
        .config("spark.io.compression.zstd.bufferPool.enabled", "true")
        .config("spark.io.compression.snappy.blockSize", "32k")
        .config("spark.io.compression.lz4.blockSize", "32k")
        .config("spark.reducer.maxSizeInFlight", "48m")
        .config("spark.shuffle.file.buffer", "32k")
        // .config("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192")
        .config("spark.databricks.delta.maxSnapshotLineageLength", 20)
        .config("spark.databricks.delta.snapshotPartitions", 1)
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", 5)
        .config("spark.databricks.delta.stalenessLimit", 3600 * 1000)
        // .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.gluten.sql.columnar.columnartorow", columnarColumnToRow)
        // .config("spark.gluten.sql.columnar.backend.lib", "ch")
        .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .config("spark.gluten.sql.columnar.backend.ch.use.v2", useV2)
        .config(GlutenConfig.GLUTEN_LOAD_NATIVE, "true")
        .config(GlutenConfig.GLUTEN_LOAD_ARROW, "false")
        .config(GlutenConfig.GLUTEN_LIB_PATH, libPath)
        .config("spark.gluten.sql.columnar.iterator", "true")
        .config("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
        .config("spark.gluten.sql.enable.native.validation", "false")
        .config("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", separateScanRDD)
        // .config("spark.gluten.sql.columnar.sort", "false")
        // .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", broadcastThreshold)
        .config("spark.sql.exchange.reuse", "true")
        .config("spark.sql.execution.reuseSubquery", "true")
        .config("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
        .config("spark.gluten.sql.columnar.coalesce.batches", coalesceBatches)
        // .config("spark.gluten.sql.columnar.filescan", "true")
        // .config("spark.sql.optimizeNullAwareAntiJoin", "false")
        // .config("spark.sql.join.preferSortMergeJoin", "false")
        .config("spark.sql.shuffledHashJoinFactor", "3")
        // .config("spark.sql.planChangeLog.level", "warn")
        // .config("spark.sql.planChangeLog.batches", "ApplyColumnarRulesAndInsertTransitions")
        // .config("spark.sql.optimizer.inSetConversionThreshold", "5")  // IN to INSET
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "20G")
        .config("spark.shuffle.sort.bypassMergeThreshold", "200")
        .config("spark.local.dir", sparkLocalDir)
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "300s")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
        .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "true")
        // .config("spark.sql.parquet.footer.use.old.api", "false")
        // .config("spark.sql.fileMetaCache.parquet.enabled", "true")
        // .config("spark.sql.columnVector.custom.clazz",
        //   "org.apache.spark.sql.execution.vectorized.PublicOffHeapColumnVector")
        // .config("spark.hadoop.io.file.buffer.size", "524288")
        .config("spark.sql.codegen.comments", "true")
        .config("spark.ui.retainedJobs", "2500")
        .config("spark.ui.retainedStages", "5000")

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
      spark.sparkContext.setLogLevel("WARN")
    }

    val createTbl = false
    if (createTbl) {
      createTables(spark, parquetFilesPath, fileFormat)
      // createClickHouseTables(spark)
    }
    val refreshTable = false
    if (refreshTable) {
      refreshClickHouseTable(spark)
    }
    // scalastyle:off println
    println("start to query ... ")
    /* spark.sql(
      s"""
         |show databases;
         |""".stripMargin).show(1000, false)
    spark.sql(
      s"""
         |use tpcdsdb1;
         |""".stripMargin).show(1000, false)
    spark.sql(
      s"""
         |desc formatted inventory;
         |""".stripMargin).show(1000, false) */

    testTPCDSOne(spark, executedCnt)
    // testTPCDSAll(spark)
    // testTPCDSDecimalOne(spark, executedCnt)
    // testTPCDSDecimalAll(spark)
    // benchmarkTPCH(spark, executedCnt)

    System.out.println("waiting for finishing")
    ClickHouseLog.clearCache()
    // JniLibLoader.unloadNativeLibs(libPath)
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

  def testTPCDSOne(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
         |use tpcdsdb;
         |""".stripMargin).show(1000, false)

    val tookTimeArr = ArrayBuffer[Long]()
    val sqlFilePath = "/data2/tpcds-data-gen/tpcds10-queries/"
    val execNum = 21
    val sqlNum = "q" + execNum + ".sql"
    val sqlFile = sqlFilePath + sqlNum
    val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(sqlStr) // .show(30, false)
      // df.explain(false)
      // df.queryExecution.debug.codegen
      val result = df.collect() // .show(100, false)  //.collect()
      df.explain(false)
      val plan = df.queryExecution.executedPlan
      DSV2BenchmarkTest.collectAllJoinSide(plan)
      println(result.size)
      // result.foreach(r => println(r.mkString(",")))
      result.foreach(r => println(r.mkString("|-|")))
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
      // Thread.sleep(5000)
    }

    println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      df.summary().show(100, false)
    }
  }

  def testTPCDSAll(spark: SparkSession): Unit = {
    spark.sql(s"""
         |use tpcdsdb;
         |""".stripMargin).show(1000, false)

    val tookTimeArr = ArrayBuffer[Long]()
    val executedCnt = 1
    val executeExplain = false
    val printData = true
    val sqlFilePath = "/data2/tpcds-data-gen/tpcds10-queries/"
    for (i <- 1 to 99) {
      val sqlStrArr = new ArrayBuffer[String]()
      if (i == 14 || i == 23 || i == 24 || i == 39) {
        var sqlNum = "q" + "%d".format(i) + "-1"
        sqlStrArr += sqlNum
        sqlNum = "q" + "%d".format(i) + "-2"
        sqlStrArr += sqlNum
      } else {
        val sqlNum = "q" + "%d".format(i)
        sqlStrArr += sqlNum
      }

      for (sqlNum <- sqlStrArr) {
        val sqlFile = sqlFilePath + sqlNum + ".sql"
        val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
        println("")
        println(s"execute sql: ${sqlNum}")
        for (j <- 1 to executedCnt) {
          val startTime = System.nanoTime()
          val df = spark.sql(sqlStr)
          val result = df.collect()
          if (executeExplain) df.explain(false)
          println(result.size)
          if (printData) result.foreach(r => println(r.mkString(",")))
          // .show(30, false)
          // .explain(false)
          // .collect()
          val tookTime = (System.nanoTime() - startTime) / 1000000
          // println(s"Execute ${i} time, time: ${tookTime}")
          tookTimeArr += tookTime
        }
      }
    }

    // println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      df.summary().show(100, false)
    }
  }

  def testTPCDSDecimalOne(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
         |use tpcdsdb_decimal;
         |""".stripMargin).show(1000, false)

    val tookTimeArr = ArrayBuffer[Long]()
    val sqlFilePath = "/data2/tpcds-data-gen/tpcds10-queries/"
    val execNum = 2
    val sqlNum = "q" + execNum + ".sql"
    val sqlFile = sqlFilePath + sqlNum
    val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(sqlStr) // .show(30, false)
      val plan = df.queryExecution.executedPlan
      // df.queryExecution.debug.codegen
      val result = df.collect() // .show(100, false)  //.collect()
      df.explain(false)
      println(result.size)
      result.foreach(r => println(r.mkString(",")))
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
      // Thread.sleep(5000)
    }

    println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      df.summary().show(100, false)
    }
  }

  def testTPCDSDecimalAll(spark: SparkSession): Unit = {
    spark.sql(s"""
         |use tpcdsdb_decimal;
         |""".stripMargin).show(1000, false)

    val tookTimeArr = ArrayBuffer[Long]()
    val executedCnt = 1
    val executeExplain = true
    val printData = false
    val sqlFilePath = "/data2/tpcds-data-gen/tpcds10-queries/"
    for (i <- 1 to 99) {
      val sqlStrArr = new ArrayBuffer[String]()
      if (i == 14 || i == 23 || i == 24 || i == 39) {
        var sqlNum = "q" + "%d".format(i) + "-1"
        sqlStrArr += sqlNum
        sqlNum = "q" + "%d".format(i) + "-2"
        sqlStrArr += sqlNum
      } else {
        val sqlNum = "q" + "%d".format(i)
        sqlStrArr += sqlNum
      }

      for (sqlNum <- sqlStrArr) {
        val sqlFile = sqlFilePath + sqlNum + ".sql"
        val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
        println("")
        println("")
        println(s"execute sql: ${sqlNum}")
        for (j <- 1 to executedCnt) {
          val startTime = System.nanoTime()
          val df = spark.sql(sqlStr)
          val result = df.collect()
          if (executeExplain) df.explain(false)
          println(result.size)
          if (printData) result.foreach(r => println(r.mkString(",")))
          // .show(30, false)
          // .explain(false)
          // .collect()
          val tookTime = (System.nanoTime() - startTime) / 1000000
          // println(s"Execute ${i} time, time: ${tookTime}")
          tookTimeArr += tookTime
        }
      }
    }

    // println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      df.summary().show(100, false)
    }
  }

  def benchmarkTPCH(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
         |use tpcdsdb;
         |""".stripMargin).show(1000, false)

    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(s"""
           |
           |""".stripMargin) // .show(30, false)
      // df.explain(false)
      val result = df.collect() // .show(100, false)  //.collect()
      println(result.size)
      // result.foreach(r => println(r.mkString(",")))
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }

  def createTables(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    val csv2Parquet = GenTPCDSTableScripts.genTPCDSCSV2ParquetSQL(
      "tpcds_gendb",
      "tpcdsdb1",
      "/data2/tpcds-data-gen/tpcds1-data/",
      "/data1/test_output/tpcds-data-sf1/",
      "tpcdsdb1.",
      "")
    val csv2ParquetDecimal = GenTPCDSDecimalTableScripts.genTPCDSCSV2ParquetSQL(
      "tpcds_gendb",
      "tpcdsdb_decimal",
      "/data2/tpcds-data-gen/tpcds10-data/",
      "/data1/test_output/tpcds-data-sf10-decimal/",
      "tpcdsdb_decimal.",
      "")

    val parquetDecimalTables = GenTPCDSDecimalTableScripts.genTPCDSParquetTables(
      "tpcdsdb_decimal",
      "/data1/test_output/tpcds-data-sf10-decimal/",
      "",
      "")
    val parquetTables = GenTPCDSTableScripts.genTPCDSParquetTables(
      "tpcdsdb",
      "/data1/test_output/tpcds-data-sf10/",
      "",
      "")

    for (sql <- csv2Parquet) {
      println(s"execute: ${sql}")
      spark.sql(sql).show(10, false)
    }
  }

  def createClickHouseTables(spark: SparkSession): Unit = {}

  def refreshClickHouseTable(spark: SparkSession): Unit = {
    val tableName = ""
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
}
// scalastyle:on
