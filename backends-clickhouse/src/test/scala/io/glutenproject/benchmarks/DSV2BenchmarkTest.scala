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
import io.glutenproject.execution.{
  BroadcastHashJoinExecTransformer,
  ShuffledHashJoinExecTransformer
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog
import org.apache.spark.sql.execution.joins.{
  BroadcastHashJoinExec,
  ShuffledHashJoinExec,
  SortMergeJoinExec
}

// scalastyle:off
object DSV2BenchmarkTest extends AdaptiveSparkPlanHelper {

  val tableName = "lineitem_ch"

  def main(args: Array[String]): Unit = {

    // val libPath = "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse-1/" +
    //   "cmake-build-release/utils/local-engine/libch.so"
    val libPath = "/usr/local/clickhouse/lib/libch.so"
    val thrdCnt = 4
    val shufflePartitions = 12
    val shuffleManager = "sort"
    // val shuffleManager = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
    val ioCompressionCodec = "SNAPPY"
    val columnarColumnToRow = "true"
    val useV2 = "false"
    val separateScanRDD = "true"
    val coalesceBatches = "true"
    val broadcastThreshold = "10MB"
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
        "/data1/test_output/tpch-data-sf10",
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
      .appName("Gluten-TPCH-Benchmark")

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
        // .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "3")
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
      // createClickHouseTable(spark, parquetFilesPath, fileFormat)
      // createLocationClickHouseTable(spark)
      // createTables(spark, parquetFilesPath, fileFormat)
      createClickHouseTables(
        spark,
        "/data1/gazelle-jni-warehouse/tpch100_ch_data",
        "default",
        false,
        "ch_",
        "100")
    }
    val refreshTable = false
    if (refreshTable) {
      refreshClickHouseTable(spark)
    }
    // scalastyle:off println
    Thread.sleep(1000)
    println("start to query ... ")

    // createClickHouseTablesAsSelect(spark)
    // createClickHouseTablesAndInsert(spark)

    // selectClickHouseTable(spark, executedCnt, sqlStr.mkString, "ch_clickhouse")
    // selectLocationClickHouseTable(spark, executedCnt, sqlStr.mkString)
    // testSerializeFromObjectExec(spark)

    // selectQ1ClickHouseTable(spark, executedCnt, sqlStr.mkString, "ch_clickhouse")
    // selectStarClickHouseTable(spark)
    // selectQ1LocationClickHouseTable(spark, executedCnt, sqlStr.mkString)

    // testSparkTPCH(spark)
    // testSQL(spark, parquetFilesPath, fileFormat, executedCnt, sqlStr.mkString)

    // createTempView(spark, "/data1/test_output/tpch-data-sf10", "parquet")
    // createGlobalTempView(spark)
    // testJoinIssue(spark)
    testTPCHOne(spark, executedCnt)
    // testSepScanRDD(spark, executedCnt)
    // testTPCHAll(spark)
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

  def collectAllJoinSide(executedPlan: SparkPlan): Unit = {
    val buildSides = collect(executedPlan) {
      case s: ShuffledHashJoinExecTransformer => "Shuffle-" + s.joinBuildSide.toString
      case b: BroadcastHashJoinExecTransformer => "Broadcast-" + b.joinBuildSide.toString
      case os: ShuffledHashJoinExec => "Shuffle-" + os.buildSide.toString
      case ob: BroadcastHashJoinExec => "Broadcast-" + ob.buildSide.toString
      case sm: SortMergeJoinExec => "SortMerge-Join"
    }
    println(buildSides.mkString(" -- "))
  }

  def testTPCHOne(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
         |use default;
         |""".stripMargin).show(1000, false)
    try {
      val tookTimeArr = ArrayBuffer[Long]()
      for (i <- 1 to executedCnt) {
        val startTime = System.nanoTime()
        val df = spark.sql(
          s"""
             |SELECT
             |    supp_nation,
             |    cust_nation,
             |    l_year,
             |    sum(volume) AS revenue
             |FROM (
             |    SELECT
             |        n1.n_name AS supp_nation,
             |        n2.n_name AS cust_nation,
             |        extract(year FROM l_shipdate) AS l_year,
             |        l_extendedprice * (1 - l_discount) AS volume
             |    FROM
             |        supplier100,
             |        lineitem100,
             |        orders100,
             |        customer100,
             |        nation100 n1,
             |        nation100 n2
             |    WHERE
             |        s_suppkey = l_suppkey
             |        AND o_orderkey = l_orderkey
             |        AND c_custkey = o_custkey
             |        AND s_nationkey = n1.n_nationkey
             |        AND c_nationkey = n2.n_nationkey
             |        AND ((n1.n_name = 'FRANCE'
             |                AND n2.n_name = 'GERMANY')
             |            OR (n1.n_name = 'GERMANY'
             |                AND n2.n_name = 'FRANCE'))
             |        AND l_shipdate BETWEEN date'1995-01-01' AND date'1996-12-31') AS shipping
             |GROUP BY
             |    supp_nation,
             |    cust_nation,
             |    l_year
             |ORDER BY
             |    supp_nation,
             |    cust_nation,
             |    l_year;
             |
             |""".stripMargin) // .show(30, false)
        // df.queryExecution.debug.codegen
        // df.explain(false)
        val result = df.collect() // .show(100, false)  //.collect()
        df.explain(false)
        val plan = df.queryExecution.executedPlan
        collectAllJoinSide(plan)
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
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def testSepScanRDD(spark: SparkSession, executedCnt: Int): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(s"""
           |SELECT /*+ SHUFFLE_HASH(ch_lineitem100) */
           |    100.00 * sum(
           |        CASE WHEN p_type LIKE 'PROMO%' THEN
           |            l_extendedprice * (1 - l_discount)
           |        ELSE
           |            0
           |        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
           |FROM
           |    ch_lineitem100,
           |    ch_part100
           |WHERE
           |    l_partkey = p_partkey
           |    AND l_shipdate >= date'1995-09-01'
           |    AND l_shipdate < date'1995-09-01' + interval 1 month;
           |
           |""".stripMargin) // .show(30, false)
      df.explain(false)
      val plan = df.queryExecution.executedPlan
      // df.queryExecution.debug.codegen
      val result = df.collect() // .show(100, false)  //.collect()
      println(result.size)
      result.foreach(r => println(r.mkString(",")))
      // .show(30, false)
      // .explain(false)
      // .collect()
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

  def testTPCHAll(spark: SparkSession): Unit = {
    spark.sql(s"""
         |use default;
         |""".stripMargin).show(1000, false)
    val tookTimeArr = ArrayBuffer[Long]()
    val executedCnt = 1
    val executeExplain = false
    val sqlFilePath = "/data2/tpch-queries-spark100-nohint/"
    for (i <- 1 to 22) {
      if (i != 21) {
        val sqlNum = "q" + "%02d".format(i)
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
          collectAllJoinSide(df.queryExecution.executedPlan)
          println(result.size)
          result.foreach(r => println(r.mkString(",")))
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

  def testJoinIssue(spark: SparkSession): Unit = {
    spark.sql(s"""
         |select
         |  l_returnflag, l_extendedprice, p_name
         |from
         |  ch_lineitem,
         |  ch_part
         |where
         |  l_partkey = p_partkey
         |""".stripMargin).show(10, false) // .show(10, false) .explain("extended")
    /* spark.sql(
      s"""
         |select
         |  l_returnflag, sum(l_extendedprice * l_discount)
         |from
         |  ch_lineitem01,
         |  ch_part01
         |where
         |  l_partkey = p_partkey
         |group by l_returnflag
         |""".stripMargin).show(10, false)       // .show(10, false) .explain("extended")
    spark.sql(
      s"""
         |select
         |  l_orderkey,
         |  l_tax,
         |  l_returnflag
         |from
         |  ch_lineitem01,
         |  ch_customer01,
         |  ch_orders01
         |where
         |  c_custkey = o_custkey
         |  and l_orderkey = o_orderkey
         |""".stripMargin).show(10, false) // .show(10, false) .explain("extended") */
    /* spark.sql(
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount)
         |FROM
         |    ch_orders01,
         |    ch_lineitem01
         |WHERE
         |    l_orderkey = o_orderkey
         |""".stripMargin).explain("extended") // .show(10, false) .explain("extended") */

    /* spark.sql(
      s"""
         |-- Q4
         |SELECT
         |    o_orderpriority,
         |    count(*) AS order_count
         |FROM
         |    ch_orders
         |WHERE
         |    o_orderdate >= date'1993-07-01'
         |    AND o_orderdate < date'1993-07-01' + interval 3 month
         |    AND EXISTS (
         |        SELECT
         |            *
         |        FROM
         |            ch_lineitem
         |        WHERE
         |            l_orderkey = o_orderkey
         |            AND l_commitdate < l_receiptdate)
         |GROUP BY
         |    o_orderpriority
         |ORDER BY
         |    o_orderpriority;
         |
         |""".stripMargin).collect() //.show(30, false)
    spark.sql(
      s"""
         |-- Q3
         |SELECT
         |    l_orderkey,
         |    sum(l_extendedprice * (1 - l_discount)) AS revenue,
         |    o_orderdate,
         |    o_shippriority
         |FROM
         |    ch_customer,
         |    ch_orders,
         |    ch_lineitem
         |WHERE
         |    c_mktsegment = 'BUILDING'
         |    AND c_custkey = o_custkey
         |    AND l_orderkey = o_orderkey
         |    AND o_orderdate < date'1995-03-15'
         |    AND l_shipdate > date'1995-03-15'
         |GROUP BY
         |    l_orderkey,
         |    o_orderdate,
         |    o_shippriority
         |ORDER BY
         |    revenue DESC,
         |    o_orderdate
         |LIMIT 10;
         |""".stripMargin).collect() // .show(100, false) */
    /* spark.sql(
      s"""
         |SELECT
         |    p_brand,
         |    p_type,
         |    p_size,
         |    count(DISTINCT ps_suppkey) AS supplier_cnt
         |FROM
         |    ch_partsupp,
         |    ch_part
         |WHERE
         |    p_partkey = ps_partkey
         |    AND p_brand <> 'Brand#45'
         |    AND p_type NOT LIKE 'MEDIUM POLISHED%'
         |    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
         |    AND ps_suppkey IN (
         |        SELECT
         |            s_suppkey
         |        FROM
         |            ch_supplier
         |        WHERE
         |            s_comment = '%Customer%Complaints%')
         |GROUP BY
         |    p_brand,
         |    p_type,
         |    p_size
         |
         |""".stripMargin).explain(true) // .show(30, false) // .explain(true) */
  }

  def benchmarkTPCH(spark: SparkSession, executedCnt: Int): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(s"""
           |SELECT
           |    l_orderkey,
           |    sum(l_extendedprice * (1 - l_discount)) AS revenue,
           |    o_orderdate,
           |    o_shippriority
           |FROM
           |    ch_customer,
           |    ch_orders,
           |    ch_lineitem
           |WHERE
           |    c_mktsegment = 'BUILDING'
           |    AND c_custkey = o_custkey
           |    AND l_orderkey = o_orderkey
           |    AND o_orderdate < date'1995-03-15'
           |    AND l_shipdate > date'1995-03-15'
           |GROUP BY
           |    l_orderkey,
           |    o_orderdate,
           |    o_shippriority
           |ORDER BY
           |    revenue DESC,
           |    o_orderdate
           |LIMIT 10;
           |
           |
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

  def testSerializeFromObjectExec(spark: SparkSession): Unit = {
    // spark.conf.set("spark.gluten.sql.enable.native.engine", "false")
    val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }

  def createClickHouseTable(
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
         | LOCATION '/data1/gazelle-jni-warehouse/ch_clickhouse'
         |""".stripMargin)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)
    spark.sql(s"""
         | desc formatted ch_clickhouse
         |""".stripMargin).show(100, false)
  }

  def createClickHouseTablesAsSelect(spark: SparkSession): Unit = {
    val targetTable = "table_as_select"
    spark.sql(s"""
         | USE default
         |""".stripMargin).show(10, false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")

    spark.sql(s"""
         | CREATE TABLE IF NOT EXISTS $targetTable USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | AS SELECT
         | l_orderkey,
         | l_partkey,
         | l_suppkey,
         | l_linenumber,
         | l_quantity,
         | l_extendedprice,
         | l_discount,
         | l_tax,
         | l_returnflag,
         | l_linestatus,
         | l_shipdate,
         | l_commitdate,
         | l_receiptdate,
         | l_shipinstruct,
         | l_shipmode,
         | l_comment
         | FROM lineitem;
         |""".stripMargin)

    spark.sql("""
        | show tables
        |""".stripMargin).show(100, false)

    spark.sql(s"""
         | desc formatted ${targetTable}
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | select * from ${targetTable}
         |""".stripMargin).show(10, false)
  }

  def createClickHouseTablesAndInsert(spark: SparkSession): Unit = {
    val targetTable = "table_insert"
    spark.sql(s"""
         | USE default
         |""".stripMargin).show(10, false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")

    spark.sql(s"""
         | CREATE TABLE IF NOT EXISTS $targetTable (
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
         | desc formatted ${targetTable}
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | INSERT INTO ${targetTable}
         | SELECT
         | l_orderkey,
         | l_partkey,
         | l_suppkey,
         | l_linenumber,
         | l_quantity,
         | l_extendedprice,
         | l_discount,
         | l_tax,
         | l_returnflag,
         | l_linestatus,
         | l_shipdate,
         | l_commitdate,
         | l_receiptdate,
         | l_shipinstruct,
         | l_shipmode,
         | l_comment
         | FROM lineitem;
         |""".stripMargin).show()

    spark.sql(s"""
         | select * from ${targetTable}
         |""".stripMargin).show(10, false)

  }

  def selectClickHouseTable(
      spark: SparkSession,
      executedCnt: Int,
      sql: String,
      targetTable: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    sum(l_extendedprice * l_discount) AS revenue
           |FROM
           |    ch_lineitem
           |WHERE
           |    l_shipdate >= date'1994-01-01'
           |    AND l_shipdate < date'1994-01-01' + interval 1 year
           |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
           |    AND l_quantity < 24;
           |""".stripMargin).show(200, false) // .explain("extended")
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

  def selectLocationClickHouseTable(spark: SparkSession, executedCnt: Int, sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    sum(l_extendedprice * l_discount) AS revenue
           |FROM
           |    ch_clickhouse
           |WHERE
           |    l_shipdate >= date'1994-01-01'
           |    AND l_shipdate < date'1994-01-01' + interval 1 year
           |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
           |    AND l_quantity < 24;
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

  def selectQ1ClickHouseTable(
      spark: SparkSession,
      executedCnt: Int,
      sql: String,
      targetTable: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    l_returnflag,
           |    l_linestatus,
           |    sum(l_quantity) AS sum_qty,
           |    sum(l_extendedprice) AS sum_base_price,
           |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
           |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
           |    avg(l_quantity) AS avg_qty,
           |    avg(l_extendedprice) AS avg_price,
           |    avg(l_discount) AS avg_disc,
           |    count(*) AS count_order
           |FROM
           |    ch_lineitem
           |WHERE
           |    l_shipdate <= date'1998-09-02' - interval 1 day
           |GROUP BY
           |    l_returnflag,
           |    l_linestatus;
           |-- ORDER BY
           |--     l_returnflag,
           |--     l_linestatus;
           |""".stripMargin).show(200, false) // .explain("extended")
      // can not use .collect(), will lead to error.
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

  def selectStarClickHouseTable(spark: SparkSession): Unit = {
    spark.sql("""
        | SELECT
        |    l_returnflag,
        |    l_linestatus,
        |    l_quantity,
        |    l_extendedprice,
        |    l_discount,
        |    l_tax
        | FROM lineitem
        |""".stripMargin).show(20, false) // .explain("extended")
    spark.sql("""
        | SELECT * FROM lineitem
        |""".stripMargin).show(20, false) // .explain("extended")
    // can not use .collect(), will lead to error.
  }

  def selectQ1LocationClickHouseTable(
      spark: SparkSession,
      executedCnt: Int,
      sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(s"""
           |SELECT
           |    l_returnflag,
           |    l_linestatus,
           |    sum(l_quantity) AS sum_qty,
           |    sum(l_extendedprice) AS sum_base_price,
           |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
           |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
           |    avg(l_quantity) AS avg_qty,
           |    avg(l_extendedprice) AS avg_price,
           |    avg(l_discount) AS avg_disc,
           |    count(*) AS count_order
           |FROM
           |    ch_clickhouse
           |WHERE
           |    l_shipdate <= date'1998-09-02' - interval 1 day
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

  def testSQL(
      spark: SparkSession,
      parquetFilesPath: String,
      fileFormat: String,
      executedCnt: Int,
      sql: String): Unit = {
    /* spark.sql(
      s"""
         | show tables
         |""".stripMargin).show(100, false) */

    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(sql).show(200, false)
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

  def testSparkTPCH(spark: SparkSession): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    val rootPath = this.getClass.getResource("/").getPath
    val resourcePath = rootPath + "../../../../jvm/src/test/resources/"
    val queryPath = resourcePath + "/queries/"
    for (i <- 1 to 22) {
      val startTime = System.nanoTime()
      val sqlFile = queryPath + "q" + "%02d".format(i) + ".sql"
      println(sqlFile)
      val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8")
      // spark.sql(sqlStr.mkString).collect()
      val sql = sqlStr.mkString
      spark.sql(sql).explain(false)
      spark.sql(sql).show(10, false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }
    println(tookTimeArr.mkString(","))
  }

  def createTables(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    /* val dataSourceMap = Map(
      "customer" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),

      "lineitem" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),

      "nation" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),

      "region" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),

      "orders" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),

      "part" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),

      "partsupp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),

      "supplier" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier")) */

    val parquetFilePath = "/data1/test_output/tpch-data-sf100"
    val customerData = parquetFilePath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS customer100 (
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

    val lineitemData = parquetFilePath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem100 (
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

    val nationData = parquetFilePath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS nation100 (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | STORED AS PARQUET LOCATION '${nationData}'
         |""".stripMargin).show(1, false)

    val regionData = parquetFilePath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS region100 (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | STORED AS PARQUET LOCATION '${regionData}'
         |""".stripMargin).show(1, false)

    val ordersData = parquetFilePath + "/order"
    spark.sql(s"DROP TABLE IF EXISTS orders100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS orders100 (
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

    val partData = parquetFilePath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS part100 (
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

    val partsuppData = parquetFilePath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp100 (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | STORED AS PARQUET LOCATION '${partsuppData}'
         |""".stripMargin).show(1, false)

    val supplierData = parquetFilePath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier100")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS supplier100 (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | STORED AS PARQUET LOCATION '${supplierData}'
         |""".stripMargin).show(1, false)

    spark.sql(s"""
         | show databases;
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         | show tables;
         |""".stripMargin).show(100, false)
    /* dataSourceMap.foreach {
      case (key, value) =>
        println(s"----------------create table $key")
        spark.sql(
          s"""
             | desc $key;
             |""".stripMargin).show(100, false)
        spark.sql(
          s"""
             | select count(1) from $key;
             |""".stripMargin).show(10, false)

    } */
  }

  def createClickHouseTables(
      spark: SparkSession,
      dataFilesPath: String,
      dbName: String,
      notNull: Boolean = false,
      tablePrefix: String = "ch_",
      tableSuffix: String = "100"): Unit = {
    spark.sql(s"""
         |CREATE DATABASE IF NOT EXISTS ${dbName}
         |WITH DBPROPERTIES (engine='MergeTree');
         |""".stripMargin)

    spark.sql(s"use ${dbName};")

    val notNullStr = if (notNull) {
      " not null"
    } else {
      " "
    }

    val customerData = dataFilesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}customer${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}customer${tableSuffix} (
         | c_custkey    bigint ${notNullStr},
         | c_name       string ${notNullStr},
         | c_address    string ${notNullStr},
         | c_nationkey  bigint ${notNullStr},
         | c_phone      string ${notNullStr},
         | c_acctbal    double ${notNullStr},
         | c_mktsegment string ${notNullStr},
         | c_comment    string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${customerData}'
         |""".stripMargin).show(1, false)

    val lineitemData = dataFilesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}lineitem${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}lineitem${tableSuffix} (
         | l_orderkey      bigint ${notNullStr},
         | l_partkey       bigint ${notNullStr},
         | l_suppkey       bigint ${notNullStr},
         | l_linenumber    bigint ${notNullStr},
         | l_quantity      double ${notNullStr},
         | l_extendedprice double ${notNullStr},
         | l_discount      double ${notNullStr},
         | l_tax           double ${notNullStr},
         | l_returnflag    string ${notNullStr},
         | l_linestatus    string ${notNullStr},
         | l_shipdate      date ${notNullStr},
         | l_commitdate    date ${notNullStr},
         | l_receiptdate   date ${notNullStr},
         | l_shipinstruct  string ${notNullStr},
         | l_shipmode      string ${notNullStr},
         | l_comment       string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${lineitemData}'
         |""".stripMargin).show(1, false)

    /* val lineitemLCData = dataFilesPath + "/lineitem_lowcardinality"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}lineitem${tableSuffix}_lc")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}lineitem${tableSuffix}_lc (
         | l_orderkey      bigint ${notNullStr},
         | l_partkey       bigint ${notNullStr},
         | l_suppkey       bigint ${notNullStr},
         | l_linenumber    bigint ${notNullStr},
         | l_quantity      double ${notNullStr},
         | l_extendedprice double ${notNullStr},
         | l_discount      double ${notNullStr},
         | l_tax           double ${notNullStr},
         | l_returnflag    string ${notNullStr},
         | l_linestatus    string ${notNullStr},
         | l_shipdate      date ${notNullStr},
         | l_commitdate    date ${notNullStr},
         | l_receiptdate   date ${notNullStr},
         | l_shipinstruct  string ${notNullStr},
         | l_shipmode      string ${notNullStr},
         | l_comment       string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${lineitemLCData}'
         |""".stripMargin).show(1, false) */

    val nationData = dataFilesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}nation${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}nation${tableSuffix} (
         | n_nationkey bigint ${notNullStr},
         | n_name      string ${notNullStr},
         | n_regionkey bigint ${notNullStr},
         | n_comment   string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${nationData}'
         |""".stripMargin).show(1, false)

    val regionData = dataFilesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}region${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}region${tableSuffix} (
         | r_regionkey bigint ${notNullStr},
         | r_name      string ${notNullStr},
         | r_comment   string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${regionData}'
         |""".stripMargin).show(1, false)

    val ordersData = dataFilesPath + "/order"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}orders${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}orders${tableSuffix} (
         | o_orderkey      bigint ${notNullStr},
         | o_custkey       bigint ${notNullStr},
         | o_orderstatus   string ${notNullStr},
         | o_totalprice    double ${notNullStr},
         | o_orderdate     date ${notNullStr},
         | o_orderpriority string ${notNullStr},
         | o_clerk         string ${notNullStr},
         | o_shippriority  bigint ${notNullStr},
         | o_comment       string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${ordersData}'
         |""".stripMargin).show(1, false)

    val partData = dataFilesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}part${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}part${tableSuffix} (
         | p_partkey     bigint ${notNullStr},
         | p_name        string ${notNullStr},
         | p_mfgr        string ${notNullStr},
         | p_brand       string ${notNullStr},
         | p_type        string ${notNullStr},
         | p_size        bigint ${notNullStr},
         | p_container   string ${notNullStr},
         | p_retailprice double ${notNullStr},
         | p_comment     string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${partData}'
         |""".stripMargin).show(1, false)

    val partsuppData = dataFilesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}partsupp${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}partsupp${tableSuffix} (
         | ps_partkey    bigint ${notNullStr},
         | ps_suppkey    bigint ${notNullStr},
         | ps_availqty   bigint ${notNullStr},
         | ps_supplycost double ${notNullStr},
         | ps_comment    string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${partsuppData}'
         |""".stripMargin).show(1, false)

    val supplierData = dataFilesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}supplier${tableSuffix}")
    spark.sql(s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}supplier${tableSuffix} (
         | s_suppkey   bigint ${notNullStr},
         | s_name      string ${notNullStr},
         | s_address   string ${notNullStr},
         | s_nationkey bigint ${notNullStr},
         | s_phone     string ${notNullStr},
         | s_acctbal   double ${notNullStr},
         | s_comment   string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${supplierData}'
         |""".stripMargin).show(1, false)

    spark.sql(s"""
         | show tables;
         |""".stripMargin).show(100, false)
  }

  def createTempView(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    val dataSourceMap = Map(
      "customer_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),
      "lineitem_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),
      "nation_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),
      "region_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),
      "orders_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),
      "part_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),
      "partsupp_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),
      "supplier_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier"))

    dataSourceMap.foreach {
      case (key, value) => value.createOrReplaceTempView(key)
    }
  }

  def createGlobalTempView(spark: SparkSession): Unit = {
    spark.sql(s"""DROP VIEW IF EXISTS global_temp.view_lineitem;""")
    spark.sql(s"""
         |CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_lineitem
         |AS SELECT * FROM lineitem;
         |
         |""".stripMargin)
    spark.sql(s"""DROP VIEW IF EXISTS global_temp.view_orders;""")
    spark.sql(s"""
         |CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_orders
         |AS SELECT * FROM orders;
         |""".stripMargin)
    spark.sql(s"""
         |SHOW VIEWS IN global_temp;
         |""".stripMargin).show(100, false)
    spark.sql(s"""
         |SHOW VIEWS;
         |""".stripMargin).show(100, false)
  }
}
// scalastyle:on
