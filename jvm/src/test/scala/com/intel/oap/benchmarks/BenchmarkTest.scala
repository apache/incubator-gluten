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

package com.intel.oap.benchmarks

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.intel.oap.GazelleJniConfig

import org.apache.spark.sql.SparkSession

object BenchmarkTest {

  def main(args: Array[String]): Unit = {

    val (parquetFilesPath, fileFormat,
    executedCnt, configed, sqlFilePath, stopFlagFile) = if (args.length > 0) {
      (args(0), args(1), args(2).toInt, true, args(3), args(4))
    } else {
      val rootPath = this.getClass.getResource("/").getPath
      val resourcePath = rootPath + "../../../src/test/resources/"
      val dataPath = resourcePath + "/tpch-data/"
      val queryPath = resourcePath + "/queries/"
      //(new File(dataPath).getAbsolutePath, "parquet", 1, false, queryPath + "q06.sql", "")
      ("/data1/test_output/tpch-data-sf10", "parquet", 100, false, queryPath + "q06.sql", "")
    }

    val sqlStr = Source.fromFile(new File(sqlFilePath), "UTF-8")

    val sessionBuilderTmp = SparkSession
      .builder()
      .appName("Gazelle-Jni-Benchmark")

    val sessionBuilder = if (!configed) {
      sessionBuilderTmp
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
        .config("spark.sql.sources.useV1SourceList", "avro")
        .config("spark.memory.fraction", "0.3")
        .config("spark.memory.storageFraction", "0.3")
        //.config("spark.sql.parquet.columnarReaderBatchSize", "20000")
        .config("spark.plugins", "com.intel.oap.GazellePlugin")
        //.config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.oap.sql.columnar.columnartorow", "false")
        .config(GazelleJniConfig.OAP_LOAD_NATIVE, "true")
        .config(GazelleJniConfig.OAP_LOAD_ARROW, "false")
        .config(GazelleJniConfig.OAP_LIB_PATH,
          "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse/cmake-build-release/utils/local-engine/liblocal_engine_jni.so")
        .config("spark.oap.sql.columnar.iterator", "false")
        //.config("spark.sql.planChangeLog.level", "info")
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "6442450944")
    } else {
      sessionBuilderTmp
    }

    val spark = sessionBuilder.getOrCreate()
    if (!configed) {
      spark.sparkContext.setLogLevel("WARN")
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
    createTempView(spark, parquetFilesPath, fileFormat)

    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(sql).show(200, false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute ${i} time, time: ${tookTime}")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    spark.conf.set("org.apache.spark.example.columnar.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, false)
  }

  def createTempView(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    val dataSourceMap = Map(
      "customer" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),

      "lineitem" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),

      "nation" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),

      "region" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),

      "orders" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),

      "part" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),

      "partsupp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),

      "supplier" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier"))

    dataSourceMap.foreach {
      case (key, value) => value.createOrReplaceTempView(key)
    }
  }
}
