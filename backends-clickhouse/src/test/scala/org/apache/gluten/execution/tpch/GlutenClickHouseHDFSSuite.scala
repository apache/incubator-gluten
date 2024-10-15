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
package org.apache.gluten.execution.tpch

import org.apache.gluten.backendsapi.clickhouse.CHConf._
import org.apache.gluten.execution.{CHNativeCacheManager, FileSourceScanExecTransformer, GlutenClickHouseTPCHAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FileStatusCache

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset

class GlutenClickHouseHDFSSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tablesPath: String = HDFS_URL_ENDPOINT + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  private val hdfsCachePath = "/tmp/gluten_hdfs_cache/"
  private val cache_name = "gluten_cache"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .setCHConfig("use_local_format", true)
      .set(prefixOf("shuffle.hash.algorithm"), "sparkMurmurHash3_32")
      .setCHConfig("gluten_cache.local.enabled", "true")
      .setCHConfig("gluten_cache.local.name", cache_name)
      .setCHConfig("gluten_cache.local.path", hdfsCachePath)
      .setCHConfig("gluten_cache.local.max_size", "10Gi")
      .setCHConfig("reuse_disk_cache", "false")
      .set("spark.sql.adaptive.enabled", "false")

    // TODO: spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm =>
    //     CHConf.prefixOf("shuffle.hash.algorithm")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    deleteCache()
  }

  override protected def afterAll(): Unit = {
    deleteCache()
    super.afterEach()
  }

  private def deleteCache(): Unit = {
    val targetFile = new Path(tablesPath)
    val fs = targetFile.getFileSystem(spark.sessionState.newHadoopConf())
    fs.listStatus(targetFile)
      .foreach(
        table => {
          if (table.isDirectory) {
            fs.listStatus(table.getPath)
              .foreach(
                data => {
                  if (data.isFile) {
                    CHNativeCacheManager
                      .removeFiles(data.getPath.toUri.getPath.substring(1), cache_name)
                  }
                })
          }
        })
  }

  val runWithoutCache: () => Unit = () => {
    runTPCHQuery(6) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: FileSourceScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans.head.metrics("readMissBytes").value != 0)
    }
  }

  val runWithCache: () => Unit = () => {
    runTPCHQuery(6) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: FileSourceScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans.head.metrics("readMissBytes").value == 0)
        assert(plans.head.metrics("readCacheBytes").value != 0)
    }
  }

  test("test hdfs cache") {
    runWithoutCache()
    runWithCache()
  }

  test("test cache file command") {
    runSql(
      s"CACHE FILES select * from '$HDFS_URL_ENDPOINT/tpch-data/lineitem'",
      noFallBack = false) { _ => }
    runWithCache()
  }

  test("test no cache by query") {
    withSQLConf(
      runtimeSettings("read_from_filesystem_cache_if_exists_otherwise_bypass_cache") -> "true") {
      runWithoutCache()
    }

    runWithoutCache()
    runWithCache()
  }

  test("GLUTEN-7542: Fix cache refresh") {
    val file_path = s"$tablesPath/issue_7542/"
    val targetDirs = new Path(file_path)
    val fs = targetDirs.getFileSystem(spark.sessionState.newHadoopConf())
    fs.mkdirs(targetDirs)
    val out = fs.create(new Path(s"$file_path/00000_0"))
    IOUtils.write("1\n2", out, Charset.defaultCharset())
    out.close()
    sql(s"""
           |CREATE external TABLE `issue_7542`(
           |  `c_custkey` int )
           |using CSV
           |LOCATION
           |  '$file_path/'
           |""".stripMargin)

    sql(s"""select * from issue_7542""").collect()
    fs.delete(new Path(s"$file_path/00000_0"), false)
    val out2 = fs.create(new Path(s"$file_path/00000_0"))
    IOUtils.write("2\n3\n4", out2, Charset.defaultCharset())
    out2.close()
    FileStatusCache.getOrCreate(spark)

    val result = sql(s"""select count(*) from issue_7542""").collect()
    assert(result.length == 1)
    assert(result.head.getLong(0) == 3)

    sql("drop table issue_7542")
  }
}
