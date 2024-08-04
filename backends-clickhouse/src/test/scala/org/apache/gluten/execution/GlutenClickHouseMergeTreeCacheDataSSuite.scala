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

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.io.File

import scala.concurrent.duration.DurationInt

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeCacheDataSSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "error")
      .set("spark.gluten.soft-affinity.enabled", "true")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.mergetree.merge_after_insert",
        "false")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new Configuration
    conf.set("fs.defaultFS", HDFS_URL)
    val fs = FileSystem.get(conf)
    fs.delete(new org.apache.hadoop.fs.Path(HDFS_URL), true)
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
    FileUtils.deleteDirectory(new File(HDFS_CACHE_PATH))
    FileUtils.forceMkdir(new File(HDFS_CACHE_PATH))
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory) {
      val files = directory.listFiles
      val count = files
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count
    } else {
      0
    }
  }

  test("test cache mergetree data sync") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
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
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$HDFS_URL/test/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
    val dataPath = new File(HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data
              |  select l_orderkey, l_partkey from lineitem_mergetree_hdfs
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(HDFS_METADATA_PATH + s"$sparkVersion/test/lineitem_mergetree_hdfs")
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    assertResult(22)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)

    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(31)(metaPath.list().length)
    assert(countFiles(dataPath) > first_cache_files)

    val sqlStr =
      s"""
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data async") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
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
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$HDFS_URL/test/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
    val dataPath = new File(HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data async
              |  select * from lineitem_mergetree_hdfs
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(HDFS_METADATA_PATH + s"$sparkVersion/test/lineitem_mergetree_hdfs")
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    eventually(timeout(60.seconds), interval(2.seconds)) {
      assertResult(22)(metaPath.list().length)
      assert(countFiles(dataPath) > initial_cache_files)
    }

    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data async select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    eventually(timeout(60.seconds), interval(2.seconds)) {
      assertResult(31)(metaPath.list().length)
      assert(countFiles(dataPath) > first_cache_files)
    }

    val sqlStr =
      s"""
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }

  test("test cache mergetree data with the path") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_hdfs;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_hdfs
                 |(
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
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$HDFS_URL/test/lineitem_mergetree_hdfs'
                 |TBLPROPERTIES (storage_policy='__hdfs_main',
                 |               orderByKey='l_linenumber,l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_hdfs
                 | select * from lineitem a
                 | where a.l_shipdate between date'1995-01-01' and date'1995-01-31'
                 |""".stripMargin)
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
    val dataPath = new File(HDFS_CACHE_PATH)
    val initial_cache_files = countFiles(dataPath)

    val res = spark
      .sql(s"""
              |cache data
              |  select * from '$HDFS_URL/test/lineitem_mergetree_hdfs'
              |  after l_shipdate AS OF '1995-01-10'
              |  CACHEPROPERTIES(storage_policy='__hdfs_main',
              |                aaa='ccc')""".stripMargin)
      .collect()
    assertResult(true)(res(0).getBoolean(0))
    val metaPath = new File(HDFS_METADATA_PATH + s"$sparkVersion/test/lineitem_mergetree_hdfs")
    assertResult(true)(metaPath.exists() && metaPath.isDirectory)
    assertResult(22)(metaPath.list().length)
    assert(countFiles(dataPath) > initial_cache_files)
    val first_cache_files = countFiles(dataPath)
    val res1 = spark.sql(s"cache data select * from lineitem_mergetree_hdfs").collect()
    assertResult(true)(res1(0).getBoolean(0))
    assertResult(31)(metaPath.list().length)
    assert(countFiles(dataPath) > first_cache_files)

    val sqlStr =
      s"""
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
         |    lineitem_mergetree_hdfs
         |WHERE
         |    l_shipdate >= date'1995-01-10'
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(7898)(addFiles.map(_.rows).sum)
      })
    spark.sql("drop table lineitem_mergetree_hdfs purge")
  }
}
// scalastyle:off line.size.limit
