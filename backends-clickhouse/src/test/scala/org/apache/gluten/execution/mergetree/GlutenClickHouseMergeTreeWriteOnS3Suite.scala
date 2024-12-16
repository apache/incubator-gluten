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
package org.apache.gluten.execution.mergetree

import org.apache.gluten.backendsapi.clickhouse.CHConf._
import org.apache.gluten.execution.{BasicScanExecTransformer, FileSourceScanExecTransformer, GlutenClickHouseTPCHAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import _root_.org.apache.commons.io.FileUtils
import io.minio._
import io.minio.messages.DeleteObject

import java.io.File
import java.util

import scala.concurrent.duration.DurationInt

class GlutenClickHouseMergeTreeWriteOnS3Suite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  private val client = MinioClient
    .builder()
    .endpoint(MINIO_ENDPOINT)
    .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
    .build()

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
      .setCHConfig("logger.level", "error")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    if (client.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build())) {
      val results =
        client.listObjects(ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build())
      val objects = new util.LinkedList[DeleteObject]()
      results.forEach(
        obj => {
          objects.add(new DeleteObject(obj.get().objectName()))
        })
      val removeResults = client.removeObjects(
        RemoveObjectsArgs.builder().bucket(BUCKET_NAME).objects(objects).build())
      removeResults.forEach(result => result.get().message())
      client.removeBucket(RemoveBucketArgs.builder().bucket(BUCKET_NAME).build())
    }
    client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build())
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
    FileUtils.forceMkdir(new File(S3_METADATA_PATH))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
  }

  test("test mergetree table write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_s3
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
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_s3'
                 |TBLPROPERTIES (storage_policy='__s3_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_s3
                 | select * from lineitem
                 |""".stripMargin)
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))

    runTPCHQueryBySQL(1, q1("lineitem_mergetree_s3")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(1)(addFiles.size)
        assertResult(600572)(addFiles.head.rows)
    }
    eventually(timeout(10.seconds), interval(2.seconds)) {
      verifyS3CompactFileExist("lineitem_mergetree_s3")
    }
    spark.sql("drop table lineitem_mergetree_s3") // clean up
  }

  private def verifyS3CompactFileExist(table: String): Unit = {
    val args = ListObjectsArgs
      .builder()
      .bucket(BUCKET_NAME)
      .recursive(true)
      .prefix(table)
      .build()
    var objectCount: Int = 0
    var metadataGlutenExist: Boolean = false
    var metadataBinExist: Boolean = false
    var dataBinExist: Boolean = false
    var hasCommits = false
    client
      .listObjects(args)
      .forEach(
        obj => {
          objectCount += 1
          val objectName = obj.get().objectName()
          if (objectName.contains("metadata.gluten")) {
            metadataGlutenExist = true
          } else if (objectName.contains("part_meta.gluten")) {
            metadataBinExist = true
          } else if (objectName.contains("part_data.gluten")) {
            dataBinExist = true
          } else if (objectName.contains("_commits")) {
            // Spark 35 has _commits directory
            // table/_delta_log/_commits/
            hasCommits = true
          }
        })

    if (isSparkVersionGE("3.5")) {
      assertResult(6)(objectCount)
      assert(hasCommits)
    } else {
      assertResult(5)(objectCount)
    }

    assert(metadataGlutenExist)
    assert(metadataBinExist)
    assert(dataBinExist)
  }

  test("test mergetree write with orderby keys / primary keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey_s3
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
                 |TBLPROPERTIES (storage_policy='__s3_main',
                 |               orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_orderbykey_s3'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey_s3
                 | select * from lineitem
                 |""".stripMargin)

    runTPCHQueryBySQL(1, q1("lineitem_mergetree_orderbykey_s3")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assertResult("l_shipdate,l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(","))
        assertResult("l_shipdate")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(","))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assertResult(1)(addFiles.size)
        assertResult(600572)(addFiles.head.rows)
    }
    spark.sql("drop table lineitem_mergetree_orderbykey_s3")
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition_s3
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
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (storage_policy='__s3_main',
                 |               orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_partition_s3'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition_s3
                 | select * from lineitem
                 |""".stripMargin)

    // write with dataframe api
    val source = spark.sql(s"""
                              |select
                              | l_orderkey      ,
                              | l_partkey       ,
                              | l_suppkey       ,
                              | l_linenumber    ,
                              | l_quantity      ,
                              | l_extendedprice ,
                              | l_discount      ,
                              | l_tax           ,
                              | l_returnflag    ,
                              | l_linestatus    ,
                              | l_shipdate      ,
                              | l_commitdate    ,
                              | l_receiptdate   ,
                              | l_shipinstruct  ,
                              | l_shipmode      ,
                              | l_comment
                              | from lineitem
                              | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
                              |""".stripMargin)

    source.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .insertInto("lineitem_mergetree_partition_s3")

    // static partition
    spark.sql(s"""
                 | insert into lineitem_mergetree_partition_s3 PARTITION (l_returnflag = 'A')
                 | (l_shipdate,
                 |  l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment)
                 | select
                 |  l_shipdate,
                 |  l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment from lineitem
                 |  where l_returnflag = 'A'
                 |""".stripMargin)

    runTPCHQueryBySQL(1, q1("lineitem_mergetree_partition_s3"), compareResult = false) {
      df =>
        val result = df.collect()
        assertResult(4)(result.length)
        assertResult("A")(result(0).getString(0))
        assertResult("F")(result(0).getString(1))
        assertResult(7578058.0)(result(0).getDouble(2))

        assertResult("N")(result(2).getString(0))
        assertResult("O")(result(2).getString(1))
        assertResult(7454519.0)(result(2).getDouble(2))

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))
        assertResult(6)(mergetreeScan.metrics("numFiles").value)

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(","))
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(","))
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(6)(addFiles.size)
        assertResult(750735)(addFiles.map(_.rows).sum)
    }
    spark.sql("drop table lineitem_mergetree_partition_s3")

  }

  test("test mergetree write with bucket table") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket_s3
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
                 |PARTITIONED BY (l_returnflag)
                 |CLUSTERED BY (l_orderkey)
                 |${if (spark32) "" else "SORTED BY (l_partkey)"} INTO 4 BUCKETS
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_bucket_s3'
                 |TBLPROPERTIES (storage_policy='__s3_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket_s3
                 | select * from lineitem
                 |""".stripMargin)

    runTPCHQueryBySQL(1, q1("lineitem_mergetree_bucket_s3")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isDefined)
        if (spark32) {
          assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        } else {
          assertResult("l_partkey")(
            ClickHouseTableV2
              .getTable(fileIndex.deltaLog)
              .orderByKeyOption
              .get
              .mkString(","))
        }
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(12)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }
    spark.sql("drop table lineitem_mergetree_bucket_s3")
  }

  test("test mergetree write with the path based") {
    val dataPath = s"s3a://$BUCKET_NAME/lineitem_mergetree_bucket_s3"

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .partitionBy("l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .option("clickhouse.numBuckets", "4")
      .option("clickhouse.bucketColumnNames", "l_orderkey")
      .option("clickhouse.storage_policy", "__s3_main")
      .save(dataPath)

    runTPCHQueryBySQL(1, q1(s"clickhouse.`$dataPath`")) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assertResult(1)(scanExec.size)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isDefined)
        assertResult("l_orderkey")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(","))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.nonEmpty)
        assertResult(1)(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size)
        assertResult("l_returnflag")(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns
            .head)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assertResult(12)(addFiles.size)
        assertResult(600572)(addFiles.map(_.rows).sum)
    }

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .count()
    assertResult(600572)(result)
  }

  test("test mergetree insert with optimize basic") {
    val tableName = "lineitem_mergetree_insert_optimize_basic_s3"
    val dataPath = s"s3a://$BUCKET_NAME/$tableName"

    withSQLConf(
      "spark.databricks.delta.optimize.minFileSize" -> "200000000",
      runtimeSettings("mergetree.insert_without_local_storage") -> "true",
      runtimeSettings("mergetree.merge_after_insert") -> "true"
    ) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS $tableName
                   |USING clickhouse
                   |LOCATION '$dataPath'
                   | as select * from lineitem
                   |""".stripMargin)

      val ret = spark.sql(s"select count(*) from $tableName").collect()
      assertResult(600572)(ret.apply(0).get(0))
      assert(
        !new File(s"$CH_DEFAULT_STORAGE_DIR/lineitem_mergetree_insert_optimize_basic").exists())
    }
  }

  test("test mergetree with primary keys pruning by driver") {
    val tableName = "lineitem_mergetree_pk_pruning_by_driver_s3"
    val dataPath = s"s3a://$BUCKET_NAME/$tableName"
    spark.sql(s"""
                 |DROP TABLE IF EXISTS $tableName;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS $tableName
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
                 |TBLPROPERTIES (storage_policy='__s3_main', orderByKey='l_shipdate')
                 |LOCATION '$dataPath'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table $tableName
                 | select * from lineitem
                 |""".stripMargin)

    FileUtils.forceDelete(new File(S3_METADATA_PATH))

    withSQLConf(runtimeSettings("enabled_driver_filter_mergetree_index") -> "true") {
      runTPCHQueryBySQL(6, q6(tableName)) {
        df =>
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          assertResult(1)(scanExec.size)

          val mergetreeScan = scanExec.head
          assert(mergetreeScan.nodeName.startsWith("ScanTransformer mergetree"))

          val plans = collect(df.queryExecution.executedPlan) {
            case scanExec: BasicScanExecTransformer => scanExec
          }
          assertResult(1)(plans.size)
          assertResult(1)(plans.head.getSplitInfos.size)
      }
    }
  }

  test("GLUTEN-6750: Optimize error if file metadata not exist") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket_s3
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
                 |PARTITIONED BY (l_returnflag)
                 |CLUSTERED BY (l_orderkey)
                 |${if (spark32) "" else "SORTED BY (l_partkey)"} INTO 4 BUCKETS
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_bucket_s3'
                 |TBLPROPERTIES (storage_policy='__s3_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket_s3
                 | select /*+ REPARTITION(3) */ * from lineitem
                 |""".stripMargin)

    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
    spark.sql("optimize lineitem_mergetree_bucket_s3")
    spark.sql("drop table lineitem_mergetree_bucket_s3")
  }
}
