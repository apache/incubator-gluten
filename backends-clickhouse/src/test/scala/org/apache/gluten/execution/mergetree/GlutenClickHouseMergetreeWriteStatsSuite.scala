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

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeConfig}
import org.apache.gluten.backendsapi.clickhouse.CHConfig.GlutenCHConf
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseTPCDSAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.io.File

class GlutenClickHouseMergetreeWriteStatsSuite
  extends GlutenClickHouseTPCDSAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.driver.memory", "2G")
      .set("spark.memory.offHeap.size", "4G")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.databricks.delta.stats.enabled", "true")
      .set("spark.databricks.delta.optimizeWrite.enabled", "true")
      .set(RuntimeConfig.LOGGER_LEVEL.key, "error")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .setCHSettings("mergetree.merge_after_insert", false)
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new Configuration
    conf.set("fs.defaultFS", HDFS_URL)
    val fs = FileSystem.get(conf)
    fs.delete(new org.apache.hadoop.fs.Path(HDFS_URL), true)
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
  }

  test("test mergetree virtual columns") {
    spark.sql("create database if not exists mergetree")
    spark.sql("use mergetree")
    spark.sql("drop table if exists store_sales")
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS mergetree.store_sales
                 |(
                 |     ss_sold_time_sk INT,
                 |     ss_item_sk INT,
                 |     ss_customer_sk INT,
                 |     ss_cdemo_sk INT,
                 |     ss_hdemo_sk INT,
                 |     ss_addr_sk INT,
                 |     ss_store_sk INT,
                 |     ss_promo_sk INT,
                 |     ss_ticket_number bigint,
                 |     ss_quantity INT,
                 |     ss_wholesale_cost DECIMAL(7,2),
                 |     ss_list_price DECIMAL(7,2),
                 |     ss_sales_price DECIMAL(7,2),
                 |     ss_ext_discount_amt DECIMAL(7,2),
                 |     ss_ext_sales_price DECIMAL(7,2),
                 |     ss_ext_wholesale_cost DECIMAL(7,2),
                 |     ss_ext_list_price DECIMAL(7,2),
                 |     ss_ext_tax DECIMAL(7,2),
                 |     ss_coupon_amt DECIMAL(7,2),
                 |     ss_net_paid DECIMAL(7,2),
                 |     ss_net_paid_inc_tax DECIMAL(7,2),
                 |     ss_net_profit DECIMAL(7,2),
                 |     ss_sold_date_sk INT
                 |)
                 |USING clickhouse
                 |LOCATION '$HDFS_URL/test/store_sales'
                 |TBLPROPERTIES (storage_policy='__hdfs_main')
                 |""".stripMargin)

    // scalastyle:off line.size.limit
    spark.sql(
      "insert into mergetree.store_sales select /*+ REPARTITION(3) */ * from tpcdsdb.store_sales")
    val df = spark.sql(
      "select input_file_name(), count(*) from mergetree.store_sales group by input_file_name()")
    val inputFiles = df.collect().map(_.getString(0))
    val snapshot = getDeltaSnapshot(df)
    val deltaLogFiles = snapshot.allFiles
      .collect()
      .map(addFile => snapshot.path.getParent.toString + "/" + addFile.path)
    assertResult(inputFiles.toSet)(deltaLogFiles.toSet)
    // scalastyle:on line.size.limit
  }

  def getDeltaSnapshot(df: DataFrame): Snapshot = {
    val scanExec = collect(df.queryExecution.sparkPlan) {
      case nf: FileSourceScanExecTransformer => nf
      case f: FileSourceScanExec => f
    }
    assertResult(1)(scanExec.size)
    val mergetreeScan = scanExec.head
    val snapshot: Snapshot = mergetreeScan.relation.location match {
      case pdf: PreparedDeltaFileIndex => pdf.preparedScan.scannedSnapshot
      case tlf: TahoeLogFileIndex => tlf.getSnapshot
    }
    assert(snapshot != null)
    snapshot
  }

}
