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

package org.apache.gluten.execution.iceberg

import com.google.common.base.Strings
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

class ClickHouseIcebergHiveTableSupport {

  private val sparkConf: SparkConf = new SparkConf()

  private var _hiveSpark: SparkSession = _

  def spark: SparkSession = _hiveSpark

  def initSparkConf(url: String, catalog: String, path: String): SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.parquet.maxmin.index", "true")
      .set("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.gluten.supported.hive.udfs", "my_add")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .setCHConfig("use_local_format", true)
      .set("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hive")
      .setMaster("local[*]")
    if (!Strings.isNullOrEmpty(url)) {
      sparkConf.set("spark.hadoop.hive.metastore.uris", url)
    }
    if (!Strings.isNullOrEmpty(catalog)) {
      sparkConf.set("spark.sql.catalog." + catalog, "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog." + catalog + ".type", "hive")
    }
    if (!Strings.isNullOrEmpty(path)) {
      sparkConf.set("spark.sql.warehouse.dir", path)
    }
    sparkConf
  }

  def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      _hiveSpark =
        SparkSession
          .builder()
          .config(sparkConf)
          .enableHiveSupport()
          .getOrCreate()
    }
  }

  def clean(): Unit = {
    try {
      if (_hiveSpark != null) {
        _hiveSpark.stop()
        _hiveSpark = null
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
