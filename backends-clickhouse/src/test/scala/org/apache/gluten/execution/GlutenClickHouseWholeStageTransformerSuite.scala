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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import org.apache.commons.io.FileUtils

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseWholeStageTransformerSuite extends WholeStageTransformerSuite {

  val DBL_EPSILON = 2.2204460492503131e-16
  val DBL_RELAX_EPSILON: Double = Math.pow(10, -11)
  val FLT_EPSILON = 1.19209290e-07f

  protected val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  val S3_METADATA_PATH = s"/tmp/metadata/s3/$sparkVersion/"
  val S3_CACHE_PATH = s"/tmp/s3_cache/$sparkVersion/"
  val S3_ENDPOINT = "s3://127.0.0.1:9000/"
  val MINIO_ENDPOINT: String = S3_ENDPOINT.replace("s3", "http")
  val BUCKET_NAME: String = sparkVersion.replace(".", "-")
  val WHOLE_PATH: String = MINIO_ENDPOINT + BUCKET_NAME + "/"

  val HDFS_METADATA_PATH = s"/tmp/metadata/hdfs/$sparkVersion/"
  val HDFS_CACHE_PATH = s"/tmp/hdfs_cache/$sparkVersion/"
  val HDFS_URL_ENDPOINT = s"hdfs://127.0.0.1:8020"
  val HDFS_URL = s"$HDFS_URL_ENDPOINT/$sparkVersion"

  val S3_ACCESS_KEY = "BypTYzcXOlfr03FFIvt4"
  val S3_SECRET_KEY = "K9MDaGItPSaphorZM8t4hXf30gHF9dBWi6L2dK5E"

  def AlmostEqualsIsRel(expected: Double, actual: Double, EPSILON: Double = DBL_EPSILON): Unit = {
    val diff = Math.abs(expected - actual)
    val epsilon = EPSILON * Math.max(Math.abs(expected), Math.abs(actual))
    if (diff > epsilon) {
      fail(s"""
              |expected: $expected
              |actual:   $actual
              | abs(expected-expected) ~ epsilon = $diff ~ $epsilon
              |""".stripMargin)
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set(
        "spark.gluten.sql.columnar.backend.ch.use.v2",
        ClickHouseConfig.DEFAULT_USE_DATASOURCE_V2)
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.user_defined_path",
        "/tmp/user_defined")
    if (UTSystemParameters.testMergeTreeOnObjectStorage) {
      conf
        .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3.type",
          "s3_gluten")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3.endpoint",
          WHOLE_PATH)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3.access_key_id",
          S3_ACCESS_KEY)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3.secret_access_key",
          S3_SECRET_KEY)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3.metadata_path",
          S3_METADATA_PATH)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3_cache.type",
          "cache")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3_cache.disk",
          "s3")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3_cache.path",
          S3_CACHE_PATH)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.s3_cache.max_size",
          "10Gi")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.policies.__s3_main.volumes",
          "main")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.policies.__s3_main.volumes.main.disk",
          "s3_cache")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs.type",
          "hdfs_gluten")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs.endpoint",
          HDFS_URL_ENDPOINT + "/")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs.metadata_path",
          HDFS_METADATA_PATH)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs_cache.type",
          "cache")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs_cache.disk",
          "hdfs")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs_cache.path",
          HDFS_CACHE_PATH)
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.disks.hdfs_cache.max_size",
          "10Gi")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.policies.__hdfs_main.volumes",
          "main")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.storage_configuration.policies.__hdfs_main.volumes.main.disk",
          "hdfs_cache")
        .set(
          "spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.dfs_client_read_shortcircuit",
          "false")
        .set("spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.dfs_default_replica", "1")
    } else {
      conf
    }
  }

  def clearDataPath(dataPath: String): Unit = {
    val dataPathDir = new File(dataPath)
    if (dataPathDir.exists()) FileUtils.forceDelete(dataPathDir)
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
  }

  protected val rootPath = this.getClass.getResource("/").getPath
  protected val basePath = rootPath + "tests-working-home"
  protected val warehouse = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute = basePath + "/meta"
  protected val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"

  final override protected val resourcePath: String = "" // ch not need this
  override protected val fileFormat: String = "parquet"
}
// scalastyle:off line.size.limit
