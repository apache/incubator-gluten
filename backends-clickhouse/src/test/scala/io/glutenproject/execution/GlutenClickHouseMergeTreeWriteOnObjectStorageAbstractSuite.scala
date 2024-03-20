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
package io.glutenproject.execution

import _root_.org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import _root_.org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeWriteOnObjectStorageAbstractSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  val S3_METADATA_PATH = "/tmp/metadata/s3"
  val S3_CACHE_PATH = "/tmp/s3_cache"
  val S3_URL = "s3://127.0.0.1:9000/"
  val MINIO_ENDPOINT: String = S3_URL.replace("s3", "http")
  val S3A_URL: String = S3_URL.replace("s3", "s3a")
  val BUCKET_NAME = "test"
  val WHOLE_PATH: String = MINIO_ENDPOINT + BUCKET_NAME + "/"

  val HDFS_METADATA_PATH = "/tmp/metadata/hdfs"
  val HDFS_CACHE_PATH = "/tmp/hdfs_cache"
  val HDFS_URL = "hdfs://127.0.0.1:8020"

  val S3_ACCESS_KEY = "BypTYzcXOlfr03FFIvt4"
  val S3_SECRET_KEY = "K9MDaGItPSaphorZM8t4hXf30gHF9dBWi6L2dK5E"

  protected lazy val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.use_local_format", "false")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "error")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.user_defined_path",
        "/tmp/user_defined")
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
        HDFS_URL + "/")
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
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
  }
  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

}
// scalastyle:off line.size.limit
