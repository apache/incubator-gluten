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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.clickhouse.CHConfig.GlutenCHConf

import org.apache.spark.SparkConf

import org.apache.commons.io.FileUtils

import java.io.File

class HDFSTestHelper(TMP_PREFIX: String) {

  // HDFS parameters
  val HDFS_CACHE_PATH = s"$TMP_PREFIX/hdfs/cache"
  private val HDFS_METADATA_PATH = s"$TMP_PREFIX/hdfs/metadata"
  private val HDFS_URL_ENDPOINT = "hdfs://127.0.0.1:8020"

  def getHdfsUrl(dirName: String): String = s"$HDFS_URL_ENDPOINT/$dirName"
  def metaPath(dirName: String): String = s"$HDFS_METADATA_PATH/$dirName"

  def setHDFSStoreConfig(conf: SparkConf): SparkConf = {
    conf.setCHConfig(
      "storage_configuration.disks.hdfs.type" -> "hdfs_gluten",
      "storage_configuration.disks.hdfs.endpoint" -> s"$HDFS_URL_ENDPOINT/",
      "storage_configuration.disks.hdfs.metadata_path" -> HDFS_METADATA_PATH,
      "storage_configuration.disks.hdfs_cache.type" -> "cache",
      "storage_configuration.disks.hdfs_cache.disk" -> "hdfs",
      "storage_configuration.disks.hdfs_cache.path" -> HDFS_CACHE_PATH,
      "storage_configuration.disks.hdfs_cache.max_size" -> "10Gi",
      "storage_configuration.policies.__hdfs_main.volumes" -> "main",
      "storage_configuration.policies.__hdfs_main.volumes.main.disk" -> "hdfs_cache"
    )
  }

  def setHDFSStoreConfigRocksDB(conf: SparkConf): SparkConf = {
    conf.setCHConfig(
      "storage_configuration.disks.hdfs2.type" -> "hdfs_gluten",
      "storage_configuration.disks.hdfs2.endpoint" -> s"$HDFS_URL_ENDPOINT/",
      "storage_configuration.disks.hdfs2.metadata_path" -> HDFS_METADATA_PATH,
      "storage_configuration.disks.hdfs2.metadata_type" -> "rocksdb",
      "storage_configuration.disks.hdfs_cache2.type" -> "cache",
      "storage_configuration.disks.hdfs_cache2.disk" -> "hdfs2",
      "storage_configuration.disks.hdfs_cache2.path" -> HDFS_CACHE_PATH,
      "storage_configuration.disks.hdfs_cache2.max_size" -> "10Gi",
      "storage_configuration.policies.__hdfs_main_rocksdb.volumes" -> "main",
      "storage_configuration.policies.__hdfs_main_rocksdb.volumes.main.disk" -> "hdfs_cache2"
    )
  }

  def setHdfsClientConfig(conf: SparkConf): SparkConf = {
    conf.setCHConfig(
      "hdfs.dfs_client_read_shortcircuit" -> "false",
      "hdfs.dfs_default_replica" -> "1"
    )
  }

  def resetMeta(): Unit = {
    FileUtils.deleteDirectory(new File(HDFS_METADATA_PATH))
    FileUtils.forceMkdir(new File(HDFS_METADATA_PATH))
  }

  def resetCache(): Unit = {
    FileUtils.deleteDirectory(new File(HDFS_CACHE_PATH))
    FileUtils.forceMkdir(new File(HDFS_CACHE_PATH))
  }
}
