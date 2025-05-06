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
  private val HDFS_URL_ENDPOINT = "hdfs://127.0.0.1:8020/"

  def getHdfsUrl(dirName: String): String = s"$HDFS_URL_ENDPOINT/$dirName"
  def metaPath(dirName: String): String = s"$HDFS_METADATA_PATH/$dirName"

  def builder(policyName: String): StoreConfigBuilder = new StoreConfigBuilder(policyName)

  def setCommonHDFSStoreConfig(
      conf: SparkConf,
      policyName: String,
      useDiskcache: Boolean = true,
      useRocksDB: Boolean = false): SparkConf = {

    builder(policyName)
      .withEndpoint(HDFS_URL_ENDPOINT)
      .withMetadataPath(HDFS_METADATA_PATH)
      .withCachePath(HDFS_CACHE_PATH)
      .withDiskcache(useDiskcache)
      .withRocksDB(useRocksDB)
      .build(conf)
  }

  def setHDFSStoreConfig(conf: SparkConf): SparkConf = {
    setCommonHDFSStoreConfig(conf, policyName = "__hdfs_main")
  }

  def setHDFSStoreConfigRocksDB(conf: SparkConf): SparkConf = {
    setCommonHDFSStoreConfig(conf, policyName = "__hdfs_main_rocksdb", useRocksDB = true)
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
