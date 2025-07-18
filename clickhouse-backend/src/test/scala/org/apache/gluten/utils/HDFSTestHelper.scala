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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File

class HDFSTestHelper(TMP_PREFIX: String, independentDir: String) {

  // HDFS parameters
  val HDFS_CACHE_PATH = s"$TMP_PREFIX/hdfs/cache"
  private val HDFS_METADATA_PATH = s"$TMP_PREFIX/hdfs/metadata"
  private val HDFS_URL_ENDPOINT = "hdfs://127.0.0.1:8020"

  def hdfsURL(dirName: String): String = s"$HDFS_URL_ENDPOINT/$dirName"
  def independentHdfsURL(dirName: String = ""): String =
    if (dirName.isEmpty) {
      s"$HDFS_URL_ENDPOINT/$independentDir"
    } else {
      s"$HDFS_URL_ENDPOINT/$independentDir/$dirName"
    }
  def metaPath(dirName: String): String = s"$HDFS_METADATA_PATH/$dirName"

  def builder(policyName: String): StoreConfigBuilder = new StoreConfigBuilder(policyName)

  val STORE_POLICY = "__hdfs_main"
  val STORE_POLICY_ROCKSDB = "__hdfs_main_rocksdb"

  def deleteDir(dirName: String): Unit = {
    val hdfs_dir = hdfsDir(dirName)
    val conf = new Configuration
    conf.set("fs.defaultFS", hdfs_dir)
    val fs = FileSystem.get(conf)
    fs.delete(new Path(hdfs_dir), true)
  }

  def countDir(dirName: String): Int = {
    val hdfs_dir = hdfsDir(dirName)

    val conf = new Configuration
    conf.set("fs.defaultFS", hdfs_dir)
    val fs = FileSystem.get(conf)
    val it = fs.listFiles(new Path(hdfs_dir), true)
    var count = 0
    while (it.hasNext) {
      val f = it.next()
      if (!f.getPath.toString.endsWith(".crc") && !f.getPath.toString.endsWith("vacuum_info")) {
        count += 1
      }
    }
    count
  }

  private def hdfsDir(dirName: String) = {
    dirName match {
      case d if d.startsWith("/") => s"$HDFS_URL_ENDPOINT$d"
      case d if d.startsWith(HDFS_URL_ENDPOINT) =>
        d // Keep paths that already start with the endpoint
      case d => s"$HDFS_URL_ENDPOINT/$d"
    }
  }

  def setStoreConfig(conf: SparkConf): SparkConf = {
    builder(STORE_POLICY)
      .withEndpoint(s"$HDFS_URL_ENDPOINT/")
      .withMetadataPath(HDFS_METADATA_PATH)
      .withCachePath(HDFS_CACHE_PATH)
      .build(conf)

    builder(STORE_POLICY_ROCKSDB)
      .withEndpoint(s"$HDFS_URL_ENDPOINT/")
      .withMetadataPath(HDFS_METADATA_PATH)
      .withDiskcache(false)
      .withRocksDB(true)
      .build(conf)
  }

  def setFileSystem(conf: SparkConf): SparkConf = {
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
