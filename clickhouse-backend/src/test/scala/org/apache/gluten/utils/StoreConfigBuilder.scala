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
import org.apache.gluten.backendsapi.clickhouse.GlutenObjectStorageConfig

import org.apache.spark.SparkConf

import scala.collection.mutable

class StoreConfigBuilder(
    val policyName: String,
    val diskType: String = GlutenObjectStorageConfig.HDFS_DISK_TYPE) {
  private var useDiskcache: Boolean = true
  private var useRocksDB: Boolean = false

  private var CACHE_PATH: String = _
  private var METADATA_PATH: String = _
  private var ENDPOINT: String = _

  private var ak: String = _
  private var sk: String = _

  def withEndpoint(endpoint: String): StoreConfigBuilder = {
    this.ENDPOINT = endpoint
    this
  }

  def withMetadataPath(metadataPath: String): StoreConfigBuilder = {
    this.METADATA_PATH = metadataPath
    this
  }

  def withCachePath(cachePath: String): StoreConfigBuilder = {
    this.CACHE_PATH = cachePath
    this
  }

  def withDiskcache(useDiskcache: Boolean): StoreConfigBuilder = {
    this.useDiskcache = useDiskcache
    this
  }

  def withAKSK(ak: String, sk: String): StoreConfigBuilder = {
    require(ak != null && sk != null, "ak and sk must not be null")
    this.ak = ak
    this.sk = sk
    this
  }

  def withRocksDB(useRocksDB: Boolean): StoreConfigBuilder = {
    this.useRocksDB = useRocksDB
    this
  }

  private def extractStorageType(typeString: String): String = {
    if (typeString.contains("_")) {
      typeString.split("_").head
    } else {
      typeString
    }
  }

  def build(conf: SparkConf): SparkConf = {
    val prefix = extractStorageType(diskType)
    val disk = if (useRocksDB) s"${prefix}2" else prefix
    val disk_cache = if (useRocksDB) s"${prefix}_cache2" else s"${prefix}_cache"
    val main_disk = if (useDiskcache) disk_cache else disk

    require(ENDPOINT != null, "ENDPOINT is null")
    require(METADATA_PATH != null, "METADATA_PATH is null")

    val settings = mutable.ArrayBuffer[(String, String)]()
    settings.appendAll(
      Seq(
        s"storage_configuration.disks.$disk.type" -> diskType,
        s"storage_configuration.disks.$disk.endpoint" -> s"$ENDPOINT",
        s"storage_configuration.disks.$disk.metadata_path" -> METADATA_PATH
      ))

    if (ak != null && sk != null) {
      settings.appendAll(
        Seq(
          s"storage_configuration.disks.$disk.access_key_id" -> ak,
          s"storage_configuration.disks.$disk.secret_access_key" -> sk
        ))
    }

    if (useDiskcache) {
      require(CACHE_PATH != null, "CACHE_PATH is null")
      settings.appendAll(
        Seq(
          s"storage_configuration.disks.$disk_cache.type" -> "cache",
          s"storage_configuration.disks.$disk_cache.disk" -> disk,
          s"storage_configuration.disks.$disk_cache.path" -> CACHE_PATH,
          s"storage_configuration.disks.$disk_cache.max_size" -> "10Gi"
        ))
    }
    settings.appendAll(
      Seq(
        s"storage_configuration.policies.$policyName.volumes" -> "main",
        s"storage_configuration.policies.$policyName.volumes.main.disk" -> main_disk
      ))

    if (useRocksDB) {
      settings.append(s"storage_configuration.disks.$disk.metadata_type" -> "rocksdb")
    }
    settings.foreach(setting => conf.setCHConfig(setting))
    conf
  }
}
