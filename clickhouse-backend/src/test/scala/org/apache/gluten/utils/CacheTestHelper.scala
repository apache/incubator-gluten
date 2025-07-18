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

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.backendsapi.clickhouse.CHConfig._
import org.apache.gluten.execution.CHNativeCacheManager

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

class CacheTestHelper(val TMP_PREFIX: String) {

  val LOCAL_CACHE_PATH = s"$TMP_PREFIX/local/cache"
  val CACHE_NAME = "gluten_cache"

  /** Configure SparkConf with cache-related settings */
  def setCacheConfig(conf: SparkConf): SparkConf = {
    conf
      .set(CHConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE.key, "true")
      .setCHConfig("gluten_cache.local.name", CACHE_NAME)
      .setCHConfig("gluten_cache.local.path", LOCAL_CACHE_PATH)
      .setCHConfig("gluten_cache.local.max_size", "10Gi")
      // If reuse_disk_cache is set to false,the cache will be deleted in JNI_OnUnload
      // but CacheManager and JobScheduler of backend are static global variables
      // and is destroyed at the end of the program which causes backend reporting logical errors.
      // TODO: fix reuse_disk_cache
      .setCHConfig("reuse_disk_cache", "true")
  }

  /** Delete cache files for all tables in the data path */
  def deleteCache(spark: SparkSession, dataPaths: String*): Unit = {
    dataPaths.foreach(
      dataPath => {
        val targetFile = new Path(dataPath)
        val fs = targetFile.getFileSystem(spark.sessionState.newHadoopConf())
        if (fs.isDirectory(targetFile)) {
          fs.listStatus(targetFile)
            .foreach(
              data => {
                if (data.isFile) {
                  CHNativeCacheManager
                    .removeFiles(data.getPath.toUri.getPath.substring(1), CACHE_NAME)
                }
              })
        }
      })
  }
}
