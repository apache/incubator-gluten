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
package org.apache.gluten.config

import org.apache.spark.network.util.ByteUnit

import java.util.concurrent.TimeUnit

object VeloxStaticConfig {
  import GlutenConfig.buildStaticConf

  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .internal()
      .doc("Enable Velox cache, default off")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .internal()
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .internal()
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .internal()
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .internal()
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .internal()
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .internal()
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .internal()
      .doc(
        "The Size of the IO thread pool in the Connector. " +
          "This thread pool is used for split preloading and DirectBufferedInput. " +
          "By default, the value is the same as the maximum task slots per Spark executor.")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_ASYNC_TIMEOUT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping")
      .internal()
      .doc(
        "Timeout for asynchronous execution when task is being stopped in Velox backend. " +
          "It's recommended to set to a number larger than network connection timeout that the " +
          "possible aysnc tasks are relying on.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000)

  val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
      .internal()
      .doc("Disables caching if false. File handle cache should be disabled " +
        "if files are mutable, i.e. file content may change while file path stays the same.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.directorySizeGuess")
      .internal()
      .doc("Set the directory size guess for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold")
      .internal()
      .doc("Set the file preload threshold for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.prefetchRowGroups")
      .internal()
      .doc("Set the prefetch row groups for velox file scan")
      .intConf
      .createWithDefault(1)

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.loadQuantum")
      .internal()
      .doc("Set the load quantum for velox file scan, recommend to use the default value (256MB) " +
        "for performance consideration. If Velox cache is enabled, it can be 8MB at most.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance")
      .internal()
      .doc(" Set the max coalesced distance bytes for velox file scan")
      .stringConf
      .createWithDefaultString("512KB")

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes")
      .internal()
      .doc("Set the max coalesced bytes for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct")
      .internal()
      .doc("Set prefetch cache min pct for velox file scan")
      .intConf
      .createWithDefault(0)
}
