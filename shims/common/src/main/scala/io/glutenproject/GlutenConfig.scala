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
package io.glutenproject

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

import com.google.common.collect.ImmutableList
import org.apache.hadoop.security.UserGroupInformation

import java.util
import java.util.Locale

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {
  import GlutenConfig._

  def enableGluten: Boolean = conf.getConf(ENABLED_GLUTEN)

  def enableAnsiMode: Boolean = conf.ansiEnabled

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  def enableNativeValidation: Boolean = conf.getConf(NATIVE_VALIDATION_ENABLED)

  def enableColumnarBatchScan: Boolean = conf.getConf(COLUMNAR_BATCHSCAN_ENABLED)

  def enableColumnarFileScan: Boolean = conf.getConf(COLUMNAR_FILESCAN_ENABLED)

  def enableColumnarHiveTableScan: Boolean = conf.getConf(COLUMNAR_HIVETABLESCAN_ENABLED)

  def enableVanillaColumnarReaders: Boolean = conf.getConf(VANILLA_COLUMNAR_READERS_ENABLED)

  def enableColumnarHashAgg: Boolean = conf.getConf(COLUMNAR_HASHAGG_ENABLED)

  def forceToUseHashAgg: Boolean = conf.getConf(COLUMNAR_FORCE_HASHAGG_ENABLED)

  def enableColumnarProject: Boolean = conf.getConf(COLUMNAR_PROJECT_ENABLED)

  def enableColumnarFilter: Boolean = conf.getConf(COLUMNAR_FILTER_ENABLED)

  def enableColumnarSort: Boolean = conf.getConf(COLUMNAR_SORT_ENABLED)

  def enableColumnarWindow: Boolean = conf.getConf(COLUMNAR_WINDOW_ENABLED)

  def enableColumnarShuffledHashJoin: Boolean = conf.getConf(COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED)

  def enableNativeColumnarToRow: Boolean = conf.getConf(COLUMNAR_COLUMNAR_TO_ROW_ENABLED)

  def forceShuffledHashJoin: Boolean = conf.getConf(COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED)

  def enableColumnarSortMergeJoin: Boolean = conf.getConf(COLUMNAR_SORTMERGEJOIN_ENABLED)

  def enableColumnarUnion: Boolean = conf.getConf(COLUMNAR_UNION_ENABLED)

  def enableColumnarExpand: Boolean = conf.getConf(COLUMNAR_EXPAND_ENABLED)

  def enableColumnarBroadcastExchange: Boolean = conf.getConf(COLUMNAR_BROADCAST_EXCHANGE_ENABLED)

  def enableColumnarBroadcastJoin: Boolean = conf.getConf(COLUMNAR_BROADCAST_JOIN_ENABLED)

  def enableColumnarArrowUDF: Boolean = conf.getConf(COLUMNAR_ARROW_UDF_ENABLED)

  def enableColumnarCoalesce: Boolean = conf.getConf(COLUMNAR_COALESCE_ENABLED)

  // whether to use ColumnarShuffleManager
  def isUseColumnarShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // whether to use CelebornShuffleManager
  def isUseCelebornShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager")

  def enableColumnarShuffle: Boolean = conf.getConf(COLUMNAR_SHUFFLE_ENABLED)

  def enablePreferColumnar: Boolean = conf.getConf(COLUMNAR_PREFER_ENABLED)

  def enableOneRowRelationColumnar: Boolean = conf.getConf(COLUMNAR_ONE_ROW_RELATION_ENABLED)

  def enableColumnarIterator: Boolean = conf.getConf(COLUMNAR_ITERATOR_ENABLED)

  def physicalJoinOptimizationThrottle: Integer =
    conf.getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE)

  def enablePhysicalJoinOptimize: Boolean =
    conf.getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED)

  def logicalJoinOptimizationThrottle: Integer =
    conf.getConf(COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_THROTTLE)

  def enableLogicalJoinOptimize: Boolean =
    conf.getConf(COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_ENABLED)

  def enableScanOnly: Boolean = conf.getConf(COLUMNAR_SCAN_ONLY_ENABLED)

  def tmpFile: Option[String] = conf.getConf(COLUMNAR_TEMP_DIR)

  @deprecated def broadcastCacheTimeout: Int = conf.getConf(COLUMNAR_BROADCAST_CACHE_TIMEOUT)

  def columnarShufflePreferSpill: Boolean = conf.getConf(COLUMNAR_SHUFFLE_PREFER_SPILL_ENABLED)

  def columnarShuffleWriteSchema: Boolean = conf.getConf(COLUMNAR_SHUFFLE_WRITE_SCHEMA_ENABLED)

  def columnarShuffleCodec: Option[String] = conf.getConf(COLUMNAR_SHUFFLE_CODEC)

  def columnarShuffleCodecBackend: Option[String] = conf
    .getConf(COLUMNAR_SHUFFLE_CODEC_BACKEND)
    .filter(Set(GLUTEN_QAT_BACKEND_NAME, GLUTEN_IAA_BACKEND_NAME).contains(_))

  def columnarShuffleEnableQat: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_QAT_BACKEND_NAME)

  def columnarShuffleEnableIaa: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_IAA_BACKEND_NAME)

  def columnarShuffleBatchCompressThreshold: Int =
    conf.getConf(COLUMNAR_SHUFFLE_BATCH_COMPRESS_THRESHOLD)

  def maxBatchSize: Int = conf.getConf(COLUMNAR_MAX_BATCH_SIZE)

  def enableColumnarLimit: Boolean = conf.getConf(COLUMNAR_LIMIT_ENABLED)

  def enableColumnarGenerate: Boolean = conf.getConf(COLUMNAR_GENERATE_ENABLED)

  def enableTakeOrderedAndProject: Boolean =
    conf.getConf(COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED)

  def enableNativeBloomFilter: Boolean = conf.getConf(COLUMNAR_NATIVE_BLOOMFILTER_ENABLED)

  def enableNativeHyperLogLogAggregateFunction: Boolean =
    conf.getConf(COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED)

  def columnarParquetWriteBlockSize: Long =
    conf.getConf(COLUMNAR_PARQUET_WRITE_BLOCK_SIZE)

  def columnarParquetWriteBlockRows: Long =
    conf.getConf(COLUMNAR_PARQUET_WRITE_BLOCK_ROWS)

  def wholeStageFallbackThreshold: Int = conf.getConf(COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD)

  def numaBindingInfo: GlutenNumaBindingInfo = {
    val enableNumaBinding: Boolean = conf.getConf(COLUMNAR_NUMA_BINDING_ENABLED)
    if (!enableNumaBinding) {
      GlutenNumaBindingInfo(enableNumaBinding = false)
    } else {
      val tmp = conf.getConf(COLUMNAR_NUMA_BINDING_CORE_RANGE)
      if (tmp.isEmpty) {
        GlutenNumaBindingInfo(enableNumaBinding = false)
      } else {
        val numCores = conf.getConfString("spark.executor.cores", "1").toInt
        val coreRangeList: Array[String] = tmp.get.split('|').map(_.trim)
        GlutenNumaBindingInfo(enableNumaBinding = true, coreRangeList, numCores)
      }

    }
  }

  def offHeapMemorySize: Long = conf.getConf(COLUMNAR_OFFHEAP_SIZE_IN_BYTES)

  def taskOffHeapMemorySize: Long = conf.getConf(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES)

  def enableVeloxCache: Boolean = conf.getConf(COLUMNAR_VELOX_CACHE_ENABLED)

  def veloxMemCacheSize: Long = conf.getConf(COLUMNAR_VELOX_MEM_CACHE_SIZE)

  def veloxSsdCachePath: String = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_PATH)

  def veloxSsdCacheSize: Long = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_SIZE)

  def veloxSsdCacheShards: Integer = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_SHARDS)

  def veloxSsdCacheIOThreads: Integer = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_IO_THREADS)

  def veloxSsdODirectEnabled: Boolean = conf.getConf(COLUMNAR_VELOX_SSD_ODIRECT_ENABLED)

  def veloxConnectorIOThreads: Integer = conf.getConf(COLUMNAR_VELOX_CONNECTOR_IO_THREADS)

  def veloxSplitPreloadPerDriver: Integer = conf.getConf(COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER)

  def veloxSpillStrategy: String = conf.getConf(COLUMNAR_VELOX_SPILL_STRATEGY)

  def transformPlanLogLevel: String = conf.getConf(TRANSFORM_PLAN_LOG_LEVEL)

  def substraitPlanLogLevel: String = conf.getConf(SUBSTRAIT_PLAN_LOG_LEVEL)

  def validateFailureLogLevel: String = conf.getConf(VALIDATE_FAILURE_LOG_LEVEL)

  def softAffinityLogLevel: String = conf.getConf(SOFT_AFFINITY_LOG_LEVEL)

  // A comma-separated list of classes for the extended columnar pre rules
  def extendedColumnarPreRules: String = conf.getConf(EXTENDED_COLUMNAR_PRE_RULES)

  // A comma-separated list of classes for the extended columnar post rules
  def extendedColumnarPostRules: String = conf.getConf(EXTENDED_COLUMNAR_POST_RULES)

  def extendedExpressionTransformer: String = conf.getConf(EXTENDED_EXPRESSION_TRAN_CONF)

  def printStackOnValidateFailure: Boolean =
    conf.getConf(VALIDATE_FAILURE_PRINT_STACK_ENABLED)

  def enableFallbackReport: Boolean = conf.getConf(FALLBACK_REPORTER_ENABLED)

  def debug: Boolean = conf.getConf(DEBUG_LEVEL_ENABLED)
  def taskStageId: Int = conf.getConf(BENCHMARK_TASK_STAGEID)
  def taskPartitionId: Int = conf.getConf(BENCHMARK_TASK_PARTITIONID)
  def taskId: Long = conf.getConf(BENCHMARK_TASK_TASK_ID)
  def getInputRowMaxBlockSize: Long =
    conf.getConfString("spark.gluten.sql.input.row.max.block.size", "8192").toLong
}

object GlutenConfig {
  import SQLConf._
  var GLUTEN_ENABLE_BY_DEFAULT = true
  val GLUTEN_ENABLE_KEY = "spark.gluten.enabled"

  val GLUTEN_LIB_NAME = "spark.gluten.sql.columnar.libname"
  val GLUTEN_LIB_PATH = "spark.gluten.sql.columnar.libpath"

  // Hive configurations.
  val SPARK_PREFIX = "spark."
  val HIVE_EXEC_ORC_STRIPE_SIZE = "hive.exec.orc.stripe.size"
  val SPARK_HIVE_EXEC_ORC_STRIPE_SIZE: String = SPARK_PREFIX + HIVE_EXEC_ORC_STRIPE_SIZE
  val HIVE_EXEC_ORC_ROW_INDEX_STRIDE = "hive.exec.orc.row.index.stride"
  val SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE: String = SPARK_PREFIX + HIVE_EXEC_ORC_ROW_INDEX_STRIDE
  val HIVE_EXEC_ORC_COMPRESS = "hive.exec.orc.compress"
  val SPARK_HIVE_EXEC_ORC_COMPRESS: String = SPARK_PREFIX + HIVE_EXEC_ORC_COMPRESS
  val SPARK_SQL_PARQUET_COMPRESSION_CODEC: String = "spark.sql.parquet.compression.codec"
  val PARQUET_BLOCK_SIZE: String = "parquet.block.size"
  val PARQUET_BLOCK_ROWS: String = "parquet.block.rows"
  // Hadoop config
  val HADOOP_PREFIX = "spark.hadoop."

  // S3 config
  val S3A_PREFIX = "fs.s3a."
  val S3_ACCESS_KEY = "fs.s3a.access.key"
  val SPARK_S3_ACCESS_KEY: String = HADOOP_PREFIX + S3_ACCESS_KEY
  val S3_SECRET_KEY = "fs.s3a.secret.key"
  val SPARK_S3_SECRET_KEY: String = HADOOP_PREFIX + S3_SECRET_KEY
  val S3_ENDPOINT = "fs.s3a.endpoint"
  val SPARK_S3_ENDPOINT: String = HADOOP_PREFIX + S3_ENDPOINT
  val S3_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled"
  val SPARK_S3_CONNECTION_SSL_ENABLED: String = HADOOP_PREFIX + S3_CONNECTION_SSL_ENABLED
  val S3_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
  val SPARK_S3_PATH_STYLE_ACCESS: String = HADOOP_PREFIX + S3_PATH_STYLE_ACCESS
  val S3_USE_INSTANCE_CREDENTIALS = "fs.s3a.use.instance.credentials"
  val SPARK_S3_USE_INSTANCE_CREDENTIALS: String = HADOOP_PREFIX + S3_USE_INSTANCE_CREDENTIALS
  val S3_IAM_ROLE = "fs.s3a.iam.role"
  val SPARK_S3_IAM: String = HADOOP_PREFIX + S3_IAM_ROLE
  val S3_IAM_ROLE_SESSION_NAME = "fs.s3a.iam.role.session.name"
  val SPARK_S3_IAM_SESSION_NAME: String = HADOOP_PREFIX + S3_IAM_ROLE_SESSION_NAME

  // Hardware acceleraters backend
  val GLUTEN_SHUFFLE_CODEC_BACKEND = "spark.gluten.sql.columnar.shuffle.codecBackend"
  // QAT config
  val GLUTEN_QAT_BACKEND_NAME = "qat"
  val GLUTEN_QAT_SUPPORTED_CODEC: Set[String] = Set("gzip", "zstd")
  // IAA config
  val GLUTEN_IAA_BACKEND_NAME = "iaa"
  val GLUTEN_IAA_SUPPORTED_CODEC: Set[String] = Set("gzip")

  // Backends.
  val GLUTEN_VELOX_BACKEND = "velox"
  val GLUTEN_CLICKHOUSE_BACKEND = "ch"

  val GLUTEN_CONFIG_PREFIX = "spark.gluten.sql.columnar.backend."

  // Private Spark configs.
  val GLUTEN_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"
  val GLUTEN_OFFHEAP_ENABLED = "spark.memory.offHeap.enabled"

  // For Soft Affinity Scheduling
  // Enable Soft Affinity Scheduling, defalut value is false
  val GLUTEN_SOFT_AFFINITY_ENABLED = "spark.gluten.soft-affinity.enabled"
  val GLUTEN_SOFT_AFFINITY_ENABLED_DEFAULT_VALUE = false
  // Calculate the number of the replcations for scheduling to the target executors per file
  val GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM = "spark.gluten.soft-affinity.replications.num"
  val GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM_DEFAULT_VALUE = 2
  // For on HDFS, if there are already target hosts,
  // and then prefer to use the orginal target hosts to schedule
  val GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS = "spark.gluten.soft-affinity.min.target-hosts"
  val GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS_DEFAULT_VALUE = 1

  // Pass through to native conf
  val GLUTEN_SAVE_DIR = "spark.gluten.saveDir"

  // Added back to Spark Conf during executor initialization
  val GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY = "spark.gluten.memory.offHeap.size.in.bytes"
  val GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY = "spark.gluten.memory.task.offHeap.size.in.bytes"

  // Batch size.
  val GLUTEN_MAX_BATCH_SIZE_KEY = "spark.gluten.sql.columnar.maxBatchSize"

  // Whether load DLL from jars
  val GLUTEN_LOAD_LIB_FROM_JAR = "spark.gluten.loadLibFromJar"
  val GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT = false

  // Expired time of execution with resource relation has cached
  val GLUTEN_RESOURCE_RELATION_EXPIRED_TIME = "spark.gluten.execution.resource.expired.time"
  // unit: SECONDS, default 1 day
  val GLUTEN_RESOURCE_RELATION_EXPIRED_TIME_DEFAULT: Int = 86400

  // Supported hive/pthon/scala udf names
  val GLUTEN_SUPPORTED_HIVE_UDFS = "spark.gluten.supported.hive.udfs"
  val GLUTEN_SUPPORTED_PYTHON_UDFS = "spark.gluten.supported.python.udfs"
  val GLUTEN_SUPPORTED_SCALA_UDFS = "spark.gluten.supported.scala.udfs"

  val GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF =
    "spark.gluten.sql.columnar.extended.expressions.transformer"

  // Principal of current user
  val GLUTEN_UGI_USERNAME = "spark.gluten.ugi.username"
  // Tokens of current user, split by `\0`
  val GLUTEN_UGI_TOKENS = "spark.gluten.ugi.tokens"

  var ins: GlutenConfig = _

  def getConf: GlutenConfig = {
    new GlutenConfig(SQLConf.get)
  }

  @deprecated
  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile.nonEmpty) {
      ins.tmpFile.get
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }

  // TODO Backend-ize this
  def getNativeSessionConf(
      backendPrefix: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()
    val keys = ImmutableList.of(
      GLUTEN_SAVE_DIR,
      GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_MAX_BATCH_SIZE_KEY
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf(k))
        }
      })

    val keyWithDefault = ImmutableList.of(
      (SQLConf.CASE_SENSITIVE.key, "false")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    // FIXME all configs with BE prefix is considered dynamic and static at the same time
    //   We'd untangle this logic
    conf
      .filter(_._1.startsWith(backendPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // Pass the latest tokens to native
    nativeConfMap.put(
      GLUTEN_UGI_TOKENS,
      UserGroupInformation.getCurrentUser.getTokens.asScala
        .map(_.encodeToUrlString)
        .mkString("\u0000"))
    nativeConfMap.put(GLUTEN_UGI_USERNAME, UserGroupInformation.getCurrentUser.getUserName)

    // return
    nativeConfMap
  }

  // TODO: some of the config is dynamic in spark, but is static in gluten, because it should be
  //  used to construct HiveConnector which intends reused in velox
  def getNativeBackendConf(
      backendPrefix: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()

    // some configs having default values
    val keyWithDefault = ImmutableList.of(
      (SPARK_S3_ACCESS_KEY, ""),
      (SPARK_S3_SECRET_KEY, ""),
      (SPARK_S3_ENDPOINT, "localhost:9000"),
      (SPARK_S3_CONNECTION_SSL_ENABLED, "false"),
      (SPARK_S3_PATH_STYLE_ACCESS, "true"),
      (SPARK_S3_USE_INSTANCE_CREDENTIALS, "false"),
      (SPARK_S3_IAM, ""),
      (SPARK_S3_IAM_SESSION_NAME, ""),
      (
        COLUMNAR_VELOX_CONNECTOR_IO_THREADS.key,
        COLUMNAR_VELOX_CONNECTOR_IO_THREADS.defaultValueString),
      (
        COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER.key,
        COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER.defaultValueString),
      (COLUMNAR_SHUFFLE_CODEC.key, ""),
      (COLUMNAR_SHUFFLE_CODEC_BACKEND.key, ""),
      ("spark.hadoop.input.connect.timeout", "180000"),
      ("spark.hadoop.input.read.timeout", "180000"),
      ("spark.hadoop.input.write.timeout", "180000"),
      ("spark.hadoop.dfs.client.log.severity", "INFO")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    val keys = ImmutableList.of(
      // datasource config
      SPARK_SQL_PARQUET_COMPRESSION_CODEC,
      // datasource config end

      GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_OFFHEAP_ENABLED
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf(k))
        }
      })

    conf
      .filter(_._1.startsWith(backendPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // put in all S3 configs
    conf
      .filter(_._1.startsWith(HADOOP_PREFIX + S3A_PREFIX))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // return
    nativeConfMap
  }

  val ENABLED_GLUTEN =
    buildConf("spark.gluten.enabled")
      .internal()
      .doc("Whether to enable gluten. Default value is true.")
      .booleanConf
      .createWithDefault(true)

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  val NATIVE_VALIDATION_ENABLED =
    buildConf("spark.gluten.sql.enable.native.validation")
      .internal()
      .doc(
        "This is tmp config to specify whether to enable the native validation based on " +
          "Substrait plan. After the validations in all backends are correctly implemented, " +
          "this config should be removed.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BATCHSCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.batchscan")
      .internal()
      .doc("Enable or disable columnar batchscan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.filescan")
      .internal()
      .doc("Enable or disable columnar filescan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HIVETABLESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.hivetablescan")
      .internal()
      .doc("Enable or disable columnar hivetablescan.")
      .booleanConf
      .createWithDefault(true)

  val VANILLA_COLUMNAR_READERS_ENABLED =
    buildConf("spark.gluten.sql.columnar.vanillaReaders")
      .internal()
      .doc("Enable or disable columnar vanilla scan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.hashagg")
      .internal()
      .doc("Enable or disable columnar hashagg.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FORCE_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.force.hashagg")
      .internal()
      .doc("Whether to force to use gluten's hash agg for replacing vanilla spark's sort agg.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.project")
      .internal()
      .doc("Enable or disable columnar project.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.filter")
      .internal()
      .doc("Enable or disable columnar filter.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORT_ENABLED =
    buildConf("spark.gluten.sql.columnar.sort")
      .internal()
      .doc("Enable or disable columnar sort.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_WINDOW_ENABLED =
    buildConf("spark.gluten.sql.columnar.window")
      .internal()
      .doc("Enable or disable columnar window.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.forceShuffledHashJoin")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffledHashJoin")
      .internal()
      .doc("Enable or disable columnar shuffledHashJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_COLUMNAR_TO_ROW_ENABLED =
    buildConf("spark.gluten.sql.columnar.columnarToRow")
      .internal()
      .doc("Enable or disable columnar columnarToRow.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORTMERGEJOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.sortMergeJoin")
      .internal()
      .doc("Enable or disable columnar sortMergeJoin. " +
        "This should be set with preferSortMergeJoin=false.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_UNION_ENABLED =
    buildConf("spark.gluten.sql.columnar.union")
      .internal()
      .doc("Enable or disable columnar union.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_EXPAND_ENABLED =
    buildConf("spark.gluten.sql.columnar.expand")
      .internal()
      .doc("Enable or disable columnar expand.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_EXCHANGE_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastExchange")
      .internal()
      .doc("Enable or disable columnar broadcastExchange.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastJoin")
      .internal()
      .doc("Enable or disable columnar broadcastJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ARROW_UDF_ENABLED =
    buildConf("spark.gluten.sql.columnar.arrowUdf")
      .internal()
      .doc("Enable or disable columnar arrow udf.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_COALESCE_ENABLED =
    buildConf("spark.gluten.sql.columnar.coalesce")
      .internal()
      .doc("Enable or disable columnar coalesce.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLE_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle")
      .internal()
      .doc("Enable or disable columnar shuffle.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PREFER_ENABLED =
    buildConf("spark.gluten.sql.columnar.preferColumnar")
      .internal()
      .doc("Prefer to use columnar operators if set to true.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ONE_ROW_RELATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.oneRowRelation")
      .internal()
      .doc("Enable or disable columnar `OneRowRelation`.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ITERATOR_ENABLED =
    buildConf("spark.gluten.sql.columnar.iterator")
      .internal()
      .doc(
        "This config is used for specifying whether to use a columnar iterator in WS transformer.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizationLevel")
      .internal()
      .doc("Fallback to row operators if there are several continuous joins.")
      .intConf
      .createWithDefault(12)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizeEnable")
      .internal()
      .doc("Enable or disable columnar physicalJoinOptimize.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_THROTTLE =
    buildConf("spark.gluten.sql.columnar.logicalJoinOptimizationLevel")
      .internal()
      .doc("Fallback to row operators if there are several continuous joins.")
      .intConf
      .createWithDefault(12)

  val COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.logicalJoinOptimizeEnable")
      .internal()
      .doc("Enable or disable columnar logicalJoinOptimize.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SCAN_ONLY_ENABLED =
    buildConf("spark.gluten.sql.columnar.scanOnly")
      .internal()
      .doc("When enabled, only scan and the filter after scan will be offloaded to native.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_TEMP_DIR =
    buildConf("spark.gluten.sql.columnar.tmp_dir")
      .internal()
      .doc("A folder to store the codegen files.")
      .stringConf
      .createOptional

  val COLUMNAR_BROADCAST_CACHE_TIMEOUT =
    buildConf("spark.sql.columnar.sort.broadcast.cache.timeout")
      .internal()
      .doc("Deprecated")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_SHUFFLE_PREFER_SPILL_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle.preferSpill")
      .internal()
      .doc(
        "Whether to spill the partition buffers when buffers are full. " +
          "If false, the partition buffers will be cached in memory first, " +
          "and the cached buffers will be spilled when reach maximum memory.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_WRITE_SCHEMA_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle.writeSchema")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_CODEC =
    buildConf("spark.gluten.sql.columnar.shuffle.codec")
      .internal()
      .doc(
        "By default, the supported codecs are lz4 and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=qat," +
          "the supported codecs are gzip and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=iaa," +
          "the supported codec is gzip.")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_CODEC_BACKEND =
    buildConf(GlutenConfig.GLUTEN_SHUFFLE_CODEC_BACKEND)
      .internal()
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_BATCH_COMPRESS_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.batchCompressThreshold")
      .internal()
      .intConf
      .createWithDefault(100)

  val COLUMNAR_MAX_BATCH_SIZE =
    buildConf(GLUTEN_MAX_BATCH_SIZE_KEY)
      .internal()
      .intConf
      .createWithDefault(4096)

  val COLUMNAR_LIMIT_ENABLED =
    buildConf("spark.gluten.sql.columnar.limit")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_GENERATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.generate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.takeOrderedAndProject")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_BLOOMFILTER_ENABLED =
    buildConf("spark.gluten.sql.native.bloomFilter")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.native.hyperLogLog.Aggregate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PARQUET_WRITE_BLOCK_SIZE =
    buildConf("spark.gluten.sql.columnar.parquet.write.blockSize")
      .internal()
      .longConf
      .createWithDefault(128 * 1024 * 1024)

  val COLUMNAR_PARQUET_WRITE_BLOCK_ROWS =
    buildConf("spark.gluten.sql.native.parquet.write.blockRows")
      .internal()
      .longConf
      .createWithDefault(100 * 1000 * 1000)

  val COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.wholeStage.fallback.threshold")
      .internal()
      .doc("The threshold for whether whole stage will fall back in AQE supported case " +
        "by counting the number of ColumnarToRow & vanilla leaf node.")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_NUMA_BINDING_ENABLED =
    buildConf("spark.gluten.sql.columnar.numaBinding")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_NUMA_BINDING_CORE_RANGE =
    buildConf("spark.gluten.sql.columnar.coreRange")
      .internal()
      .stringConf
      .createOptional

  val COLUMNAR_OFFHEAP_SIZE_IN_BYTES =
    buildConf(GlutenConfig.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY)
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf(GlutenConfig.GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY)
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  // velox caching options
  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .internal()
      .doc("Enable Velox cache, default off")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .internal()
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .internal()
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .internal()
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .internal()
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .internal()
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .internal()
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .internal()
      .doc("The IO threads for connector split preloading")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver")
      .internal()
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val COLUMNAR_VELOX_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillStrategy")
      .internal()
      .doc(
        "none: Disable spill on Velox backend; " +
          "threshold: Use spark.gluten.sql.columnar.backend.velox.memoryCapRatio " +
          "to calculate a memory threshold number for triggering spill; " +
          "auto: Let Spark memory manager manage Velox's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "threshold", "auto"))
      .createWithDefault("threshold")

  val TRANSFORM_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.transform.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val SUBSTRAIT_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.substrait.plan.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val VALIDATE_FAILURE_LOG_LEVEL =
    buildConf("spark.gluten.sql.validate.failure.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("INFO")

  val SOFT_AFFINITY_LOG_LEVEL =
    buildConf("spark.gluten.soft-affinity.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val VALIDATE_FAILURE_PRINT_STACK_ENABLED =
    buildConf("spark.gluten.sql.validate.failure.printStack")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DEBUG_LEVEL_ENABLED =
    buildConf("spark.gluten.sql.debug")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val BENCHMARK_TASK_STAGEID =
    buildConf("spark.gluten.sql.benchmark_task.stageId")
      .internal()
      .intConf
      .createWithDefault(1)

  val BENCHMARK_TASK_PARTITIONID =
    buildConf("spark.gluten.sql.benchmark_task.partitionId")
      .internal()
      .intConf
      .createWithDefault(-1)

  val BENCHMARK_TASK_TASK_ID =
    buildConf("spark.gluten.sql.benchmark_task.taskId")
      .internal()
      .longConf
      .createWithDefault(-1L)

  val UT_STATISTIC =
    buildConf("spark.gluten.sql.ut.statistic")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val EXTENDED_COLUMNAR_PRE_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.pre.rules")
      .doc("A comma-separated list of classes for the extended columnar pre rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_COLUMNAR_POST_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.post.rules")
      .doc("A comma-separated list of classes for the extended columnar post rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_EXPRESSION_TRAN_CONF =
    buildConf(GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF)
      .doc("A class for the extended expressions transformer.")
      .stringConf
      .createWithDefaultString("")

  val FALLBACK_REPORTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.fallbackReporter")
      .doc("When true, enable fallback reporter rule to print fallback reason")
      .booleanConf
      .createWithDefault(true)
}
