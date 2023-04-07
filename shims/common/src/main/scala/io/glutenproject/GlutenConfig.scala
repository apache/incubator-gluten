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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import com.google.common.collect.ImmutableList

import java.util
import java.util.Locale

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {

  def enableAnsiMode: Boolean =
    conf.getConfString("spark.sql.ansi.enabled", "false").toBoolean

  // This is tmp config to specify whether to enable the native validation based on
  // Substrait plan. After the validations in all backends are correctly implemented,
  // this config should be removed.
  //
  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  def enableNativeValidation: Boolean =
    conf.getConfString("spark.gluten.sql.enable.native.validation", "true").toBoolean

  // enable or disable columnar batchscan
  def enableColumnarBatchScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.batchscan", "true").toBoolean

  // enable or disable columnar filescan
  def enableColumnarFileScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filescan", "true").toBoolean

  // enable or disable columnar hashagg
  def enableColumnarHashAgg: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.hashagg", "true").toBoolean

  // enable or disable columnar project
  def enableColumnarProject: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.project", "true").toBoolean

  // enable or disable columnar filter
  def enableColumnarFilter: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filter", "true").toBoolean

  // enable or disable columnar sort
  def enableColumnarSort: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sort", "true").toBoolean

  // enable or disable columnar window
  def enableColumnarWindow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.window", "true").toBoolean

  // enable or disable columnar shuffledhashjoin
  def enableColumnarShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffledhashjoin", "true").toBoolean

  def enableNativeColumnarToRow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.columnartorow", "true").toBoolean

  def forceShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.forceshuffledhashjoin", "true").toBoolean

  // enable or disable columnar sortmergejoin
  // this should be set with preferSortMergeJoin=false
  def enableColumnarSortMergeJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sortmergejoin", "true").toBoolean

  // enable or disable columnar union
  def enableColumnarUnion: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.union", "true").toBoolean

  // enable or disable columnar expand
  def enableColumnarExpand: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.expand", "true").toBoolean

  // enable or disable columnar broadcastexchange
  def enableColumnarBroadcastExchange: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastexchange", "true").toBoolean

  // enable or disable columnar BroadcastHashJoin
  def enableColumnarBroadcastJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastJoin", "true").toBoolean

  // enable or disable columnar columnar arrow udf
  def enableColumnarArrowUDF: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.arrowudf", "true").toBoolean

  // enable or disable columnar columnar coalesce
  def enableColumnarCoalesce: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.coalesce", "true").toBoolean

  // whether to use ColumnarShuffleManager
  def isUseColumnarShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // whether to use CelebornShuffleManager
  def isUseCelebornShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.celeborn.CelebornShuffleManager")

  // enable or disable columnar exchange
  def enableColumnarShuffle: Boolean =
    conf
      .getConfString("spark.gluten.sql.columnar.shuffle", "true")
      .toBoolean

  // prefer to use columnar operators if set to true
  def enablePreferColumnar: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.preferColumnar", "true").toBoolean

  // This config is used for specifying whether to use a columnar iterator in WS transformer.
  def enableColumnarIterator: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.iterator", "true").toBoolean

  // fallback to row operators if there are several continuous joins
  def physicalJoinOptimizationThrottle: Integer =
    conf.getConfString("spark.gluten.sql.columnar.physicalJoinOptimizationLevel", "12").toInt

  def enablePhysicalJoinOptimize: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.physicalJoinOptimizeEnable", "false").toBoolean

  def logicalJoinOptimizationThrottle: Integer =
    conf.getConfString("spark.gluten.sql.columnar.logicalJoinOptimizationLevel", "12").toInt

  def enableLogicalJoinOptimize: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.logicalJoinOptimizeEnable", "false").toBoolean

  // When enabled, only scan and the filter after scan will be offloaded to native.
  def enableScanOnly: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.scanOnly", "false").toBoolean

  // a folder to store the codegen files
  def tmpFile: String =
    conf.getConfString("spark.gluten.sql.columnar.tmp_dir", null)

  @deprecated def broadcastCacheTimeout: Int =
    conf.getConfString("spark.sql.columnar.sort.broadcast.cache.timeout", "-1").toInt

  // Whether to spill the partition buffers when buffers are full.
  // If false, the partition buffers will be cached in memory first,
  // and the cached buffers will be spilled when reach maximum memory.
  def columnarShufflePreferSpill: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.preferSpill", "true").toBoolean

  def columnarShuffleWriteSchema: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.writeSchema", "false").toBoolean

  // When spark.gluten.sql.columnar.qat=false, the supported codecs are lz4 and zstd.
  // When spark.gluten.sql.columnar.qat=true, the supported codec is gzip.
  def columnarShuffleUseCustomizedCompressionCodec: String =
    conf
      .getConfString("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "lz4")
      .toUpperCase(Locale.ROOT)

  def columnarShuffleBatchCompressThreshold: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.batchCompressThreshold", "100").toInt

  def shuffleSplitDefaultSize: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192").toInt

  def enableCoalesceBatches: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.coalesce.batches", "true").toBoolean

  def enableColumnarLimit: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.limit", "true").toBoolean

  def enableColumnarGenerate: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.generate", "true").toBoolean

  def enableTakeOrderedAndProject: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.takeOrderedAndProject", "true").toBoolean

  def enableNativeBloomFilter: Boolean =
    conf.getConfString("spark.gluten.sql.native.bloomFilter", "true").toBoolean

  // The threshold for whether whole stage will fall back in AQE supported case by counting the
  // number of ColumnarToRow & vanilla leaf node.
  def wholeStageFallbackThreshold: Int =
    conf.getConfString("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "-1").toInt

  def numaBindingInfo: GlutenNumaBindingInfo = {
    val enableNumaBinding: Boolean =
      conf.getConfString("spark.gluten.sql.columnar.numaBinding", "false").toBoolean
    if (!enableNumaBinding) {
      GlutenNumaBindingInfo(enableNumaBinding = false)
    } else {
      val tmp = conf.getConfString("spark.gluten.sql.columnar.coreRange", null)
      if (tmp == null) {
        GlutenNumaBindingInfo(enableNumaBinding = false)
      } else {
        val numCores = conf.getConfString("spark.executor.cores", "1").toInt
        val coreRangeList: Array[String] = tmp.split('|').map(_.trim)
        GlutenNumaBindingInfo(enableNumaBinding = true, coreRangeList, numCores)
      }

    }
  }

  def offHeapMemorySize: Long =
    conf.getConfString(GlutenConfig.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY, "0").toLong

  def transformPlanLogLevel: String =
    conf.getConfString("spark.gluten.sql.transform.logLevel", "DEBUG")

  def substraitPlanLogLevel: String =
    conf.getConfString("spark.gluten.sql.substrait.plan.logLevel", "DEBUG")

  def validateFailureLogLevel: String =
    conf.getConfString("spark.gluten.sql.validate.failure.logLevel", "INFO")

  def printStackOnValidateFailure: Boolean =
    conf.getConfString("spark.gluten.sql.validate.failure.printStack", "false").toBoolean

  def debug: Boolean = conf.getConfString("spark.gluten.sql.debug", "false").toBoolean
  def taskStageId: Int = conf.getConfString("spark.gluten.sql.benchmark_task.stageId", "1").toInt
  def taskPartitionId: Int =
    conf.getConfString("spark.gluten.sql.benchmark_task.partitionId", "-1").toInt
  def taskId: Long = conf.getConfString("spark.gluten.sql.benchmark_task.taskId", "-1").toLong
}

object GlutenConfig {

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

  // Hadoop config
  val HADOOP_PREFIX = "spark.hadoop."
  val HDFS_URI = "fs.defaultFS"
  val SPARK_HDFS_URI: String = HADOOP_PREFIX + HDFS_URI

  // S3 config
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

  // QAT config
  val GLUTEN_ENABLE_QAT = "spark.gluten.sql.columnar.qat"
  val GLUTEN_QAT_CODEC_PREFIX = "gluten_qat_"
  val GLUTEN_QAT_SUPPORTED_CODEC: Seq[String] = "GZIP" :: Nil

  // Backends.
  val GLUTEN_VELOX_BACKEND = "velox"
  val GLUTEN_CLICKHOUSE_BACKEND = "ch"

  val GLUTEN_CONFIG_PREFIX = "spark.gluten.sql.columnar.backend."

  // Private Spark configs.
  val GLUTEN_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"

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

  // Added back to Spark Conf during driver / executor initialization
  val GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY = "spark.gluten.memory.offHeap.size.in.bytes"

  // Whether load DLL from jars
  val GLUTEN_LOAD_LIB_FROM_JAR = "spark.gluten.loadLibFromJar"
  val GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT = false

  var ins: GlutenConfig = _

  def getConf: GlutenConfig = {
    new GlutenConfig(SQLConf.get)
  }

  @deprecated
  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile != null) {
      ins.tmpFile
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }

  // TODO Backend-ize this
  def getNativeSessionConf(backendPrefix: String): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()
    val conf = SQLConf.get
    val keys = ImmutableList.of(
      GLUTEN_SAVE_DIR
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf.getConfString(k))
        }
      })

    val keyWithDefault = ImmutableList.of(
      (SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "32768"),
      (SQLConf.CASE_SENSITIVE.key, "false")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getConfString(e._1, e._2)))

    // FIXME all configs with BE prefix is considered dynamic and static at the same time
    //   We'd untangle this logic
    conf.getAllConfs
      .filter(_._1.startsWith(backendPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // return
    nativeConfMap
  }

  // TODO: some of the config is dynamic in spark, but is static in gluten, because it should be
  //  used to construct HiveConnector which intends reused in velox
  def getNativeStaticConf(conf: SparkConf, backendPrefix: String): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()
    val keys = ImmutableList.of(
      // DWRF datasource config
      SPARK_HIVE_EXEC_ORC_STRIPE_SIZE,
      SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE,
      SPARK_HIVE_EXEC_ORC_COMPRESS,
      // DWRF datasource config end
      GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf.get(k))
        }
      })

    val keyWithDefault = ImmutableList.of(
      (SPARK_HDFS_URI, "hdfs://localhost:9000"),
      (SPARK_S3_ACCESS_KEY, "minio"),
      (SPARK_S3_SECRET_KEY, "miniopass"),
      (SPARK_S3_ENDPOINT, "localhost:9000"),
      (SPARK_S3_CONNECTION_SSL_ENABLED, "false"),
      (SPARK_S3_PATH_STYLE_ACCESS, "true"),
      (SPARK_S3_USE_INSTANCE_CREDENTIALS, "false")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.get(e._1, e._2)))
    // velox cache and HiveConnector config
    conf.getAll
      .filter(_._1.startsWith(backendPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // return
    nativeConfMap
  }
}
