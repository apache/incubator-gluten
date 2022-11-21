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
import org.apache.spark.sql.internal.SQLConf

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {

  val enableNativeEngine: Boolean =
    conf.getConfString("spark.gluten.sql.enable.native.engine", "true").toBoolean

  // This is tmp config to specify whether to enable the native validation based on
  // Substrait plan. After the validations in all backends are correctly implemented,
  // this config should be removed.
  val enableNativeValidation: Boolean =
    conf.getConfString("spark.gluten.sql.enable.native.validation", "true").toBoolean

  // enable or disable columnar batchscan
  val enableColumnarBatchScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.batchscan", "true").toBoolean

  // enable or disable columnar filescan
  val enableColumnarFileScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filescan", "true").toBoolean

  // enable or disable columnar hashagg
  val enableColumnarHashAgg: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.hashagg", "true").toBoolean

  // enable or disable columnar project
  val enableColumnarProject: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.project", "true").toBoolean

  // enable or disable columnar filter
  val enableColumnarFilter: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filter", "true").toBoolean

  // enable or disable columnar sort
  val enableColumnarSort: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sort", "true").toBoolean

  // enable or disable codegen columnar sort
  val enableColumnarCodegenSort: Boolean = conf
    .getConfString("spark.gluten.sql.columnar.codegen.sort", "true")
    .toBoolean && enableColumnarSort

  // enable or disable columnar window
  val enableColumnarWindow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.window", "true").toBoolean

  // enable or disable columnar shuffledhashjoin
  val enableColumnarShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffledhashjoin", "true").toBoolean

  val enableNativeColumnarToRow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.columnartorow", "true").toBoolean

  val forceShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.forceshuffledhashjoin", "true").toBoolean

  // enable or disable columnar sortmergejoin
  // this should be set with preferSortMergeJoin=false
  val enableColumnarSortMergeJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sortmergejoin", "true").toBoolean

  val enableColumnarSortMergeJoinLazyRead: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sortmergejoin.lazyread", "false").toBoolean

  // enable or disable columnar union
  val enableColumnarUnion: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.union", "true").toBoolean

  // enable or disable columnar expand
  val enableColumnarExpand: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.expand", "true").toBoolean

  // enable or disable columnar broadcastexchange
  val enableColumnarBroadcastExchange: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastexchange", "true").toBoolean

  // enable or disable NAN check
  val enableColumnarNaNCheck: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.nanCheck", "true").toBoolean

  // enable or disable hashcompare in hashjoins or hashagg
  val hashCompare: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.hashCompare", "true").toBoolean

  // enable or disable columnar BroadcastHashJoin
  val enableColumnarBroadcastJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastJoin", "true").toBoolean

  // enable or disable columnar columnar arrow udf
  val enableColumnarArrowUDF: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.arrowudf", "true").toBoolean

  // enable or disable columnar wholestage transform
  val enableColumnarWholeStageTransform: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.wholestagetransform", "true").toBoolean

  // whether to use ColumnarShuffleManager
  val isUseColumnarShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // enable or disable columnar exchange
  val enableColumnarShuffle: Boolean =
    conf
      .getConfString("spark.gluten.sql.columnar.shuffle", "true")
      .toBoolean || isUseColumnarShuffleManager

  // prefer to use columnar operators if set to true
  val enablePreferColumnar: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.preferColumnar", "true").toBoolean

  // This config is used for specifying whether to use a columnar iterator in WS transformer.
  val enableColumnarIterator: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.iterator", "true").toBoolean

  // This config is used for deciding whether to load the native library.
  // When false, only Java code will be executed for a quick test.
  val loadNative: Boolean =
    conf.getConfString(GlutenConfig.GLUTEN_LOAD_NATIVE, "true").toBoolean

  // This config is used for specifying the name of the native library.
  val nativeLibName: String =
    conf.getConfString(GlutenConfig.GLUTEN_LIB_NAME, "spark_columnar_jni")

  // This config is used for specifying the absolute path of the native library.
  val nativeLibPath: String =
    conf.getConfString(GlutenConfig.GLUTEN_LIB_PATH, "")

  // fallback to row operators if there are several continous joins
  val joinOptimizationThrottle: Integer =
    conf.getConfString("spark.gluten.sql.columnar.joinOptimizationLevel", "12").toInt

  val batchSize: Int =
    conf.getConfString(GlutenConfig.SPARK_BATCH_SIZE, "32768").toInt

  // enable or disable metrics in columnar wholestagecodegen operator
  val enableMetricsTime: Boolean =
    conf
      .getConfString("spark.gluten.sql.columnar.wholestagecodegen.breakdownTime", "false")
      .toBoolean

  // a folder to store the codegen files
  val tmpFile: String =
    conf.getConfString("spark.gluten.sql.columnar.tmp_dir", null)

  @deprecated val broadcastCacheTimeout: Int =
    conf.getConfString("spark.sql.columnar.sort.broadcast.cache.timeout", "-1").toInt

  // Whether to spill the partition buffers when buffers are full.
  // If false, the partition buffers will be cached in memory first,
  // and the cached buffers will be spilled when reach maximum memory.
  val columnarShufflePreferSpill: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.preferSpill", "true").toBoolean

  val columnarShuffleWriteSchema: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.writeSchema", "false").toBoolean

  // The supported customized compression codec is lz4.
  val columnarShuffleUseCustomizedCompressionCodec: String =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "lz4")

  val columnarShuffleBatchCompressThreshold: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.batchCompressThreshold", "100").toInt

  val shuffleSplitDefaultSize: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192").toInt

  val enableCoalesceBatches: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.coalesce.batches", "true").toBoolean

  val enableColumnarLimit: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.limit", "true").toBoolean

  val numaBindingInfo: GlutenNumaBindingInfo = {
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

  val transformPlanLogLevel: String =
    conf.getConfString("spark.gluten.sql.transform.logLevel", "DEBUG")

}

object GlutenConfig {

  val GLUTEN_LOAD_NATIVE = "spark.gluten.sql.columnar.loadnative"
  val GLUTEN_LIB_NAME = "spark.gluten.sql.columnar.libname"
  val GLUTEN_LIB_PATH = "spark.gluten.sql.columnar.libpath"

  // Hive configurations.
  val HIVE_EXEC_ORC_STRIPE_SIZE = "hive.exec.orc.stripe.size"
  val SPARK_HIVE_EXEC_ORC_STRIPE_SIZE: String = "spark." + HIVE_EXEC_ORC_STRIPE_SIZE
  val HIVE_EXEC_ORC_ROW_INDEX_STRIDE = "hive.exec.orc.row.index.stride"
  val SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE: String = "spark." + HIVE_EXEC_ORC_ROW_INDEX_STRIDE
  val HIVE_EXEC_ORC_COMPRESS = "hive.exec.orc.compress"
  val SPARK_HIVE_EXEC_ORC_COMPRESS: String = "spark." + HIVE_EXEC_ORC_COMPRESS

  // S3 config
  val S3_ACCESS_KEY = "hadoop.fs.s3a.access.key"
  val SPARK_S3_ACCESS_KEY: String = "spark." + S3_ACCESS_KEY
  val S3_SECRET_KEY = "hadoop.fs.s3a.secret.key"
  val SPARK_S3_SECRET_KEY: String = "spark." + S3_SECRET_KEY
  val S3_ENDPOINT = "hadoop.fs.s3a.endpoint"
  val SPARK_S3_ENDPOINT: String = "spark." + S3_ENDPOINT
  val S3_CONNECTION_SSL_ENABLED = "hadoop.fs.s3a.connection.ssl.enabled"
  val SPARK_S3_CONNECTION_SSL_ENABLED: String = "spark." + S3_CONNECTION_SSL_ENABLED
  val S3_PATH_STYLE_ACCESS = "hadoop.fs.s3a.path.style.access"
  val SPARK_S3_PATH_STYLE_ACCESS: String = "spark." + S3_PATH_STYLE_ACCESS

  val SPARK_BATCH_SIZE = "spark.sql.execution.arrow.maxRecordsPerBatch"

  // Backends.
  val GLUTEN_VELOX_BACKEND = "velox"
  val GLUTEN_CLICKHOUSE_BACKEND = "ch"
  val GLUTEN_GAZELLE_BACKEND = "gazelle_cpp"

  // For ClickHouse Backends.
  // FIXME 11/21/22 Hongze:
  //   Options with prefix "GLUTEN_CLICKHOUSE" should be finally removed from this file
  val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = true
  val GLUTEN_CLICKHOUSE_CONFIG_PREFIX = "spark.gluten.sql.columnar.backend.ch"

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

  var ins: GlutenConfig = _
  var random_temp_dir_path: String = _

  /** @deprecated We should avoid caching this value in entire JVM. use #getSessionConf instead. */
  @deprecated
  def getConf: GlutenConfig = synchronized {
    if (ins == null) {
      ins = getSessionConf
    }
    ins
  }

  def getSessionConf: GlutenConfig = {
    new GlutenConfig(SQLConf.get)
  }

  def getBatchSize: Int = synchronized {
    if (ins == null) {
      10000
    } else {
      ins.batchSize
    }
  }

  def getEnableMetricsTime: Boolean = synchronized {
    if (ins == null) {
      false
    } else {
      ins.enableMetricsTime
    }
  }

  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile != null) {
      ins.tmpFile
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }

  def getRandomTempDir: String = synchronized {
    random_temp_dir_path
  }

  def setRandomTempDir(path: String): Unit = synchronized {
    random_temp_dir_path = path
  }

}
