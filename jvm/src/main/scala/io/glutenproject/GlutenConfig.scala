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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {
  def getCpu: Boolean = {
    return true
    // only for developing on mac
    if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac")) {
      true
    } else {
      val source = scala.io.Source.fromFile("/proc/cpuinfo")
      val lines = try source.mkString finally source.close()
      // TODO(): check CPU flags to enable/disable AVX512
      if (lines.contains("GenuineIntel")) {
        true
      } else {
        // System.out.println(actualSchemaRoot.getRowCount());
        logWarning("running on non-intel CPU, disable all columnar operators")
        false
      }
    }
  }

  // for all operators
  val enableCpu: Boolean = getCpu

  val enableNativeEngine: Boolean =
    conf.getConfString("spark.gluten.sql.enable.native.engine", "true").toBoolean && enableCpu

  // This is tmp config to specify whether to enable the native validation based on
  // Substrait plan. After the validations in all backends are correctly implemented,
  // this config should be removed.
  val enableNativeValidation: Boolean =
    conf.getConfString("spark.gluten.sql.enable.native.validation", "true").toBoolean

  // enable or disable columnar batchscan
  val enableColumnarBatchScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.batchscan", "true").toBoolean && enableCpu

  // enable or disable columnar filescan
  val enableColumnarFileScan: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filescan", "true").toBoolean && enableCpu

  // enable or disable columnar hashagg
  val enableColumnarHashAgg: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.hashagg", "true").toBoolean && enableCpu

  // A tmp config used to fallback Final Aggregation.
  // Can be removed after Final Aggregation is fully supported.
  val enableColumnarFinalAgg: Boolean = conf.getConfString(
    "spark.gluten.sql.columnar.hashagg.enablefinal", "true").toBoolean && enableCpu

  // enable or disable columnar project
  val enableColumnarProject: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.project", "true").toBoolean && enableCpu

  // enable or disable columnar filter
  val enableColumnarFilter: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.filter", "true").toBoolean && enableCpu

  // enable or disable columnar sort
  val enableColumnarSort: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sort", "true").toBoolean && enableCpu

  // enable or disable codegen columnar sort
  val enableColumnarCodegenSort: Boolean = conf.getConfString(
    "spark.gluten.sql.columnar.codegen.sort", "true").toBoolean && enableColumnarSort

  // enable or disable columnar window
  val enableColumnarWindow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.window", "true").toBoolean && enableCpu

  // enable or disable columnar shuffledhashjoin
  val enableColumnarShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.shuffledhashjoin", "true").toBoolean && enableCpu

  val enableNativeColumnarToRow: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.columnartorow", "true").toBoolean && enableCpu

  val forceShuffledHashJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.forceshuffledhashjoin", "false").toBoolean &&
      enableCpu

  // enable or disable columnar sortmergejoin
  // this should be set with preferSortMergeJoin=false
  val enableColumnarSortMergeJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sortmergejoin", "true").toBoolean && enableCpu

  val enableColumnarSortMergeJoinLazyRead: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.sortmergejoin.lazyread", "false").toBoolean

  // enable or disable columnar union
  val enableColumnarUnion: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.union", "true").toBoolean && enableCpu

  // enable or disable columnar expand
  val enableColumnarExpand: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.expand", "true").toBoolean && enableCpu

  // enable or disable columnar broadcastexchange
  val enableColumnarBroadcastExchange: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastexchange", "true").toBoolean && enableCpu

  // enable or disable NAN check
  val enableColumnarNaNCheck: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.nanCheck", "true").toBoolean

  // enable or disable hashcompare in hashjoins or hashagg
  val hashCompare: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.hashCompare", "true").toBoolean

  // enable or disable columnar BroadcastHashJoin
  val enableColumnarBroadcastJoin: Boolean =
    conf.getConfString("spark.gluten.sql.columnar.broadcastJoin", "true").toBoolean && enableCpu

  // enable or disable columnar columnar arrow udf
  val enableColumnarArrowUDF: Boolean = conf.getConfString(
    "spark.gluten.sql.columnar.arrowudf", "true").toBoolean && enableCpu

  // enable or disable columnar wholestage transform
  val enableColumnarWholeStageTransform: Boolean = conf.getConfString(
    "spark.gluten.sql.columnar.wholestagetransform", "true").toBoolean && enableCpu

  // enable or disable columnar exchange
  val enableColumnarShuffle: Boolean = conf
    .getConfString("spark.shuffle.manager", "sort")
    .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager") && enableCpu

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

  // This config is used for deciding whether to load Arrow and Gandiva libraries from
  // the native library. If the native library does not depend on Arrow and Gandiva,
  // this config should will set as false.
  val loadArrow: Boolean =
    conf.getConfString(GlutenConfig.GLUTEN_LOAD_ARROW, "true").toBoolean

  // This config is used for specifying the name of the native library.
  val nativeLibName: String =
    conf.getConfString(GlutenConfig.GLUTEN_LIB_NAME, "spark_columnar_jni")

  // This config is used for specifying the absolute path of the native library.
  val nativeLibPath: String =
    conf.getConfString(GlutenConfig.GLUTEN_LIB_PATH, "")

  // customized backend library name
  val glutenBackendLib: String =
    conf.getConfString(GlutenConfig.GLUTEN_BACKEND_LIB, "")

  val isVeloxBackend: Boolean =
    glutenBackendLib.equalsIgnoreCase(GlutenConfig.GLUTEN_VELOX_BACKEND)

  val isClickHouseBackend: Boolean =
    glutenBackendLib.equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)

  val isGazelleBackend: Boolean =
    glutenBackendLib.equalsIgnoreCase(GlutenConfig.GLUTEN_GAZELLE_BACKEND)

  // fallback to row operators if there are several continous joins
  val joinOptimizationThrottle: Integer =
    conf.getConfString("spark.gluten.sql.columnar.joinOptimizationLevel", "12").toInt

  val batchSize: Int =
    conf.getConfString("spark.sql.execution.arrow.maxRecordsPerBatch", "10000").toInt

  // enable or disable metrics in columnar wholestagecodegen operator
  val enableMetricsTime: Boolean =
    conf.getConfString(
      "spark.gluten.sql.columnar.wholestagecodegen.breakdownTime",
      "false").toBoolean

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

  // The supported customized compression codec is lz4 and fastpfor.
  val columnarShuffleUseCustomizedCompressionCodec: String =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "lz4")

  val columnarShuffleBatchCompressThreshold: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffle.batchCompressThreshold", "100").toInt

  val shuffleSplitDefaultSize: Int =
    conf.getConfString("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192").toInt

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

}

object GlutenConfig {

  val GLUTEN_LOAD_NATIVE = "spark.gluten.sql.columnar.loadnative"
  val GLUTEN_LIB_NAME = "spark.gluten.sql.columnar.libname"
  val GLUTEN_LIB_PATH = "spark.gluten.sql.columnar.libpath"
  val GLUTEN_LOAD_ARROW = "spark.gluten.sql.columnar.loadarrow"
  val GLUTEN_BACKEND_LIB = "spark.gluten.sql.columnar.backend.lib"
  val GLUTEN_VELOX_BACKEND = "velox"
  val GLUTEN_CLICKHOUSE_BACKEND = "ch"
  val GLUTEN_GAZELLE_BACKEND = "gazelle_cpp"

  var ins: GlutenConfig = null
  var random_temp_dir_path: String = null

  /**
   * @deprecated We should avoid caching this value in entire JVM. us
   */
  @Deprecated
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
  def setRandomTempDir(path: String): Unit = synchronized {
    random_temp_dir_path = path
  }
  def getRandomTempDir: String = synchronized {
    random_temp_dir_path
  }
}
