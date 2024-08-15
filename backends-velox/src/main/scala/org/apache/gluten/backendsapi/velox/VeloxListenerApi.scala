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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.ListenerApi
import org.apache.gluten.execution.datasource.{GlutenOrcWriterInjects, GlutenParquetWriterInjects, GlutenRowSplitter}
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.init.NativeBackendInitializer
import org.apache.gluten.utils._
import org.apache.gluten.vectorized.{JniLibLoader, JniWorkspace}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.velox.{VeloxOrcWriterInjects, VeloxParquetWriterInjects, VeloxRowSplitter}
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.internal.{GlutenConfigUtil, StaticSQLConf}
import org.apache.spark.util.SparkDirectoryUtil

import org.apache.commons.lang3.StringUtils

class VeloxListenerApi extends ListenerApi with Logging {
  import VeloxListenerApi._

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    val conf = pc.conf()
    // sql table cache serializer
    if (conf.getBoolean(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, defaultValue = false)) {
      conf.set(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        "org.apache.spark.sql.execution.ColumnarCachedBatchSerializer")
    }
    SparkDirectoryUtil.init(conf)
    UDFResolver.resolveUdfConf(conf, isDriver = true)
    initialize(conf)
  }

  override def onDriverShutdown(): Unit = shutdown()

  override def onExecutorStart(pc: PluginContext): Unit = {
    val conf = pc.conf
    if (inLocalMode(conf)) {
      // Don't do static initializations from executor side in local mode.
      // Driver already did that.
      logInfo(
        "Gluten is running with Spark local mode. Skip running static initializer for executor.")
      return
    }
    SparkDirectoryUtil.init(conf)
    UDFResolver.resolveUdfConf(conf, isDriver = false)
    initialize(conf)
  }

  override def onExecutorShutdown(): Unit = shutdown()

  private def initialize(conf: SparkConf): Unit = {
    if (conf.getBoolean(GlutenConfig.GLUTEN_DEBUG_KEEP_JNI_WORKSPACE, defaultValue = false)) {
      val debugDir = conf.get(GlutenConfig.GLUTEN_DEBUG_KEEP_JNI_WORKSPACE_DIR)
      JniWorkspace.enableDebug(debugDir)
    }
    val loader = JniWorkspace.getDefault.libLoader

    val osName = System.getProperty("os.name")
    if (osName.startsWith("Mac OS X") || osName.startsWith("macOS")) {
      loadLibWithMacOS(loader)
    } else {
      loadLibWithLinux(conf, loader)
    }

    // Set the system properties.
    // Use appending policy for children with the same name in a arrow struct vector.
    System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, false)
    } else {
      val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "gluten")
      loader.mapAndLoad(baseLibName, false)
      loader.mapAndLoad(VeloxBackend.BACKEND_NAME, false)
    }

    val parsed = GlutenConfigUtil.parseConfig(conf.getAll.toMap)
    NativeBackendInitializer.initializeBackend(parsed)

    // inject backend-specific implementations to override spark classes
    // FIXME: The following set instances twice in local mode?
    GlutenParquetWriterInjects.setInstance(new VeloxParquetWriterInjects())
    GlutenOrcWriterInjects.setInstance(new VeloxOrcWriterInjects())
    GlutenRowSplitter.setInstance(new VeloxRowSplitter())
  }

  private def shutdown(): Unit = {
    // TODO shutdown implementation in velox to release resources
  }
}

object VeloxListenerApi {
  // See org.apache.spark.SparkMasterRegex
  private val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
  private val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r

  private def inLocalMode(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master")
    master match {
      case "local" => true
      case LOCAL_N_REGEX(_) => true
      case LOCAL_N_FAILURES_REGEX(_, _) => true
      case _ => false
    }
  }

  private def loadLibFromJar(load: JniLibLoader, conf: SparkConf): Unit = {
    val loader = SharedLibraryLoader.find(conf)
    loader.loadLib(load)
  }

  private def loadLibWithLinux(conf: SparkConf, loader: JniLibLoader): Unit = {
    if (
      conf.getBoolean(
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR,
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT)
    ) {
      loadLibFromJar(loader, conf)
    }
  }

  private def loadLibWithMacOS(loader: JniLibLoader): Unit = {
    // Placeholder for loading shared libs on MacOS if user needs.
  }
}
