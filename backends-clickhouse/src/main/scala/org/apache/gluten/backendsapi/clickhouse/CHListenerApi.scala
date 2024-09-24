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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.ListenerApi
import org.apache.gluten.execution.CHBroadcastBuildSideCache
import org.apache.gluten.execution.datasource.{GlutenOrcWriterInjects, GlutenParquetWriterInjects, GlutenRowSplitter}
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.jni.JniLibLoader
import org.apache.gluten.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.listener.CHGlutenSQLAppStatusListener
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{GlutenDriverEndpoint, GlutenExecutorEndpoint}
import org.apache.spark.sql.execution.datasources.v1._
import org.apache.spark.sql.utils.ExpressionUtil
import org.apache.spark.util.SparkDirectoryUtil

import org.apache.commons.lang3.StringUtils

import java.util.TimeZone

class CHListenerApi extends ListenerApi with Logging {

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    GlutenDriverEndpoint.glutenDriverEndpointRef = (new GlutenDriverEndpoint).self
    CHGlutenSQLAppStatusListener.registerListener(sc)
    initialize(pc.conf, isDriver = true)

    val expressionExtensionTransformer = ExpressionUtil.extendedExpressionTransformer(
      pc.conf.get(GlutenConfig.GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF, "")
    )
    if (expressionExtensionTransformer != null) {
      ExpressionExtensionTrait.expressionExtensionTransformer = expressionExtensionTransformer
    }
  }

  override def onDriverShutdown(): Unit = shutdown()

  override def onExecutorStart(pc: PluginContext): Unit = {
    GlutenExecutorEndpoint.executorEndpoint = new GlutenExecutorEndpoint(pc.executorID, pc.conf)
    if (pc.conf().get("spark.master").startsWith("local")) {
      logDebug("Skipping duplicate initializing clickhouse backend on spark local mode")
    } else {
      initialize(pc.conf, isDriver = false)
    }
  }

  override def onExecutorShutdown(): Unit = shutdown()

  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    SparkDirectoryUtil.init(conf)
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable clickhouse backend")
    }
    if (isDriver) {
      JniLibLoader.loadFromPath(libPath, true)
    } else {
      val executorLibPath = conf.get(GlutenConfig.GLUTEN_EXECUTOR_LIB_PATH, libPath)
      JniLibLoader.loadFromPath(executorLibPath, true)
    }
    // Add configs
    import org.apache.gluten.backendsapi.clickhouse.CHConf._
    conf.setCHConfig(
      "timezone" -> conf.get("spark.sql.session.timeZone", TimeZone.getDefault.getID),
      "local_engine.settings.log_processors_profiles" -> "true")
    conf.setCHSettings("spark_version", SPARK_VERSION)
    // add memory limit for external sort
    val externalSortKey = CHConf.runtimeSettings("max_bytes_before_external_sort")
    if (conf.getLong(externalSortKey, -1) < 0) {
      if (conf.getBoolean("spark.memory.offHeap.enabled", defaultValue = false)) {
        val memSize = JavaUtils.byteStringAsBytes(conf.get("spark.memory.offHeap.size"))
        if (memSize > 0L) {
          val cores = conf.getInt("spark.executor.cores", 1).toLong
          val sortMemLimit = ((memSize / cores) * 0.8).toLong
          logDebug(s"max memory for sorting: $sortMemLimit")
          conf.set(externalSortKey, sortMemLimit.toString)
        }
      }
    }

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    CHNativeExpressionEvaluator.initNative(conf)

    // inject backend-specific implementations to override spark classes
    // FIXME: The following set instances twice in local mode?
    GlutenParquetWriterInjects.setInstance(new CHParquetWriterInjects())
    GlutenOrcWriterInjects.setInstance(new CHOrcWriterInjects())
    GlutenMergeTreeWriterInjects.setInstance(new CHMergeTreeWriterInjects())
    GlutenRowSplitter.setInstance(new CHRowSplitter())
  }

  private def shutdown(): Unit = {
    CHBroadcastBuildSideCache.cleanAll()
    CHNativeExpressionEvaluator.finalizeNative()
  }
}
