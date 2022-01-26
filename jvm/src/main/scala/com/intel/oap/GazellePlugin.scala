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

package com.intel.oap

import java.util.{Collections, Objects}

import scala.language.implicitConversions

import com.intel.oap.GazellePlugin.{GAZELLE_SESSION_EXTENSION_NAME, SPARK_SESSION_EXTS_KEY}
import com.intel.oap.extension.{ColumnarOverrides, StrategyOverrides}
import com.intel.oap.vectorized.ExpressionEvaluator
import java.util
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.internal.StaticSQLConf

class GazellePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GazelleDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GazelleExecutorPlugin()
  }
}

private[oap] class GazelleDriverPlugin extends DriverPlugin {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val conf = pluginContext.conf()
    setPredefinedConfigs(conf)
    Collections.emptyMap()
  }

  def setPredefinedConfigs(conf: SparkConf): Unit = {
    if (conf.contains(SPARK_SESSION_EXTS_KEY)) {
      throw new IllegalArgumentException("Spark extensions are already specified before " +
          "enabling Gazelle plugin: " + conf.get(GazellePlugin.SPARK_SESSION_EXTS_KEY))
    }
    conf.set(SPARK_SESSION_EXTS_KEY, GAZELLE_SESSION_EXTENSION_NAME)
  }
}

private[oap] class GazelleExecutorPlugin extends ExecutorPlugin {
  /**
   * Initialize the executor plugin.
   */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // SQLConf is not initialed here, so it can not use 'GazelleJniConfig.getConf' to get conf.
    if (ctx.conf().getBoolean(GazelleJniConfig.OAP_LOAD_NATIVE, true)) {
      val initKernel = new ExpressionEvaluator(java.util.Collections.emptyList[String],
        ctx.conf().get(GazelleJniConfig.OAP_LIB_NAME, "spark_columnar_jni"),
        ctx.conf().get(GazelleJniConfig.OAP_LIB_PATH, ""),
        ctx.conf().getBoolean(GazelleJniConfig.OAP_LOAD_ARROW, true))
      initKernel.initNative()
    }
  }

  /**
   * Clean up and terminate this plugin.
   * For example: close the native engine.
   */
  override def shutdown(): Unit = {
    super.shutdown()
  }
}

private[oap] class GazelleSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    GazellePlugin.DEFAULT_INJECTORS.foreach(injector => injector.inject(exts))
  }
}

private[oap] class SparkConfImplicits(conf: SparkConf) {
  def enableGazellePlugin(): SparkConf = {
    if (conf.contains(GazellePlugin.SPARK_SQL_PLUGINS_KEY)) {
      throw new IllegalArgumentException("A Spark plugin is already specified before enabling " +
          "Gazelle plugin: " + conf.get(GazellePlugin.SPARK_SQL_PLUGINS_KEY))
    }
    conf.set(GazellePlugin.SPARK_SQL_PLUGINS_KEY, GazellePlugin.GAZELLE_PLUGIN_NAME)
  }
}

private[oap] trait GazelleSparkExtensionsInjector {
  def inject(extensions: SparkSessionExtensions)
}

private[oap] object GazellePlugin {
  // To enable GazellePlugin in production, set "spark.plugins=com.intel.oap.GazellePlugin"
  val SPARK_SQL_PLUGINS_KEY: String = "spark.plugins"
  val GAZELLE_PLUGIN_NAME: String = Objects.requireNonNull(classOf[GazellePlugin]
      .getCanonicalName)
  val SPARK_SESSION_EXTS_KEY: String = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  val GAZELLE_SESSION_EXTENSION_NAME: String = Objects.requireNonNull(
    classOf[GazelleSessionExtensions].getCanonicalName)

  /**
   * Specify all injectors that Gazelle is using in following list.
   */
  val DEFAULT_INJECTORS: List[GazelleSparkExtensionsInjector] = List(
    ColumnarOverrides,
    StrategyOverrides
  )

  implicit def sparkConfImplicit(conf: SparkConf): SparkConfImplicits = {
    new SparkConfImplicits(conf)
  }
}
