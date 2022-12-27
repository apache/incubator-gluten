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

import java.util
import java.util.{Collections, Objects}
import scala.language.implicitConversions
import com.google.protobuf.Any
import io.glutenproject.GlutenPlugin.{GLUTEN_SESSION_EXTENSION_NAME, SPARK_SESSION_EXTS_KEY}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.{ColumnarOverrides, ColumnarQueryStagePrepOverrides, OthersExtensionOverrides, StrategyOverrides}
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.vectorized.NativeExpressionEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[glutenproject] class GlutenDriverPlugin extends DriverPlugin {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val conf = pluginContext.conf()
    // Initialize Backends API
    BackendsApiManager.initialize()
    BackendsApiManager.getInitializerApiInstance.initialize(conf)
    setPredefinedConfigs(conf)
    Collections.emptyMap()
  }

  def setPredefinedConfigs(conf: SparkConf): Unit = {
    val extensions = if (conf.contains(SPARK_SESSION_EXTS_KEY)) {
      s"${conf.get(SPARK_SESSION_EXTS_KEY)},${GLUTEN_SESSION_EXTENSION_NAME}"
    } else {
      s"${GLUTEN_SESSION_EXTENSION_NAME}"
    }
    conf.set(SPARK_SESSION_EXTS_KEY, String.format("%s", extensions))
    if (BackendsApiManager.getSettings.disableVanillaColumnarReaders()) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue was fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set("spark.sql.parquet.enableVectorizedReader", "false")
      conf.set("spark.sql.orc.enableVectorizedReader", "false")
      conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")
    }
  }
}

private[glutenproject] class GlutenExecutorPlugin extends ExecutorPlugin {
  /**
   * Initialize the executor plugin.
   */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    val conf = ctx.conf()
    // Must set the 'spark.memory.offHeap.size' value to native memory malloc
    if (!conf.getBoolean("spark.memory.offHeap.enabled", false) ||
      (JavaUtils.byteStringAsBytes(
        conf.get("spark.memory.offHeap.size").toString) / 1024 / 1024).toInt <= 0) {
      throw new IllegalArgumentException(s"Must set the 'spark.memory.offHeap.enabled' to true" +
        s" and set the off heap memory size of the 'spark.memory.offHeap.size'")
    }
    // Initialize Backends API
    BackendsApiManager.initialize()
    BackendsApiManager.getInitializerApiInstance.initialize(conf)
  }

  /**
   * Clean up and terminate this plugin.
   * For example: close the native engine.
   */
  override def shutdown(): Unit = {
    super.shutdown()
  }
}

private[glutenproject] class GlutenSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    GlutenPlugin.DEFAULT_INJECTORS.foreach(injector => injector.inject(exts))
  }
}

private[glutenproject] class SparkConfImplicits(conf: SparkConf) {
  def enableGlutenPlugin(): SparkConf = {
    if (conf.contains(GlutenPlugin.SPARK_SQL_PLUGINS_KEY)) {
      throw new IllegalArgumentException("A Spark plugin is already specified before enabling " +
        "Gluten plugin: " + conf.get(GlutenPlugin.SPARK_SQL_PLUGINS_KEY))
    }
    conf.set(GlutenPlugin.SPARK_SQL_PLUGINS_KEY, GlutenPlugin.GLUTEN_PLUGIN_NAME)
  }
}

private[glutenproject] trait GlutenSparkExtensionsInjector {
  def inject(extensions: SparkSessionExtensions)
}

private[glutenproject] object GlutenPlugin {
  // To enable GlutenPlugin in production, set "spark.plugins=io.glutenproject.GlutenPlugin"
  val SPARK_SQL_PLUGINS_KEY: String = "spark.plugins"
  val GLUTEN_PLUGIN_NAME: String = Objects.requireNonNull(classOf[GlutenPlugin]
    .getCanonicalName)
  val SPARK_SESSION_EXTS_KEY: String = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  val GLUTEN_SESSION_EXTENSION_NAME: String = Objects.requireNonNull(
    classOf[GlutenSessionExtensions].getCanonicalName)

  /**
   * Specify all injectors that Gluten is using in following list.
   */
  val DEFAULT_INJECTORS: List[GlutenSparkExtensionsInjector] = List(
    ColumnarQueryStagePrepOverrides,
    ColumnarOverrides,
    StrategyOverrides,
    OthersExtensionOverrides
  )

  implicit def sparkConfImplicit(conf: SparkConf): SparkConfImplicits = {
    new SparkConfImplicits(conf)
  }

  def buildNativeConfNode(conf: SparkConf): PlanNode = {
    val nativeConfMap = new util.HashMap[String, String]()

    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_STRIPE_SIZE)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_STRIPE_SIZE,
        conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_STRIPE_SIZE))
    }
    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_ROW_INDEX_STRIDE,
        conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE))
    }
    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_COMPRESS)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_COMPRESS, conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_COMPRESS))
    }

    // S3 config
    nativeConfMap.put(
      GlutenConfig.SPARK_S3_ACCESS_KEY, conf.get(GlutenConfig.SPARK_S3_ACCESS_KEY, "minio"))
    nativeConfMap.put(
      GlutenConfig.SPARK_S3_SECRET_KEY, conf.get(GlutenConfig.SPARK_S3_SECRET_KEY, "miniopass"))
    nativeConfMap.put(
      GlutenConfig.SPARK_S3_ENDPOINT, conf.get(GlutenConfig.SPARK_S3_ENDPOINT, "localhost:9000"))
    nativeConfMap.put(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED,
      conf.get(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED, "false"))
    nativeConfMap.put(GlutenConfig.SPARK_S3_PATH_STYLE_ACCESS,
      conf.get(GlutenConfig.SPARK_S3_PATH_STYLE_ACCESS, "true"))
    nativeConfMap.put(GlutenConfig.SPARK_S3_USE_INSTANCE_CREDENTIALS,
      conf.get(GlutenConfig.SPARK_S3_USE_INSTANCE_CREDENTIALS, "false"))

    conf.getAll.filter {
      case (k, v) => k.startsWith(BackendsApiManager.getSettings.getBackendConfigPrefix())
    }.foreach {
      case (k, v) => nativeConfMap.put(k, v)
    }

    nativeConfMap.put(
      SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key,
      conf.get(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "32768"))

    if (conf.contains(GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY)) {
      nativeConfMap.put(
        GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY,
        conf.getSizeAsBytes(GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY).toString)
    }

    nativeConfMap.put(GlutenConfig.GLUTEN_SAVE_DIR, conf.get(GlutenConfig.GLUTEN_SAVE_DIR, ""))

    val stringMapNode = ExpressionBuilder.makeStringMap(nativeConfMap)
    val extensionNode = ExtensionBuilder
      .makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf))
    PlanBuilder.makePlan(extensionNode)
  }
}
