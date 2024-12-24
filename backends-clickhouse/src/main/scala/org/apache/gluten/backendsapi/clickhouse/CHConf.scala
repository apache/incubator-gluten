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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

object CHConf {
  private[clickhouse] val BACKEND_NAME: String = "ch"
  private[clickhouse] val CONF_PREFIX: String = GlutenConfig.prefixOf(BACKEND_NAME)
  private val RUNTIME_SETTINGS: String = s"$CONF_PREFIX.runtime_settings"
  private val RUNTIME_CONFIG = s"$CONF_PREFIX.runtime_config"
  implicit class GlutenCHConf(conf: SparkConf) {
    def setCHSettings(settings: (String, String)*): SparkConf = {
      settings.foreach { case (k, v) => conf.set(runtimeSettings(k), v) }
      conf
    }

    def setCHSettings[T](k: String, v: T): SparkConf = {
      conf.set(runtimeSettings(k), v.toString)
      conf
    }

    def setCHConfig(config: (String, String)*): SparkConf = {
      config.foreach { case (k, v) => conf.set(runtimeConfig(k), v) }
      conf
    }

    def setCHConfig[T](k: String, v: T): SparkConf = {
      conf.set(runtimeConfig(k), v.toString)
      conf
    }
  }

  /** CH configuration prefix at Java side */
  def prefixOf(key: String): String = s"$CONF_PREFIX.$key"
  def runtimeConfig(key: String): String = s"$RUNTIME_CONFIG.$key"
  def runtimeSettings(key: String): String = s"$RUNTIME_SETTINGS.$key"

  def startWithSettingsPrefix(key: String): Boolean = key.startsWith(RUNTIME_SETTINGS)
  def removeSettingsPrefix(key: String): String = key.substring(RUNTIME_SETTINGS.length + 1)

  def get: CHConf = new CHConf(SQLConf.get)

  import SQLConf._

  val ENABLE_ONEPIPELINE_MERGETREE_WRITE =
    buildConf(prefixOf("mergetree.write.pipeline"))
      .doc("Using one pipeline to write data to MergeTree table in Spark 3.5")
      .booleanConf
      .createWithDefault(false)
}

class CHConf(conf: SQLConf) extends GlutenConfig(conf) {
  def enableOnePipelineMergeTreeWrite: Boolean =
    conf.getConf(CHConf.ENABLE_ONEPIPELINE_MERGETREE_WRITE)
}
