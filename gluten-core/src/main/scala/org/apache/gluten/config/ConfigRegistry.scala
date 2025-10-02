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

import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

trait ConfigRegistry {
  private val configEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def register(entry: ConfigEntry[_]): Unit = {
    val existing = configEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  /** Visible for testing. */
  private[config] def allEntries: Seq[ConfigEntry[_]] = {
    configEntries.values.toSeq
  }

  protected def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate {
      entry =>
        register(entry)
        ConfigRegistry.registerToAllEntries(entry)
    }
  }

  protected def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate {
      entry =>
        SQLConf.registerStaticConfigKey(key)
        register(entry)
        ConfigRegistry.registerToAllEntries(entry)
    }
  }

  def get: GlutenCoreConfig
}

object ConfigRegistry {
  private val allConfigEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def registerToAllEntries(entry: ConfigEntry[_]): Unit = {
    val existing = allConfigEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  def containsEntry(entry: ConfigEntry[_]): Boolean = {
    allConfigEntries.contains(entry.key)
  }

  def findEntry(key: String): Option[ConfigEntry[_]] = allConfigEntries.get(key)
}
