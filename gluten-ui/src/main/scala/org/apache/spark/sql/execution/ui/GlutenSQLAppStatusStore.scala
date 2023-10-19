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
package org.apache.spark.sql.execution.ui

import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVIndex, KVStore, KVStoreView}

import com.fasterxml.jackson.annotation.JsonIgnore

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * Provides a view of a KVStore with methods that make it easy to query SQL-specific state. There's
 * no state kept in this class, so it's ok to have multiple instances of it in an application.
 */
class GlutenSQLAppStatusStore(store: KVStore) {

  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator())(iter => iter.asScala.toList)
  }

  def buildInfo(): GlutenBuildInfoUIData = {
    val kClass = classOf[GlutenBuildInfoUIData]
    store.read(kClass, kClass.getName)
  }

  def executionsList(): Seq[GlutenSQLExecutionUIData] = {
    viewToSeq(store.view(classOf[GlutenSQLExecutionUIData]))
  }

  def executionsList(offset: Int, length: Int): Seq[GlutenSQLExecutionUIData] = {
    viewToSeq(store.view(classOf[GlutenSQLExecutionUIData]).skip(offset).max(length))
  }

  def execution(executionId: Long): Option[GlutenSQLExecutionUIData] = {
    try {
      Some(store.read(classOf[GlutenSQLExecutionUIData], executionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def executionsCount(): Long = {
    store.count(classOf[GlutenSQLExecutionUIData])
  }
}

class GlutenSQLExecutionUIData(
    @KVIndexParam val executionId: Long,
    val description: String,
    val numGlutenNodes: Int,
    val numFallbackNodes: Int,
    val fallbackDescription: String,
    val fallbackNodeToReason: Seq[(String, String)]) {}

class GlutenBuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[GlutenBuildInfoUIData].getName()
}
