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
package io.glutenproject.execution

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.vectorized.StorageJoinBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation}

import org.sparkproject.guava.cache.{Cache, CacheBuilder, RemovalNotification}

import java.util.concurrent.TimeUnit

object CHBroadcastBuildSideCache extends Logging {

  private lazy val expiredTime = SparkEnv.get.conf.getLong(
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME,
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT
  )

  // Use for controling to build bhj hash table once.
  // key: hashtable id, value is hashtable backend pointer(long to string).
  private val buildSideRelationCache: Cache[String, String] =
    CacheBuilder.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(
        (notification: RemovalNotification[String, String]) => {
          cleanBuildHashTable(notification.getKey, notification.getValue.toLong)
        })
      .build[String, String]()

  def getOrBuildBroadcastHashTable(
      broadcast: Broadcast[BuildSideRelation],
      broadCastContext: BroadCastHashJoinContext): String = {
    buildSideRelationCache
      .get(
        broadCastContext.buildHashTableId,
        () => {
          val bsr = broadcast.value.asReadOnlyCopy(broadCastContext)
          bsr.asInstanceOf[ClickHouseBuildSideRelation].hashTableData.toString
        }
      )
  }

  def invalidateBroadcastHashtable(broadcastHashtableId: String): Unit = {
    val v = buildSideRelationCache.getIfPresent(broadcastHashtableId)
    if (v != null) {
      // Cleanup operations on the backend are idempotent.
      buildSideRelationCache.invalidate(broadcastHashtableId)
    }
  }

  private def cleanBuildHashTable(key: String, value: Long): Unit = {
    StorageJoinBuilder.nativeCleanBuildHashTable(key, value)
    logTrace(
      s"Clean build hash table $key success." +
        s"Cache size now is ${buildSideRelationCache.size()}")
  }
}
