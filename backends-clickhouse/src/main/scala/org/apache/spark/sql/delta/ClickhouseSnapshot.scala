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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression, Predicate}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.DeltaScan
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}

import com.google.common.base.Objects
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path

import java.util.concurrent.TimeUnit
case class AddFileAsKey(addFile: AddFile) {
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: AddFileAsKey => that.addFile == this.addFile
      case _ => false
    }
  }

  override def hashCode(): Int = {
    addFile.path.hashCode
  }
}

case class FilterExprsAsKey(
    path: Path,
    snapshotId: String,
    filters: Seq[Expression],
    limit: Option[Long]) {

  // to transform l_shipdate_912 to l_shiptate_0 so that Attribute reference
  // of same column in different queries can be compared
  private val semanticFilters = filters.map(
    e => {
      Predicate.createInterpreted(
        BindReferences.bindReference(
          e.transform {
            case a: AttributeReference =>
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                a.exprId.copy(id = 0),
                a.qualifier
              )
          },
          Nil,
          allowFailures = true
        )
      )
    })
  override def hashCode(): Int = {
    Objects.hashCode(path, snapshotId, semanticFilters, limit)
  }

  override def equals(o: Any): Boolean = {
    o match {
      case that: FilterExprsAsKey =>
        that.path == this.path &&
        that.snapshotId.equals(this.snapshotId) &&
        that.semanticFilters == this.semanticFilters &&
        that.limit == this.limit
      case _ => false
    }
  }

}

object ClickhouseSnapshot {
  protected def sparkSession: SparkSession = SparkSession.active

  val deltaScanCache: Cache[FilterExprsAsKey, DeltaScan] = CacheBuilder.newBuilder
    .maximumSize(
      sparkSession.conf
        .get(CHBackendSettings.GLUTEN_CLICKHOUSE_DELTA_SCAN_CACHE_SIZE, "10000")
        .toLong)
    .expireAfterAccess(7200L, TimeUnit.SECONDS)
    .recordStats()
    .build()

  val addFileToAddMTPCache: LoadingCache[AddFileAsKey, AddMergeTreeParts] = CacheBuilder.newBuilder
    .maximumSize(
      sparkSession.conf
        .get(CHBackendSettings.GLUTEN_CLICKHOUSE_ADDFILES_TO_MTPS_CACHE_SIZE, "1000000")
        .toLong)
    .expireAfterAccess(7200L, TimeUnit.SECONDS)
    .recordStats
    .build[AddFileAsKey, AddMergeTreeParts](new CacheLoader[AddFileAsKey, AddMergeTreeParts]() {
      @throws[Exception]
      override def load(key: AddFileAsKey): AddMergeTreeParts = {
        AddFileTags.addFileToAddMergeTreeParts(key.addFile)
      }
    })

  val pathToAddMTPCache: Cache[String, AddMergeTreeParts] = CacheBuilder.newBuilder
    .maximumSize(
      sparkSession.conf
        .get(CHBackendSettings.GLUTEN_CLICKHOUSE_TABLE_PATH_TO_MTPS_CACHE_SIZE, "1000000")
        .toLong)
    .expireAfterAccess(7200L, TimeUnit.SECONDS)
    .recordStats()
    .build()

  def clearAllFileStatusCache(): Unit = {
    addFileToAddMTPCache.invalidateAll()
    pathToAddMTPCache.invalidateAll()
    deltaScanCache.invalidateAll()
  }

  // use timestamp + version as the snapshot id for ch backend
  def genSnapshotId(snapshot: Snapshot): String = {
    // When CTAS, there is no latest timestamp in the Snapshot
    s"${snapshot.metadata.createdTime.getOrElse(System.currentTimeMillis())}_${snapshot.version}"
  }
}
