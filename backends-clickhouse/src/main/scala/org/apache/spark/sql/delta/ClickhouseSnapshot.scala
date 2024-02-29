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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

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

object ClickhouseSnapshot {
  val fileStatusCache: LoadingCache[AddFileAsKey, AddMergeTreeParts] = CacheBuilder.newBuilder
    .maximumSize(100000)
    .expireAfterAccess(3600L, TimeUnit.SECONDS)
    .recordStats
    .build[AddFileAsKey, AddMergeTreeParts](new CacheLoader[AddFileAsKey, AddMergeTreeParts]() {
      @throws[Exception]
      override def load(key: AddFileAsKey): AddMergeTreeParts = {
        AddFileTags.addFileToAddMergeTreeParts(key.addFile)
      }
    })
  def clearAllFileStatusCache: Unit = fileStatusCache.invalidateAll()
}
