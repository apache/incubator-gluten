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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata

import org.apache.spark.sql.delta.actions.AddFile

case class AddMergeTreeParts(
    database: String,
    table: String,
    engine: String,
    path: String,
    targetNode: String,
    name: String,
    uuid: String,
    rows: Long,
    bytesOnDisk: Long,
    dataCompressedBytes: Long,
    dataUncompressedBytes: Long,
    modificationTime: Long,
    partitionId: String,
    minBlockNumber: Long,
    maxBlockNumber: Long,
    level: Int,
    dataVersion: Long,
    bucketNum: String,
    dirName: String,
    dataChange: Boolean,
    partition: String = "",
    defaultCompressionCodec: String = "LZ4",
    stats: String = "",
    partitionValues: Map[String, String] = Map.empty[String, String],
    partType: String = "Wide",
    active: Int = 1,
    marks: Long = -1L,
    marksBytes: Long = -1L,
    removeTime: Long = -1L,
    refcount: Int = -1,
    minDate: Int = -1,
    maxDate: Int = -1,
    minTime: Long = -1L,
    maxTime: Long = -1L,
    primaryKeyBytesInMemory: Long = -1L,
    primaryKeyBytesInMemoryAllocated: Long = -1L,
    isFrozen: Int = 0,
    diskName: String = "default",
    hashOfAllFiles: String = "",
    hashOfUncompressedFiles: String = "",
    uncompressedHashOfCompressedFiles: String = "",
    deleteTtlInfoMin: Long = -1L,
    deleteTtlInfoMax: Long = -1L,
    moveTtlInfoExpression: String = "",
    moveTtlInfoMin: Long = -1L,
    moveTtlInfoMax: Long = -1L,
    recompressionTtlInfoExpression: String = "",
    recompressionTtlInfoMin: Long = -1L,
    recompressionTtlInfoMax: Long = -1L,
    groupByTtlInfoExpression: String = "",
    groupByTtlInfoMin: Long = -1L,
    groupByTtlInfoMax: Long = -1L,
    rowsWhereTtlInfoExpression: String = "",
    rowsWhereTtlInfoMin: Long = -1L,
    rowsWhereTtlInfoMax: Long = -1L)

object AddFileTags {
  // scalastyle:off argcount
  def partsInfoToAddFile(
      database: String,
      table: String,
      engine: String,
      path: String,
      targetNode: String,
      name: String,
      uuid: String,
      rows: Long,
      bytesOnDisk: Long,
      dataCompressedBytes: Long,
      dataUncompressedBytes: Long,
      modificationTime: Long,
      partitionId: String,
      minBlockNumber: Long,
      maxBlockNumber: Long,
      level: Int,
      dataVersion: Long,
      bucketNum: String,
      dirName: String,
      dataChange: Boolean,
      partition: String = "",
      defaultCompressionCodec: String = "LZ4",
      stats: String = "",
      partitionValues: Map[String, String] = Map.empty[String, String]): AddFile = {
    // scalastyle:on argcount
    val tags = Map[String, String](
      "database" -> database,
      "table" -> table,
      "engine" -> engine,
      "path" -> path,
      "targetNode" -> targetNode,
      "partition" -> partition,
      "uuid" -> uuid,
      "rows" -> rows.toString,
      "bytesOnDisk" -> bytesOnDisk.toString,
      "dataCompressedBytes" -> dataCompressedBytes.toString,
      "dataUncompressedBytes" -> dataUncompressedBytes.toString,
      "modificationTime" -> modificationTime.toString,
      "partitionId" -> partitionId,
      "minBlockNumber" -> minBlockNumber.toString,
      "maxBlockNumber" -> maxBlockNumber.toString,
      "level" -> level.toString,
      "dataVersion" -> dataVersion.toString,
      "defaultCompressionCodec" -> defaultCompressionCodec,
      "bucketNum" -> bucketNum,
      "dirName" -> dirName
    )
    AddFile(name, partitionValues, bytesOnDisk, modificationTime, dataChange, stats, tags)
  }

  def partsMapToParts(addFile: AddFile): AddMergeTreeParts = {
    assert(addFile.tags != null && !addFile.tags.isEmpty)
    AddMergeTreeParts(
      addFile.tags.get("database").get,
      addFile.tags.get("table").get,
      addFile.tags.get("engine").get,
      addFile.tags.get("path").get,
      addFile.tags.get("targetNode").get,
      addFile.path,
      addFile.tags.get("uuid").get,
      addFile.tags.get("rows").get.toLong,
      addFile.size,
      addFile.tags.get("dataCompressedBytes").get.toLong,
      addFile.tags.get("dataUncompressedBytes").get.toLong,
      addFile.modificationTime,
      addFile.tags.get("partitionId").get,
      addFile.tags.get("minBlockNumber").get.toLong,
      addFile.tags.get("maxBlockNumber").get.toLong,
      addFile.tags.get("level").get.toInt,
      addFile.tags.get("dataVersion").get.toLong,
      addFile.tags.get("bucketNum").get,
      addFile.tags.get("dirName").get,
      addFile.dataChange,
      addFile.tags.get("partition").get,
      addFile.tags.get("defaultCompressionCodec").get,
      addFile.stats,
      addFile.partitionValues
    )
  }
}
