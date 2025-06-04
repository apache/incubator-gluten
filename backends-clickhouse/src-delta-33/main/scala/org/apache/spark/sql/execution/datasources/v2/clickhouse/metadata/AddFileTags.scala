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

import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor}
import org.apache.spark.sql.delta.util.MergeTreePartitionUtils
import org.apache.spark.sql.execution.datasources.clickhouse.WriteReturnedMetric

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path

import java.util.{List => JList}

import scala.collection.JavaConverters._

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class AddMergeTreeParts(
    val database: String,
    val table: String,
    val engine: String, // default is "MergeTree"
    val tablePath: String, // table path
    val targetNode: String, // the node which the current part is generated
    val name: String, // part name
    val uuid: String,
    val rows: Long, // row count
    override val size: Long, // the size of the part
    val dataCompressedBytes: Long,
    val dataUncompressedBytes: Long,
    override val modificationTime: Long,
    val partitionId: String,
    val minBlockNumber: Long,
    val maxBlockNumber: Long,
    val level: Int,
    val dataVersion: Long,
    val bucketNum: String,
    val dirName: String,
    override val dataChange: Boolean,
    val partition: String = "",
    val defaultCompressionCodec: String = "LZ4",
    override val stats: String = "",
    override val partitionValues: Map[String, String] = Map.empty[String, String],
    val partType: String = "Wide",
    val active: Int = 1,
    val marks: Long = -1L, // mark count
    val marksBytes: Long = -1L,
    val removeTime: Long = -1L,
    val refcount: Int = -1,
    val minDate: Int = -1,
    val maxDate: Int = -1,
    val minTime: Long = -1L,
    val maxTime: Long = -1L,
    val primaryKeyBytesInMemory: Long = -1L,
    val primaryKeyBytesInMemoryAllocated: Long = -1L,
    val isFrozen: Int = 0,
    val diskName: String = "default",
    val hashOfAllFiles: String = "",
    val hashOfUncompressedFiles: String = "",
    val uncompressedHashOfCompressedFiles: String = "",
    val deleteTtlInfoMin: Long = -1L,
    val deleteTtlInfoMax: Long = -1L,
    val moveTtlInfoExpression: String = "",
    val moveTtlInfoMin: Long = -1L,
    val moveTtlInfoMax: Long = -1L,
    val recompressionTtlInfoExpression: String = "",
    val recompressionTtlInfoMin: Long = -1L,
    val recompressionTtlInfoMax: Long = -1L,
    val groupByTtlInfoExpression: String = "",
    val groupByTtlInfoMin: Long = -1L,
    val groupByTtlInfoMax: Long = -1L,
    val rowsWhereTtlInfoExpression: String = "",
    val rowsWhereTtlInfoMin: Long = -1L,
    val rowsWhereTtlInfoMax: Long = -1L,
    override val tags: Map[String, String] = null,
    override val deletionVector: DeletionVectorDescriptor = null)
  extends AddFile(
    name,
    partitionValues,
    size,
    modificationTime,
    dataChange,
    stats,
    tags,
    deletionVector) {

  def fullPartPath(): String = {
    dirName + "/" + name
  }
}

object AddFileTags {
  // scalastyle:off argcount
  private def partsInfoToAddFile(
      database: String,
      table: String,
      engine: String,
      tablePath: String,
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
      marks: Long = -1L): AddFile = {
    // scalastyle:on argcount
    val tags = Map[String, String](
      "database" -> database,
      "table" -> table,
      "engine" -> engine,
      "path" -> tablePath,
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
      "dirName" -> dirName,
      "marks" -> marks.toString
    )
    val mapper: ObjectMapper = new ObjectMapper()
    val rootNode = mapper.createObjectNode()
    rootNode.put("numRecords", rows)
    rootNode.put("minValues", "")
    rootNode.put("maxValues", "")
    rootNode.put("nullCount", "")
    // Add the `stats` into delta meta log
    val metricsStats = mapper.writeValueAsString(rootNode)
    val uriName = new Path(name).toUri.toString
    AddFile(uriName, partitionValues, bytesOnDisk, modificationTime, dataChange, metricsStats, tags)
  }

  def addFileToAddMergeTreeParts(addFile: AddFile): AddMergeTreeParts = {
    assert(addFile.tags != null && addFile.tags.nonEmpty)
    new AddMergeTreeParts(
      addFile.tags("database"),
      addFile.tags("table"),
      addFile.tags("engine"),
      addFile.tags("path"),
      addFile.tags("targetNode"),
      addFile.path,
      addFile.tags("uuid"),
      addFile.tags("rows").toLong,
      addFile.size,
      addFile.tags("dataCompressedBytes").toLong,
      addFile.tags("dataUncompressedBytes").toLong,
      addFile.modificationTime,
      addFile.tags("partitionId"),
      addFile.tags("minBlockNumber").toLong,
      addFile.tags("maxBlockNumber").toLong,
      addFile.tags("level").toInt,
      addFile.tags("dataVersion").toLong,
      addFile.tags("bucketNum"),
      addFile.tags("dirName"),
      addFile.dataChange,
      addFile.tags("partition"),
      addFile.tags("defaultCompressionCodec"),
      addFile.stats,
      addFile.partitionValues,
      marks = addFile.tags("marks").toLong,
      tags = addFile.tags,
      deletionVector = addFile.deletionVector
    )
  }

  def partsMetricsToAddFile(
      database: String,
      tableName: String,
      originPathStr: String,
      returnedMetrics: String,
      hostName: Seq[String]): Seq[AddFile] = {

    val mapper: ObjectMapper = new ObjectMapper()
    val values: JList[WriteReturnedMetric] =
      mapper.readValue(returnedMetrics, new TypeReference[JList[WriteReturnedMetric]]() {})
    val path = new Path(originPathStr)
    val modificationTime = System.currentTimeMillis()

    values.asScala.map {
      value =>
        val partitionValues = if (value.getPartitionValues.isEmpty) {
          Map.empty[String, String]
        } else {
          MergeTreePartitionUtils.parsePartitions(value.getPartitionValues)
        }

        AddFileTags.partsInfoToAddFile(
          database,
          tableName,
          "MergeTree",
          path.toUri.getPath,
          hostName.map(_.trim).mkString(","),
          value.getPartName,
          "",
          value.getRowCount,
          value.getDiskSize,
          -1L,
          -1L,
          modificationTime,
          "",
          -1L,
          -1L,
          -1,
          -1L,
          value.getBucketId,
          path.toString,
          dataChange = true,
          "",
          partitionValues = partitionValues,
          marks = value.getMarkCount
        )
    }.toSeq
  }
}
