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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.vectorized.NativeExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.files.MergeTreeDelayedCommitProtocol2
import org.apache.spark.sql.delta.stats.DeltaStatistics
import org.apache.spark.sql.delta.util.{JsonUtils, MergeTreePartitionUtils}
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path

import java.util.UUID

/**
 * {{{
 * val schema =
 *   StructType(
 *     StructField("part_name", StringType, false) ::
 *     StructField("partition_id", StringType, false) ::
 *     StructField("record_count", LongType, false) ::
 *     StructField("marks_count", LongType, false) ::
 *     StructField("size_in_bytes", LongType, false) :: Nil)
 * }}}
 */
case class BasicMergeTreeNativeStat(
    part_name: String,
    partition_id: String,
    record_count: Long,
    marks_count: Long,
    size_in_bytes: Long) {
  def apply(
      database: String,
      table: String,
      path: Path,
      modificationTime: Long,
      hostName: Seq[String]): FileAction = {
    val partitionValues = if (partition_id == "__NO_PARTITION_ID__") {
      Map.empty[String, String]
    } else {
      MergeTreePartitionUtils.parsePartitions(partition_id)
    }
    val tags = Map[String, String](
      "database" -> database,
      "table" -> table,
      "engine" -> "MergeTree",
      "path" -> path.toUri.getPath,
      "targetNode" -> hostName.map(_.trim).mkString(","),
      "partition" -> "",
      "uuid" -> "",
      "rows" -> record_count.toString,
      "bytesOnDisk" -> size_in_bytes.toString,
      "dataCompressedBytes" -> -1L.toString,
      "dataUncompressedBytes" -> -1L.toString,
      "modificationTime" -> modificationTime.toString,
      "partitionId" -> "",
      "minBlockNumber" -> -1L.toString,
      "maxBlockNumber" -> -1L.toString,
      "level" -> -1L.toString,
      "dataVersion" -> -1L.toString,
      "defaultCompressionCodec" -> "lz4",
      "bucketNum" -> "",
      "dirName" -> path.toString,
      "marks" -> marks_count.toString
    )

    val stats = Map[String, Any](
      DeltaStatistics.NUM_RECORDS -> record_count,
      DeltaStatistics.MIN -> "",
      DeltaStatistics.MAX -> "",
      DeltaStatistics.NULL_COUNT -> ""
    )
    AddFile(
      part_name,
      partitionValues,
      size_in_bytes,
      modificationTime,
      dataChange = true,
      JsonUtils.toJson(stats),
      tags)
  }
}

object BasicMergeTreeNativeStats {

  /**
   * we assume the number of records is less than 10,000.So the memory overhead is acceptable.
   * otherwise, we need to access ColumnarBatch row by row, which is not efficient.
   */
  def apply(cb: ColumnarBatch): Seq[BasicMergeTreeNativeStat] =
    CHExecUtil
      .c2r(cb)
      .map(
        row =>
          BasicMergeTreeNativeStat(
            row.getUTF8String(0).toString,
            row.getUTF8String(1).toString,
            row.getLong(2),
            row.getLong(3),
            row.getLong(4)
          ))
      .toSeq
}

case class MergeTreeDeltaColumnarWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: MergeTreeDelayedCommitProtocol2)
  extends CHColumnarWrite[MergeTreeDelayedCommitProtocol2]
  with Logging {
  override def doSetupNativeTask(): Unit = {
    assert(description.path == committer.outputPath)
    val writePath = StorageMeta.normalizeRelativePath(committer.outputPath)
    val split = taskAttemptContext.getTaskAttemptID.getTaskID.getId
    val partPrefixWithoutPartitionAndBucket = s"${UUID.randomUUID.toString}_$split"
    logDebug(
      s"Pipeline write path: $writePath with part prefix: $partPrefixWithoutPartitionAndBucket")
    val settings =
      Map(
        RuntimeSettings.TASK_WRITE_TMP_DIR.key -> writePath,
        RuntimeSettings.PART_NAME_PREFIX.key -> partPrefixWithoutPartitionAndBucket)
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
  }

  private def doCollectNativeResult(cb: ColumnarBatch): Option[WriteTaskResult] = {
    val basicNativeStats = BasicMergeTreeNativeStats(cb)

    // TODO: we need close iterator here before processing the result.

    if (basicNativeStats.isEmpty) {
      None
    } else {
      val modificationTime = System.currentTimeMillis()
      val hostName = Seq(Utils.localHostName())
      val path = new Path(committer.outputPath)
      var numRows: Long = 0
      var numBytes: Long = 0
      val numFiles = basicNativeStats.size
      val addFiles = basicNativeStats.map {
        stat =>
          if (stat.partition_id != "__NO_PARTITION_ID__") {
            basicWriteJobStatsTracker.newPartition(
              new GenericInternalRow(Array[Any](stat.partition_id)))
          }
          numRows += stat.record_count
          numBytes += stat.size_in_bytes
          stat(committer.database, committer.tableName, path, modificationTime, hostName)
      }

      Some {
        WriteTaskResult(
          new TaskCommitMessage(addFiles),
          ExecutedWriteSummary(
            updatedPartitions = Set.empty,
            stats =
              Seq(finalStats.copy(numFiles = numFiles, numBytes = numBytes, numRows = numRows)))
        )
      }
    }
  }

  override def commitTask(batch: ColumnarBatch): Option[WriteTaskResult] = {
    doCollectNativeResult(batch)
  }
}
