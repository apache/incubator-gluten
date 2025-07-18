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
package org.apache.spark.sql.delta.files

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.memory.CHThreadGroup
import org.apache.gluten.vectorized.NativeExpressionEvaluator

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeCommiterHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddFileTags
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.UUID

trait MergeTreeFileCommitProtocol extends FileCommitProtocol {

  def outputPath: String
  def database: String
  def tableName: String

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    CHThreadGroup.registerNewThreadGroup()

    val jobID = taskContext.getJobID.toString
    val taskAttemptID = taskContext.getTaskAttemptID.toString
    MergeTreeCommiterHelper.prepareTaskWriteInfo(jobID, taskAttemptID)
    val settings = Map(RuntimeSettings.NATIVE_WRITE_RESERVE_PARTITION_COLUMNS.key -> "true")
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {

    val partitionStr = dir.map(p => new Path(p).toString)
    val bucketIdStr = ext.split("\\.").headOption.filter(_.startsWith("_")).map(_.substring(1))
    val split = taskContext.getTaskAttemptID.getTaskID.getId

    // The partPrefix is used to generate the part name in the MergeTree table.
    // outputPath/partition-dir/bucket-id/UUID_split
    val partition = partitionStr.map(_ + "/").getOrElse("")
    val bucket = bucketIdStr.map(_ + "/").getOrElse("")
    val partPrefix = s"$partition$bucket${UUID.randomUUID.toString}_$split"

    val settings = Map(
      RuntimeSettings.PART_NAME_PREFIX.key -> partPrefix,
      RuntimeSettings.PARTITION_DIR.key -> partitionStr.getOrElse(""),
      RuntimeSettings.BUCKET_DIR.key -> bucketIdStr.getOrElse("")
    )
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
    outputPath
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val returnedMetrics = MergeTreeCommiterHelper.getAndResetCurrentTaskWriteInfo(
      taskContext.getJobID.toString,
      taskContext.getTaskAttemptID.toString)
    val statuses = returnedMetrics.flatMap(
      AddFileTags.partsMetricsToAddFile(
        database,
        tableName,
        outputPath,
        _,
        Seq(Utils.localHostName()))
    )
    new TaskCommitMessage(statuses)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    MergeTreeCommiterHelper.resetCurrentTaskWriteInfo()
  }
}
