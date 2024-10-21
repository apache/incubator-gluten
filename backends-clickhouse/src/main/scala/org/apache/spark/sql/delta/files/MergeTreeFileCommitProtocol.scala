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

import org.apache.gluten.backendsapi.clickhouse.CHConf
import org.apache.gluten.memory.CHThreadGroup
import org.apache.gluten.vectorized.ExpressionEvaluatorJniWrapper

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeCommiterHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddFileTags
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import scala.collection.JavaConverters._

trait MergeTreeFileCommitProtocol extends FileCommitProtocol {

  def outputPath: String
  def database: String
  def tableName: String

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    CHThreadGroup.registerNewThreadGroup()

    MergeTreeCommiterHelper.prepareTaskWriteInfo(
      taskContext.getJobID.toString,
      taskContext.getTaskAttemptID.toString)

    val settings = Map(CHConf.runtimeSettings("gluten.reserve_partition_columns") -> "true")
    ExpressionEvaluatorJniWrapper.updateQueryRuntimeSettings(settings.asJava)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {

    taskContext.getConfiguration.set(
      "mapreduce.task.gluten.mergetree.partition.dir",
      dir.map(p => new Path(p).toUri.toString).getOrElse(""))

    val bucketIdStr = ext.split("\\.").headOption.filter(_.startsWith("_")).map(_.substring(1))

    taskContext.getConfiguration.set(
      "mapreduce.task.gluten.mergetree.bucketid.str",
      bucketIdStr.getOrElse(""))
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
}
