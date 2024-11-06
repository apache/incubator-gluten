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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.files.DelayedCommitProtocol
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.mapreduce.TaskAttemptContext

import scala.collection.mutable.ArrayBuffer

/** A Wrapper of [[DelayedCommitProtocol]] for accessing protected methods and fields. */
class CHDelayedCommitProtocol(
    jobId: String,
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String])
  extends DelayedCommitProtocol(jobId, outputPath, randomPrefixLength, subdir) {

  override def getFileName(
      taskContext: TaskAttemptContext,
      ext: String,
      partitionValues: Map[String, String]): String = {
    super.getFileName(taskContext, ext, partitionValues)
  }

  def updateAddedFiles(files: Seq[(Map[String, String], String)]): Unit = {
    assert(addedFiles.isEmpty)
    addedFiles ++= files
  }

  override def parsePartitions(dir: String): Map[String, String] =
    super.parsePartitions(dir)
}

case class CHDelayedCommitProtocolWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: CHDelayedCommitProtocol)
  extends CHColumnarWrite[CHDelayedCommitProtocol]
  with Logging {

  override def doSetupNativeTask(): Unit = {
    assert(description.path == committer.outputPath)
    val nameSpec = CreateFileNameSpec(taskAttemptContext, description)
    val writePath = description.path
    val writeFileName = committer.getFileName(taskAttemptContext, nameSpec.suffix, Map.empty)
    logDebug(s"Native staging write path: $writePath and file name: $writeFileName")
    BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, writeFileName)
  }

  private def doCollectNativeResult(
      cb: ColumnarBatch): Option[(Seq[(Map[String, String], String)], ExecutedWriteSummary)] = {
    val basicNativeStats = BasicNativeStats(cb)

    // TODO: we need close iterator here before processing the result.

    // Write an empty iterator
    if (basicNativeStats.isEmpty) {
      None
    } else {
      // process stats
      val addedFiles: ArrayBuffer[(Map[String, String], String)] =
        new ArrayBuffer[(Map[String, String], String)]
      var numWrittenRows: Long = 0

      basicNativeStats.foreach {
        stat =>
          val absolutePath = s"${description.path}/${stat.relativePath}"
          if (stat.partition_id == "__NO_PARTITION_ID__") {
            addedFiles.append((Map.empty[String, String], stat.filename))
          } else {
            val partitionValues = committer.parsePartitions(stat.partition_id)
            addedFiles.append((partitionValues, stat.relativePath))
            basicWriteJobStatsTracker.newPartition(
              new GenericInternalRow(Array[Any](stat.partition_id)))
          }
          basicWriteJobStatsTracker.newFile(absolutePath)
          basicWriteJobStatsTracker.closeFile(absolutePath)
          numWrittenRows += stat.record_count
      }

      Some(
        (
          addedFiles.toSeq,
          ExecutedWriteSummary(
            updatedPartitions = Set.empty,
            stats = Seq(finalStats(numWrittenRows)))))
    }
  }

  override def commitTask(batch: ColumnarBatch): Option[WriteTaskResult] = {
    doCollectNativeResult(batch).map {
      case (addedFiles, summary) =>
        require(addedFiles.nonEmpty, "No files to commit")

        committer.updateAddedFiles(addedFiles)

        val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(taskAttemptContext)
        }

        // Just for update task commit time
        description.statsTrackers.foreach {
          stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
        }
        WriteTaskResult(taskCommitMessage, summary)
    }
  }
}

object CHDeltaColumnarWrite {
  def apply(
      jobTrackerID: String,
      description: WriteJobDescription,
      committer: FileCommitProtocol): CHColumnarWrite[FileCommitProtocol] = committer match {
    case c: CHDelayedCommitProtocol =>
      CHDelayedCommitProtocolWrite(jobTrackerID, description, c)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case _ =>
      throw new GlutenNotSupportException(
        s"Unsupported committer type: ${committer.getClass.getSimpleName}")
  }
}
