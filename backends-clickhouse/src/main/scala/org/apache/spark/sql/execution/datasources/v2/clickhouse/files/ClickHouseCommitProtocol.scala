/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.clickhouse.files

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.files.DelayedCommitProtocol

class ClickHouseCommitProtocol(jobId: String,
                               path: String,
                               queryId: String,
                               randomPrefixLength: Option[Int])
  extends DelayedCommitProtocol(jobId, path, randomPrefixLength) {

  override def setupJob(jobContext: JobContext): Unit = {
    val fs = new Path(path).getFileSystem(jobContext.getConfiguration)
    val tmpPath = new Path(path, queryId)
    if (!fs.exists(tmpPath)) {
      fs.mkdirs(tmpPath)
    }
  }

  override def commitJob(jobContext: JobContext,
                         taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit =
    super.commitJob(jobContext, taskCommits)

  override def abortJob(jobContext: JobContext): Unit = super.abortJob(jobContext)

  override def setupTask(taskContext: TaskAttemptContext): Unit = super.setupTask(taskContext)

  override def commitTask(taskContext: TaskAttemptContext
                         ): FileCommitProtocol.TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(path, addedFiles.head._2).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[FileAction] = addedFiles.map { f =>
        val filePath = new Path(path, new Path(new URI(f._2)))
        val stat = fs.getFileStatus(filePath)

        buildActionFromAddedFile(f, stat, taskContext)
      }

      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Nil)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = super.abortTask(taskContext)
}
