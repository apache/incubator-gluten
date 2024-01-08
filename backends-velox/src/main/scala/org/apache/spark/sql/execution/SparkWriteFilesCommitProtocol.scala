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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol}
import org.apache.spark.sql.execution.datasources.WriteJobDescription
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.lang.reflect.Field

/**
 * A wrapper for [[HadoopMapReduceCommitProtocol]]. This class only affects the task side commit
 * process. e.g., `setupTask`, `newTaskAttemptTempPath`, `commitTask`, `abortTask`. The job commit
 * process is at vanilla Spark driver side.
 */
class SparkWriteFilesCommitProtocol(
    jobTrackerID: String,
    description: WriteJobDescription,
    committer: FileCommitProtocol)
  extends Logging {
  assert(committer.isInstanceOf[HadoopMapReduceCommitProtocol])

  val sparkStageId = TaskContext.get().stageId()
  val sparkPartitionId = TaskContext.get().partitionId()
  val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
  private val jobId = createJobID(jobTrackerID, sparkStageId)

  private val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
  private val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

  // Set up the attempt context required to use in the output committer.
  val taskAttemptContext: TaskAttemptContext = {
    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }

  private lazy val internalCommitter: OutputCommitter = {
    val field: Field = classOf[HadoopMapReduceCommitProtocol].getDeclaredField("committer")
    field.setAccessible(true)
    field.get(committer).asInstanceOf[OutputCommitter]
  }

  def setupTask(): Unit = {
    committer.setupTask(taskAttemptContext)
  }

  def getJobId: String = jobId.toString

  def newTaskAttemptTempPath(): String = {
    assert(internalCommitter != null)
    val stagingDir: Path = internalCommitter match {
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(description.path))
      case _ =>
        new Path(description.path)
    }
    stagingDir.toString
  }

  def commitTask(): Unit = {
    val (_, taskCommitTime) = Utils.timeTakenMs {
      committer.commitTask(taskAttemptContext)
    }

    // Just for update task commit time
    description.statsTrackers.foreach {
      stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
    }
  }

  def abortTask(): Unit = {
    committer.abortTask(taskAttemptContext)
  }

  // Copied from `SparkHadoopWriterUtils.createJobID` to be compatible with multi-version
  private def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }
}
