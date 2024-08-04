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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, HadoopMapReduceCommitProtocol}
import org.apache.spark.sql.execution.datasources.{WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.lang.reflect.Field

object CreateFileNameSpec {
  def apply(taskContext: TaskAttemptContext, description: WriteJobDescription): FileNameSpec = {
    val fileCounter = 0
    val suffix = f".c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskContext)
    FileNameSpec("", suffix)
  }
}

/** [[HadoopMapReduceAdapter]] for [[HadoopMapReduceCommitProtocol]]. */
case class HadoopMapReduceAdapter(sparkCommitter: HadoopMapReduceCommitProtocol) {
  private lazy val committer: OutputCommitter = {
    val field: Field = classOf[HadoopMapReduceCommitProtocol].getDeclaredField("committer")
    field.setAccessible(true)
    field.get(sparkCommitter).asInstanceOf[OutputCommitter]
  }
  private lazy val GetFilename = {
    val m = classOf[HadoopMapReduceCommitProtocol]
      .getDeclaredMethod("getFilename", classOf[TaskAttemptContext], classOf[FileNameSpec])
    m.setAccessible(true)
    m
  }

  private def newTaskAttemptTempPath(defaultPath: String): String = {
    assert(committer != null)
    val stagingDir: Path = committer match {
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(defaultPath))
      case _ =>
        new Path(defaultPath)
    }
    stagingDir.toString
  }

  private def getFilename(taskContext: TaskAttemptContext, spec: FileNameSpec): String = {
    GetFilename.invoke(sparkCommitter, taskContext, spec).asInstanceOf[String]
  }

  def getTaskAttemptTempPathAndFilename(
      taskContext: TaskAttemptContext,
      description: WriteJobDescription): (String, String) = {
    val stageDir = newTaskAttemptTempPath(description.path)
    val filename = getFilename(taskContext, CreateFileNameSpec(taskContext, description))
    (stageDir, filename)
  }
}

/** [[GlutenCommitProtocol]] wrapper for [[FileCommitProtocol]]. */
trait GlutenCommitProtocol {
  def setupTask(): Unit
  def commitTask(batch: ColumnarBatch): Option[WriteTaskResult]
  def abortTask(): Unit
  def jobId: String
  def taskAttemptContext: TaskAttemptContext
}

/**
 * A wrapper for [[FileCommitProtocol]]. This class only affects the task side commit process. e.g.,
 * `setupTask`, `newTaskAttemptTempPath`, `commitTask`, `abortTask`. The job commit process is at
 * vanilla Spark driver side.
 */
abstract class SparkWriteFilesCommitProtocol(
    val description: WriteJobDescription,
    val committer: FileCommitProtocol,
    val jobTrackerID: String)
  extends GlutenCommitProtocol
  with Logging {

  val sparkStageId: Int = TaskContext.get().stageId()
  val sparkPartitionId: Int = TaskContext.get().partitionId()
  private val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
  private val jobID = createJobID(jobTrackerID, sparkStageId)

  private val taskId = new TaskID(jobID, TaskType.MAP, sparkPartitionId)
  private val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

  // Set up the attempt context required to use in the output committer.
  override lazy val taskAttemptContext: TaskAttemptContext = {
    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobID.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }

  override def setupTask(): Unit = {
    committer.setupTask(taskAttemptContext)
    doSetupNativeTask()
  }

  override def jobId: String = jobID.toString

  override def abortTask(): Unit = {
    committer.abortTask(taskAttemptContext)
  }

  // Copied from `SparkHadoopWriterUtils.createJobID` to be compatible with multi-version
  private def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }

  def doSetupNativeTask(): Unit
}

abstract class SparkHadoopMapReduceCommitProtocol(
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends SparkWriteFilesCommitProtocol(description, committer, jobTrackerID) {
  assert(committer.isInstanceOf[HadoopMapReduceCommitProtocol])

  private lazy val hadoopMapReduceAdapter: HadoopMapReduceAdapter = HadoopMapReduceAdapter(
    committer.asInstanceOf[HadoopMapReduceCommitProtocol])

  def doCollectNativeResult(batch: ColumnarBatch): Option[WriteTaskResult]

  override def commitTask(batch: ColumnarBatch): Option[WriteTaskResult] = {
    doCollectNativeResult(batch).map(
      nativeWriteTaskResult => {
        val (_, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(taskAttemptContext)
        }

        // Just for update task commit time
        description.statsTrackers.foreach {
          stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
        }
        nativeWriteTaskResult
      })
  }

  /**
   * This function is used in [[ColumnarWriteFilesRDD]] to inject the staging write path before
   * initializing the native plan and collect native write files metrics for each backend.
   */
  override def doSetupNativeTask(): Unit = {
    val (writePath, writeFileName) =
      hadoopMapReduceAdapter.getTaskAttemptTempPathAndFilename(taskAttemptContext, description)
    logDebug(s"Native staging write path: $writePath and file name: $writeFileName")
    BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, writeFileName)
  }
}
