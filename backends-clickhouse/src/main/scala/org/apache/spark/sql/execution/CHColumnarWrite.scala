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
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, ExecutedWriteSummary, PartitioningUtils, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, OutputCommitter, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.lang.reflect.Field

import scala.collection.mutable

trait CHColumnarWrite[T <: FileCommitProtocol] {

  def description: WriteJobDescription
  def jobTrackerID: String
  def committer: T
  def doSetupNativeTask(): Unit

  def setupTask(): Unit = {
    committer.setupTask(taskAttemptContext)
    doSetupNativeTask()
  }

  def abortTask(): Unit = {
    committer.abortTask(taskAttemptContext)
  }
  def commitTask(batch: ColumnarBatch): Option[WriteTaskResult]

  lazy val (taskAttemptContext: TaskAttemptContext, jobId: String) = {
    // Copied from `SparkHadoopWriterUtils.createJobID` to be compatible with multi-version
    def createJobID(jobTrackerID: String, id: Int): JobID = {
      if (id < 0) {
        throw new IllegalArgumentException("Job number is negative")
      }
      new JobID(jobTrackerID, id)
    }

    val sparkStageId: Int = TaskContext.get().stageId()
    val sparkPartitionId: Int = TaskContext.get().partitionId()
    val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
    val jobID = createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobID, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobID.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    (new TaskAttemptContextImpl(hadoopConf, taskAttemptId), jobID.toString)
  }
}

object CreateFileNameSpec {
  def apply(taskContext: TaskAttemptContext, description: WriteJobDescription): FileNameSpec = {
    val fileCounter = 0
    val suffix = f".c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskContext)
    FileNameSpec("", suffix)
  }
}

object CreateBasicWriteTaskStats {
  def apply(
      numFiles: Int,
      updatedPartitions: Set[String],
      numWrittenRows: Long): BasicWriteTaskStats = {
    val partitionsInternalRows = updatedPartitions.map {
      part =>
        val parts = new Array[Any](1)
        parts(0) = part
        new GenericInternalRow(parts)
    }.toSeq
    BasicWriteTaskStats(
      partitions = partitionsInternalRows,
      numFiles = numFiles,
      numBytes = 101,
      numRows = numWrittenRows)
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

case class HadoopMapReduceCommitProtocolWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: HadoopMapReduceCommitProtocol)
  extends CHColumnarWrite[HadoopMapReduceCommitProtocol]
  with Logging {

  private lazy val adapter: HadoopMapReduceAdapter = HadoopMapReduceAdapter(committer)

  /**
   * This function is used in [[CHColumnarWriteFilesRDD]] to inject the staging write path before
   * initializing the native plan and collect native write files metrics for each backend.
   */
  override def doSetupNativeTask(): Unit = {
    val (writePath, writeFileName) =
      adapter.getTaskAttemptTempPathAndFilename(taskAttemptContext, description)
    logDebug(s"Native staging write path: $writePath and file name: $writeFileName")
    BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, writeFileName)
  }

  def doCollectNativeResult(cb: ColumnarBatch): Option[WriteTaskResult] = {
    val numFiles = cb.numRows()
    // Write an empty iterator
    if (numFiles == 0) {
      None
    } else {
      val file_col = cb.column(0)
      val partition_col = cb.column(1)
      val count_col = cb.column(2)

      val outputPath = description.path
      val partitions: mutable.Set[String] = mutable.Set[String]()
      val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()

      var numWrittenRows: Long = 0
      Range(0, cb.numRows()).foreach {
        i =>
          val targetFileName = file_col.getUTF8String(i).toString
          val partition = partition_col.getUTF8String(i).toString
          if (partition != "__NO_PARTITION_ID__") {
            partitions += partition
            val tmpOutputPath = outputPath + "/" + partition + "/" + targetFileName
            val customOutputPath =
              description.customPartitionLocations.get(
                PartitioningUtils.parsePathFragment(partition))
            if (customOutputPath.isDefined) {
              addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
            }
          }
          numWrittenRows += count_col.getLong(i)
      }

      val updatedPartitions = partitions.toSet
      val summary =
        ExecutedWriteSummary(
          updatedPartitions = updatedPartitions,
          stats = Seq(CreateBasicWriteTaskStats(numFiles, updatedPartitions, numWrittenRows)))
      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
          summary))
    }
  }

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
}

object CHColumnarWrite {
  def apply(
      jobTrackerID: String,
      description: WriteJobDescription,
      committer: FileCommitProtocol): CHColumnarWrite[FileCommitProtocol] = committer match {
    case h: HadoopMapReduceCommitProtocol =>
      HadoopMapReduceCommitProtocolWrite(jobTrackerID, description, h)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case other => CHDeltaColumnarWrite(jobTrackerID, description, other)
  }
}
