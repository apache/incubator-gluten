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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, BasicWriteTaskStats, ExecutedWriteSummary, PartitioningUtils, WriteJobDescription, WriteTaskResult, WriteTaskStatsTracker}
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, OutputCommitter, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.lang.reflect.Field

import scala.collection.mutable
import scala.language.implicitConversions

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
  def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult]

  lazy val basicWriteJobStatsTracker: WriteTaskStatsTracker = description.statsTrackers
    .find(_.isInstanceOf[BasicWriteJobStatsTracker])
    .map(_.newTaskInstance())
    .get

  lazy val deltaWriteJobStatsTracker: Option[DeltaJobStatisticsTracker] =
    description.statsTrackers
      .find(_.isInstanceOf[DeltaJobStatisticsTracker])
      .map(_.asInstanceOf[DeltaJobStatisticsTracker])

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

/**
 * {{{
 * val schema =
 *   StructType(
 *     StructField("filename", StringType, false) ::
 *     StructField("partition_id", StringType, false) ::
 *     StructField("record_count", LongType, false) :: Nil)
 * }}}
 */
case class NativeFileWriteResult(filename: String, partition_id: String, record_count: Long) {
  lazy val relativePath: String = if (partition_id == "__NO_PARTITION_ID__") {
    filename
  } else {
    s"$partition_id/$filename"
  }
}

object NativeFileWriteResult {
  implicit def apply(row: InternalRow): NativeFileWriteResult = {
    NativeFileWriteResult(row.getString(0), row.getString(1), row.getLong(2))
  }
}

case class NativeStatCompute(rows: Seq[InternalRow]) {
  def apply[T](stats: Seq[T => Unit], extra: Option[InternalRow => Unit] = None)(implicit
      creator: InternalRow => T): Unit = {
    rows.foreach {
      row =>
        val stat = creator(row)
        stats.foreach(agg => agg(stat))
        extra.foreach(_(row))
    }
  }
}

case class NativeBasicWriteTaskStatsTracker(
    description: WriteJobDescription,
    basicWriteJobStatsTracker: WriteTaskStatsTracker)
  extends (NativeFileWriteResult => Unit) {
  private var numWrittenRows: Long = 0
  override def apply(stat: NativeFileWriteResult): Unit = {
    val absolutePath = s"${description.path}/${stat.relativePath}"
    if (stat.partition_id != "__NO_PARTITION_ID__") {
      basicWriteJobStatsTracker.newPartition(new GenericInternalRow(Array[Any](stat.partition_id)))
    }
    basicWriteJobStatsTracker.newFile(absolutePath)
    basicWriteJobStatsTracker.closeFile(absolutePath)
    numWrittenRows += stat.record_count
  }
  private def finalStats: BasicWriteTaskStats = basicWriteJobStatsTracker
    .getFinalStats(0)
    .asInstanceOf[BasicWriteTaskStats]

  def result: BasicWriteTaskStats = finalStats.copy(numRows = numWrittenRows)
}

case class FileCommitInfo(description: WriteJobDescription)
  extends (NativeFileWriteResult => Unit) {
  private val partitions: mutable.Set[String] = mutable.Set[String]()
  private val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()

  def apply(stat: NativeFileWriteResult): Unit = {
    val tmpAbsolutePath = s"${description.path}/${stat.relativePath}"
    if (stat.partition_id != "__NO_PARTITION_ID__") {
      partitions += stat.partition_id
      val customOutputPath =
        description.customPartitionLocations.get(
          PartitioningUtils.parsePathFragment(stat.partition_id))
      if (customOutputPath.isDefined) {
        addedAbsPathFiles(tmpAbsolutePath) = customOutputPath.get + "/" + stat.filename
      }
    }
  }

  def result: (Set[String], Map[String, String]) = {
    (partitions.toSet, addedAbsPathFiles.toMap)
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

  def doCollectNativeResult(stats: Seq[InternalRow]): Option[WriteTaskResult] = {
    // Write an empty iterator
    if (stats.isEmpty) {
      None
    } else {
      val commitInfo = FileCommitInfo(description)
      val basicNativeStat = NativeBasicWriteTaskStatsTracker(description, basicWriteJobStatsTracker)
      val basicNativeStats = Seq(commitInfo, basicNativeStat)
      NativeStatCompute(stats)(basicNativeStats)
      val (partitions, addedAbsPathFiles) = commitInfo.result
      val updatedPartitions = partitions
      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles -> updatedPartitions),
          ExecutedWriteSummary(
            updatedPartitions = updatedPartitions,
            stats = Seq(basicNativeStat.result))
        ))
    }
  }

  override def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult] = {
    doCollectNativeResult(writeResults).map(
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
