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

import org.apache.spark.{Partition, SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.FileAlreadyExistsException

import java.util.Date

import scala.collection.mutable

/**
 * This RDD is used to make sure we have injected staging write path before initializing the native
 * plan, and support Spark file commit protocol.
 */
class CHColumnarWriteFilesRDD(
    var prev: RDD[ColumnarBatch],
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends RDD[WriterCommitMessage](prev) {

  private def collectNativeResult(cb: ColumnarBatch): Option[WriteTaskResult] = {
    val numFiles = cb.numRows()
    // Write an empty iterator
    if (numFiles == 0) {
      None
    } else {
      val file_col = cb.column(0)
      val partition_col = cb.column(1)
      val count_col = cb.column(2)

      val outputPath = description.path
      var updatedPartitions = Set.empty[String]
      val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()

      val write_stats = Range(0, cb.numRows()).map {
        i =>
          val targetFileName = file_col.getUTF8String(i).toString
          val partition = partition_col.getUTF8String(i).toString
          if (partition != "__NO_PARTITION_ID__") {
            updatedPartitions += partition
            val tmpOutputPath = outputPath + "/" + partition + "/" + targetFileName
            val customOutputPath =
              description.customPartitionLocations.get(
                PartitioningUtils.parsePathFragment(partition))
            if (customOutputPath.isDefined) {
              addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
            }
          }
          count_col.getLong(i)
      }

      val partitionsInternalRows = updatedPartitions.map {
        part =>
          val parts = new Array[Any](1)
          parts(0) = part
          new GenericInternalRow(parts)
      }.toSeq

      val numWrittenRows = write_stats.sum
      val stats = BasicWriteTaskStats(
        partitions = partitionsInternalRows,
        numFiles = numFiles,
        numBytes = 101,
        numRows = numWrittenRows)
      val summary =
        ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
          summary))
    }
  }

  private def reportTaskMetrics(writeTaskResult: WriteTaskResult): Unit = {
    val stats = writeTaskResult.summary.stats.head.asInstanceOf[BasicWriteTaskStats]
    val (numBytes, numWrittenRows) = (stats.numBytes, stats.numRows)
    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    // We should update it after calling `commitTask` to overwrite the metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach {
      outputMetrics =>
        outputMetrics.setBytesWritten(numBytes)
        outputMetrics.setRecordsWritten(numWrittenRows)
    }
  }

  private def writeFilesForEmptyIterator(
      commitProtocol: SparkWriteFilesCommitProtocol): WriteTaskResult = {
    val taskAttemptContext = commitProtocol.taskAttemptContext

    val dataWriter =
      if (commitProtocol.sparkPartitionId != 0) {
        // In case of empty job, leave first partition to save meta for file format like parquet.
        new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty) {
        new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
      }

    // We have done `setupTask` outside
    dataWriter.writeWithIterator(Iterator.empty)
    dataWriter.commit()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[WriterCommitMessage] = {
    val commitProtocol = new SparkWriteFilesCommitProtocol(jobTrackerID, description, committer)

    commitProtocol.setupTask()
    val writePath = commitProtocol.newTaskAttemptTempPath()
    val writeFileName = commitProtocol.getFilename
    logDebug(s"Native staging write path: $writePath and file name: $writeFileName")

    var writeTaskResult: WriteTaskResult = null
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, writeFileName)

        // Initialize the native plan
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)
        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)
        val nativeWriteTaskResult = collectNativeResult(resultColumnarBatch)
        if (nativeWriteTaskResult.isEmpty) {
          // If we are writing an empty iterator, then velox would do nothing.
          // Here we fallback to use vanilla Spark write files to generate an empty file for
          // metadata only.
          writeTaskResult = writeFilesForEmptyIterator(commitProtocol)
          // We have done commit task inside `writeFilesForEmptyIterator`.
        } else {
          writeTaskResult = nativeWriteTaskResult.get
          commitProtocol.commitTask()
        }
      })(
        catchBlock = {
          // If there is an error, abort the task
          commitProtocol.abortTask()
          logError(s"Job ${commitProtocol.getJobId} aborted.")
        }
      )
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw new SparkException(
          s"Task failed while writing rows to staging path: $writePath, " +
            s"output path: ${description.path}",
          t)
    }

    assert(writeTaskResult != null)
    reportTaskMetrics(writeTaskResult)
    Iterator.single(writeTaskResult)
  }

  override protected def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

case class CHColumnarWriteFilesExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec
) extends ColumnarWriteFilesExec(left, right) {

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(newLeft, newRight, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    assert(child.supportsColumnar)

    val rdd = child.executeColumnar()
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    if (rdd.partitions.length == 0) {
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      writeFilesForEmptyRDD(description, committer, jobTrackerID)
    } else {
      new CHColumnarWriteFilesRDD(rdd, description, committer, jobTrackerID)
    }
  }
}
