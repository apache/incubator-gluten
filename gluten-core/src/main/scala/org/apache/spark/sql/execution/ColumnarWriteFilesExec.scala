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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.{Partition, SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.Date

/**
 * This RDD is used to make sure we have injected staging write path before initializing the native
 * plan, and support Spark file commit protocol.
 */
class ColumnarWriteFilesRDD(
    var prev: RDD[ColumnarBatch],
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends RDD[WriterCommitMessage](prev) {

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
      taskAttemptContext: TaskAttemptContext,
      sparkPartitionId: Int
  ): WriteTaskResult = {

    val dataWriter =
      if (sparkPartitionId != 0) {
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

    val commitProtocol =
      BackendsApiManager.getSparkPlanExecApiInstance.createCommitter(
        description,
        committer,
        jobTrackerID)

    commitProtocol.setupTask()

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {

        // Initialize the native plan
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)
        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)
        val writeTaskResult = commitProtocol
          .commitTask(resultColumnarBatch)
          .orElse({
            // If we are writing an empty iterator, then gluten backend would do nothing.
            // Here we fallback to use vanilla Spark write files to generate an empty file for
            // metadata only.
            Some(writeFilesForEmptyIterator(commitProtocol.taskAttemptContext, context.partitionId))
            // We have done commit task inside `writeFilesForEmptyIterator`.
          })
          .get
        reportTaskMetrics(writeTaskResult)
        Iterator.single(writeTaskResult)
      })(
        catchBlock = {
          // If there is an error, abort the task
          commitProtocol.abortTask()
          logError(s"Job ${commitProtocol.jobId} aborted.")
        }
      )
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw new SparkException(
          s"Task failed while writing rows to output path: ${description.path}",
          t)
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

// The class inherits from "BinaryExecNode" instead of "UnaryExecNode" because
// we need to expose a dummy child (as right child) with type "WriteFilesExec" to let Spark
// choose the new write code path (version >= 3.4). The actual plan to write is the left child
// of this operator.
case class ColumnarWriteFilesExec private (
    override val left: SparkPlan,
    override val right: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends BinaryExecNode
  with GlutenPlan
  with ColumnarWriteFilesExec.ExecuteWriteCompatible {

  val child: SparkPlan = left

  override lazy val references: AttributeSet = AttributeSet.empty

  override def supportsColumnar(): Boolean = true

  override def output: Seq[Attribute] = Seq.empty

  override protected def doExecute(): RDD[InternalRow] = {
    throw new GlutenException(s"$nodeName does not support doExecute")
  }

  /** Fallback to use vanilla Spark write files to generate an empty file for metadata only. */
  private def writeFilesForEmptyRDD(
      description: WriteJobDescription,
      committer: FileCommitProtocol,
      jobTrackerID: String): RDD[WriterCommitMessage] = {
    val rddWithNonEmptyPartitions = session.sparkContext.parallelize(Seq.empty[InternalRow], 1)
    rddWithNonEmptyPartitions.mapPartitionsInternal {
      iterator =>
        val sparkStageId = TaskContext.get().stageId()
        val sparkPartitionId = TaskContext.get().partitionId()
        val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue

        val ret = SparkShimLoader.getSparkShims.writeFilesExecuteTask(
          description,
          jobTrackerID,
          sparkStageId,
          sparkPartitionId,
          sparkAttemptNumber,
          committer,
          iterator
        )
        Iterator(ret)
    }
  }

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
      new ColumnarWriteFilesRDD(rdd, description, committer, jobTrackerID)
    }
  }
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(newLeft, newRight, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)
}

object ColumnarWriteFilesExec {

  def apply(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec = {
    // This is a workaround for FileFormatWriter#write. Vanilla Spark (version >= 3.4) requires for
    // a plan that has at least one node exactly of type `WriteFilesExec` that is a Scala
    // case-class, to decide to choose new `#executeWrite` code path over the legacy `#execute`
    // for write operation.
    //
    // So we add a no-op `WriteFilesExec` child to let Spark pick the new code path.
    //
    // See: FileFormatWriter#write
    // See: V1Writes#getWriteFilesOpt
    val right: SparkPlan =
      WriteFilesExec(
        NoopLeaf(),
        fileFormat,
        partitionColumns,
        bucketSpec,
        options,
        staticPartitions)

    ColumnarWriteFilesExec(
      child,
      right,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
  }

  private case class NoopLeaf() extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new GlutenException(s"$nodeName does not support doExecute")
    override def output: Seq[Attribute] = Seq.empty
  }

  sealed trait ExecuteWriteCompatible {
    // To be compatible with Spark (version < 3.4)
    protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
      throw new GlutenException(
        s"Internal Error ${this.getClass} has write support" +
          s" mismatch:\n$this")
    }
  }
}
