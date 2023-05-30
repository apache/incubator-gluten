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
package org.apache.spark.sql.execution.datasources.v2

import io.glutenproject.execution.{FirstZippedPartitionsPartition, GlutenFilePartition}
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.ddlplan.{DllNode, DllTransformContext, InsertOutputBuilder, InsertPlanNode}
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.SnowflakeIdWorker

import org.apache.spark.{Partition, SparkEnv, SparkException, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, Write, WriterCommitMessage}
import org.apache.spark.sql.delta.{DeltaOperations, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.files.ClickHouseCommitProtocol
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.ClickHouseBatchWrite
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{LongAccumulator, SerializableConfiguration, Utils}

import com.google.common.collect.Lists

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

case class ClickHouseAppendDataExec(
    table: SupportsWrite,
    writeOptions: CaseInsensitiveStringMap,
    query: SparkPlan,
    write: Write,
    refreshCache: () => Unit)
  extends V2TableWriteExec
  with ImplicitMetadataOperation
  with DeltaCommand {

  override protected val canMergeSchema: Boolean = false

  override protected val canOverwriteSchema: Boolean = false

  private val clickhouseTableV2 = table.asInstanceOf[ClickHouseTableV2]

  private val deltaLog = clickhouseTableV2.deltaLog

  private val sparkSession = clickhouseTableV2.spark

  private val configuration = deltaLog.snapshot.metadata.configuration

  private val mode = SaveMode.Append

  private val deltaOptions = new DeltaOptions(
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*).toMap,
    sparkSession.sessionState.conf)

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(write.toBatch)
    refreshCache()
    writtenRows
  }

  override protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val chBatchWrite = batchWrite.asInstanceOf[ClickHouseBatchWrite]
    val schema = chBatchWrite.querySchema
    val queryId = chBatchWrite.queryId

    if (schema != clickhouseTableV2.schema()) {
      throw new SparkException(
        s"Writing job failed: " +
          s"the schema of inserting is different from table schema.")
    }

    try {
      deltaLog.withNewTransaction {
        txn =>
          if (txn.readVersion < 0) {
            logError(s"Table ${clickhouseTableV2.tableIdentifier} doesn't exists.")
            throw new SparkException(
              s"Writing job failed: " +
                s"table ${clickhouseTableV2.tableIdentifier} doesn't exists.")
          }

          updateMetadata(
            sparkSession,
            txn,
            schema,
            Nil,
            configuration,
            false,
            deltaOptions.rearrangeOnly)

          // Validate partition predicates
          // val replaceWhere = deltaOptions.replaceWhere
          // val partitionFilters = None

          val rdd: RDD[InternalRow] = {
            val tempRdd = query.execute()
            // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
            // partition rdd to make sure we at least set up one write task to write the metadata.
            if (tempRdd.partitions.length == 0) {
              sparkContext.parallelize(Array.empty[InternalRow], 1)
            } else {
              tempRdd
            }
          }
          val partitions = rdd.partitions
          val useCommitCoordinator = batchWrite.useCommitCoordinator
          val messages = new Array[WriterCommitMessage](partitions.length)
          val totalNumRowsAccumulator = new LongAccumulator()

          val newFiles = deltaTxnWriteMergeTree(
            txn,
            partitions,
            query.output,
            messages,
            useCommitCoordinator,
            totalNumRowsAccumulator,
            queryId)
          logInfo(s"Data source write support $batchWrite is committing.")
          batchWrite.commit(messages)
          logInfo(s"Data source write support $batchWrite committed.")
          commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
          val operation = DeltaOperations.Write(
            SaveMode.Append,
            Option(Seq.empty[String]),
            deltaOptions.replaceWhere,
            deltaOptions.userMetadata)
          txn.commit(newFiles, operation)
      }
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          // batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }
    Seq.empty[InternalRow]
  }

  private def deltaTxnWriteMergeTree(
      txn: OptimisticTransaction,
      partitions: Array[Partition],
      queryOutput: Seq[Attribute],
      messages: Array[WriterCommitMessage],
      useCommitCoordinator: Boolean,
      totalNumRowsAccumulator: LongAccumulator,
      queryId: String): Seq[FileAction] = {
    // write the mergetree data
    logInfo(
      s"Start processing data source append support. " +
        s"The input RDD has ${messages.length} partitions.")

    val database = clickhouseTableV2.catalogTable.get.identifier.database.get
    val tableName = clickhouseTableV2.catalogTable.get.identifier.table
    val engine = deltaLog.snapshot.metadata.configuration.get("engine").get
    val tablePath = deltaLog.dataPath.toString.substring(6)
    val partitionSchema = deltaLog.snapshot.metadata.partitionSchema
    val outputPath = deltaLog.dataPath
    val committer = new ClickHouseCommitProtocol("clickhouse", outputPath.toString, queryId, None)
    val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)
    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
    if (sparkSession.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
      val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
        new SerializableConfiguration(sparkSession.sessionState.newHadoopConf()),
        BasicWriteJobStatsTracker.metrics)
      txn.registerSQLMetrics(sparkSession, basicWriteJobStatsTracker.driverSideMetrics)
      statsTrackers.append(basicWriteJobStatsTracker)
    }

    // Generate insert substrait plan for per partition
    val substraitContext = new SubstraitContext
    val dllCxt = genInsertPlan(substraitContext, queryOutput)
    val substraitPlanPartition = partitions.map(
      p => {
        p match {
          case FirstZippedPartitionsPartition(index: Int, inputPartition: GlutenFilePartition, _) =>
            val files = inputPartition.files
            if (files.length > 1) {
              throw new SparkException(
                s"Writing job failed: " +
                  s"can not support multiple input files in one partition.")
            }
            if (files.head.start != 0) {
              throw new SparkException(
                s"Writing job failed: " +
                  s"can not read a parquet file from non-zero start.")
            }
            val paths = new java.util.ArrayList[String]()
            val starts = new java.util.ArrayList[java.lang.Long]()
            val lengths = new java.util.ArrayList[java.lang.Long]()
            paths.add(files.head.filePath)
            starts.add(files.head.start)
            lengths.add(files.head.length)
            val localFilesNode =
              LocalFilesBuilder
                .makeLocalFiles(index, paths, starts, lengths, ReadFileFormat.UnknownFormat)
            val insertOutputNode = InsertOutputBuilder.makeInsertOutputNode(
              SnowflakeIdWorker.getInstance().nextId(),
              database,
              tableName,
              tablePath)
            dllCxt.substraitContext.setLocalFilesNodes(Seq(localFilesNode))
            dllCxt.substraitContext.setInsertOutputNode(insertOutputNode)
            val substraitPlan = dllCxt.root.toProtobuf
            logWarning(dllCxt.root.toProtobuf.toString)
          case _ =>
            throw new SparkException(
              s"Writing job failed: " +
                s"can not support input partition.")
        }
      })

    // committer.setupJob(jobContext = )
    val newFiles = Nil
    val addFiles = newFiles.collect { case a: AddFile => a }

    /* sparkContext.runJob(
          rdd,
          (context: TaskContext, iter: Iterator[InternalRow]) =>
            CHDataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator),
          rdd.partitions.indices,
          (index, result: DataWritingSparkTaskResult) => {
            val commitMessage = result.writerCommitMessage
            messages(index) = commitMessage
            totalNumRowsAccumulator.add(result.numRows)
            batchWrite.onDataWriterCommit(commitMessage)
          }
        ) */

    Nil
  }

  def genInsertPlan(
      substraitContext: SubstraitContext,
      queryOutput: Seq[Attribute]): DllTransformContext = {
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(queryOutput)
    val nameList = new java.util.ArrayList[String]()
    for (attr <- queryOutput) {
      nameList.add(ConverterUtils.getShortAttributeName(attr) + "#" + attr.exprId.id)
    }
    val relNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      null,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName))

    val inputPlanNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode), nameList)

    val insertPlanNode = new InsertPlanNode(substraitContext, inputPlanNode)

    val dllNode = new DllNode(Lists.newArrayList(insertPlanNode))
    DllTransformContext(queryOutput, queryOutput, dllNode, substraitContext)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ClickHouseAppendDataExec =
    copy(query = newChild)
}

object CHDataWritingSparkTask extends Logging {
  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId)

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        // Count is here.
        count += 1
        // dataWriter.write(iter.next())
      }

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(
            s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
              s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val message =
            s"Commit denied for partition $partId (task $taskId, attempt $attemptId, " +
              s"stage $stageId.$stageAttempt)"
          logInfo(message)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw new CommitDeniedException(message, stageId, partId, attemptId)
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(
        s"Committed partition $partId (task $taskId, attempt $attemptId, " +
          s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(
      catchBlock = {
        // If there is an error, abort this writer
        logError(
          s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
        dataWriter.abort()
        logError(
          s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
      },
      finallyBlock = {
        dataWriter.close()
      }
    )
  }
}
