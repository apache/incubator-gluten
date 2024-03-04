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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.execution.datasource.GlutenOrcWriterInjects
import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.execution.datasource.GlutenTextWriterInjects

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.util.{Date, UUID}

/** A helper object for writing FileFormat data out to a location. */
object FileFormatWriter extends Logging {

  var executeWriterWrappedSparkPlan: SparkPlan => RDD[InternalRow] = null

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
      outputPath: String,
      customPartitionLocations: Map[TablePartitionSpec, String],
      outputColumns: Seq[Attribute])

  /** A function that converts the empty string to null for partition values. */
  case class Empty2Null(child: Expression) extends UnaryExpression with String2StringExpression {
    override def convert(v: UTF8String): UTF8String = if (v.numBytes() == 0) null else v
    override def nullable: Boolean = true
    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      nullSafeCodeGen(
        ctx,
        ev,
        c => {
          s"""if ($c.numBytes() == 0) {
             |  ${ev.isNull} = true;
             |  ${ev.value} = null;
             |} else {
             |  ${ev.value} = $c;
             |}""".stripMargin
        }
      )
    }

    override protected def withNewChildInternal(newChild: Expression): Empty2Null =
      copy(child = newChild)
  }

  /** Describes how concurrent output writers should be executed. */
  case class ConcurrentOutputWriterSpec(
      maxWriters: Int,
      createSorter: () => UnsafeExternalRowSorter)

  /**
   * Basic work flow of this command is:
   *   1. Driver side setup, including output committer initialization and data source specific
   *      preparation work for the write job to be issued. 2. Issues a write job consists of one or
   *      more executor side tasks, each of which writes all rows within an RDD partition. 3. If no
   *      exception is thrown in a task, commits that task, otherwise aborts that task; If any
   *      exception is thrown during task commitment, also aborts that task. 4. If all tasks are
   *      committed, commit the job, otherwise aborts the job; If any exception is thrown during job
   *      commitment, also aborts the job. 5. If the job is successfully committed, perform
   *      post-commit operations such as processing statistics.
   * @return
   *   The set of all partition paths that were updated during this write job.
   */

  // scalastyle:off argcount
  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String]): Set[String] = write(
    sparkSession = sparkSession,
    plan = plan,
    fileFormat = fileFormat,
    committer = committer,
    outputSpec = outputSpec,
    hadoopConf = hadoopConf,
    partitionColumns = partitionColumns,
    bucketSpec = bucketSpec,
    statsTrackers = statsTrackers,
    options = options,
    numStaticPartitionCols = 0
  )

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String],
      numStaticPartitionCols: Int = 0): Set[String] = {

    val nativeEnabled =
      "true".equals(sparkSession.sparkContext.getLocalProperty("isNativeAppliable"))
    val staticPartitionWriteOnly =
      "true".equals(sparkSession.sparkContext.getLocalProperty("staticPartitionWriteOnly"))

    if (nativeEnabled) {
      logInfo("Use Gluten partition write for hive")
      assert(plan.isInstanceOf[IFakeRowAdaptor])
    }

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
      .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation))
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partitionSet.contains(p) && p.dataType == StringType && p.nullable =>
        needConvert = true
        Alias(Empty2Null(p), p.name)()
      case attr => attr
    }

    val empty2NullPlan = if (staticPartitionWriteOnly && nativeEnabled) {
      // Velox backend only support static partition write.
      // And no need to add sort operator for static partition write.
      plan
    } else {
      if (needConvert) ProjectExec(projectList, plan) else plan
    }

    val writerBucketSpec = bucketSpec.map {
      spec =>
        val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)

        if (
          options.getOrElse(BucketingUtils.optionForHiveCompatibleBucketWrite, "false") ==
            "true"
        ) {
          // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression.
          // Without the extra bitwise-and operation, we can get wrong bucket id when hash value of
          // columns is negative. See Hive implementation in
          // `org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#getBucketNumber()`.
          val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
          val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

          // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
          // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
          //
          // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
          // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
          val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
          WriterBucketSpec(bucketIdExpression, fileNamePrefix)
        } else {
          // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
          // expression, so that we can guarantee the data distribution is same between shuffle and
          // bucketed data source, which enables us to only shuffle one side when join a bucketed
          // table and a normal one.
          val bucketIdExpression =
            HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
          WriterBucketSpec(bucketIdExpression, (_: Int) => "")
        }
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    DataSourceUtils.verifySchema(fileFormat, dataSchema)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = finalOutputSpec.outputPath,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions
        .get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = empty2NullPlan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    def nativeWrap(plan: SparkPlan) = {
      var wrapped: SparkPlan = plan
      if (writerBucketSpec.isDefined) {
        // We need to add the bucket id expression to the output of the sort plan,
        // so that we can use backend to calculate the bucket id for each row.
        wrapped = ProjectExec(
          wrapped.output :+ Alias(writerBucketSpec.get.bucketIdExpression, "__bucket_value__")(),
          wrapped)
        // TODO: to optimize, bucket value is computed twice here
      }

      val nativeFormat = sparkSession.sparkContext.getLocalProperty("nativeFormat")
      if ("parquet".equals(nativeFormat)) {
        (GlutenParquetWriterInjects.getInstance().executeWriterWrappedSparkPlan(wrapped), None)
      } else if ("orc".equals(nativeFormat)) {
        (GlutenOrcWriterInjects.getInstance().executeWriterWrappedSparkPlan(wrapped), None)
      } else {
        (GlutenTextWriterInjects.getInstance().executeWriterWrappedSparkPlan(wrapped), None)
      }
    }

    try {
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched) {
        if (!nativeEnabled || (staticPartitionWriteOnly && nativeEnabled)) {
          (empty2NullPlan.execute(), None)
        } else {
          nativeWrap(empty2NullPlan)
        }
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = bindReferences(
          requiredOrdering.map(SortOrder(_, Ascending)),
          finalOutputSpec.outputColumns)
        val sortPlan = SortExec(orderingExpr, global = false, child = empty2NullPlan)

        val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
        var concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
        if (nativeEnabled && concurrentWritersEnabled) {
          log.warn(
            s"spark.sql.maxConcurrentOutputFileWriters(being set to $maxWriters) will be " +
              "ignored when native writer is being active. No concurrent Writers.")
          concurrentWritersEnabled = false
        }

        if (concurrentWritersEnabled) {
          (
            empty2NullPlan.execute(),
            Some(ConcurrentOutputWriterSpec(maxWriters, () => sortPlan.createSorter())))
        } else {
          if (staticPartitionWriteOnly && nativeEnabled) {
            // remove the sort operator for static partition write.
            (empty2NullPlan.execute(), None)
          } else {
            if (!nativeEnabled) {
              (sortPlan.execute(), None)
            } else {
              nativeWrap(sortPlan)
            }
          }
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        rdd
      }

      val jobIdInstant = new Date().getTime
      val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            jobIdInstant = jobIdInstant,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec
          )
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        }
      )

      val commitMsgs = ret.map(_.commitMsg)

      logInfo(s"Start to commit write Job ${description.uuid}.")
      val (_, duration) = Utils.timeTakenMs(committer.commitJob(job, commitMsgs))
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${description.uuid}.", cause)
        committer.abortJob(job)
        throw QueryExecutionErrors.jobAbortedError(cause)
    }
  }
  // scalastyle:on argcount

  /** Writes data out in a single Spark task. */
  private def executeTask(
      description: WriteJobDescription,
      jobIdInstant: Long,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow],
      concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec]): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date(jobIdInstant), sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

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

    committer.setupTask(taskAttemptContext)

    val dataWriter =
      if (sparkPartitionId != 0 && !iterator.hasNext) {
        // In case of empty job,
        // leave first partition to save meta for file format like parquet/orc.
        new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new DynamicPartitionDataConcurrentWriter(
              description,
              taskAttemptContext,
              committer,
              spec)
          case _ =>
            new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
        }
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        dataWriter.writeWithIterator(iterator)
        dataWriter.commit()
      })(
        catchBlock = {
          // If there is an error, abort the task
          dataWriter.abort()
          logError(s"Job $jobId aborted.")
        },
        finallyBlock = {
          dataWriter.close()
        })
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        // If any output file to write already exists, it does not make sense to re-run this task.
        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(t)
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it the
   * corresponding [[WriteTaskStats]] from all executors.
   */
  private[datasources] def processStats(
      statsTrackers: Seq[WriteJobStatsTracker],
      statsPerTask: Seq[Seq[WriteTaskStats]],
      jobCommitDuration: Long): Unit = {

    val numStatsTrackers = statsTrackers.length
    assert(
      statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin
    )

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }
}
