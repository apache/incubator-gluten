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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.clickhouse.CHConf

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.{DelayedCommitProtocol, DeltaFileFormatWriter, MergeTreeDelayedCommitProtocol, TransactionalWrite}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.schema.{InnerInvariantViolationException, InvariantViolationException}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.execution.{CHDelayedCommitProtocol, QueryExecution, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, GlutenWriterColumnarRules, WriteFiles, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.v1.MergeTreeWriterInjects
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeFileFormatWriter
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer

object ClickhouseOptimisticTransaction {}

class ClickhouseOptimisticTransaction(
    override val deltaLog: DeltaLog,
    override val catalogTable: Option[CatalogTable],
    override val snapshot: Snapshot)
  extends OptimisticTransaction(deltaLog, catalogTable, snapshot) {

  def this(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None) = {
    this(
      deltaLog,
      catalogTable,
      snapshotOpt.getOrElse(deltaLog.update())
    )
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    if (ClickHouseConfig.isMergeTreeFormatEngine(metadata.configuration)) {
      hasWritten = true

      val spark = inputData.sparkSession
      val (data, partitionSchema) = performCDCPartition(inputData)
      val outputPath = deltaLog.dataPath

      val (queryExecution, output, generatedColumnConstraints, _) =
        normalizeData(deltaLog, writeOptions, data)

      val tableV2 = ClickHouseTableV2.getTable(deltaLog)
      val committer =
        new MergeTreeDelayedCommitProtocol(
          outputPath.toString,
          None,
          None,
          tableV2.dataBaseName,
          tableV2.tableName)

      // val (optionalStatsTracker, _) =
      //   getOptionalStatsTrackerAndStatsCollection(output, outputPath, partitionSchema, data)
      val (optionalStatsTracker, _) = (None, None)

      val constraints =
        Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

      SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
        val queryPlan = queryExecution.executedPlan
        val (newQueryPlan, newOutput) =
          MergeTreeWriterInjects.insertFakeRowAdaptor(queryPlan, output)
        val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, newOutput)
        val partitioningColumns = getPartitioningColumns(partitionSchema, newOutput)

        val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

        if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
          val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
            new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
            BasicWriteJobStatsTracker.metrics)
          //        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
          statsTrackers.append(basicWriteJobStatsTracker)
        }

        // Iceberg spec requires partition columns in data files
        val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata)
        // Retain only a minimal selection of Spark writer options to avoid any potential
        // compatibility issues
        var options = (writeOptions match {
          case None => Map.empty[String, String]
          case Some(writeOptions) =>
            writeOptions.options.filterKeys {
              key =>
                key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
                key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
            }.toMap
        }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)

        spark.conf.getAll.foreach(
          entry => {
            if (
              CHConf.startWithSettingsPrefix(entry._1)
              || entry._1.equalsIgnoreCase(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE.key)
            ) {
              options += (entry._1 -> entry._2)
            }
          })

        try {
          val format = tableV2.getFileFormat(protocol, metadata)
          GlutenWriterColumnarRules.injectSparkLocalProperty(spark, Some(format.shortName()))
          MergeTreeFileFormatWriter.write(
            sparkSession = spark,
            plan = newQueryPlan,
            fileFormat = format,
            // formats.
            committer = committer,
            outputSpec = outputSpec,
            // scalastyle:off deltahadoopconfiguration
            hadoopConf = spark.sessionState
              .newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
            // scalastyle:on deltahadoopconfiguration
            partitionColumns = partitioningColumns,
            bucketSpec =
              tableV2.normalizedBucketSpec(output.map(_.name), spark.sessionState.conf.resolver),
            statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
            options = options,
            constraints = constraints
          )
        } catch {
          case s: SparkException =>
            // Pull an InvariantViolationException up to the top level if it was the root cause.
            val violationException = ExceptionUtils.getRootCause(s)
            if (violationException.isInstanceOf[InvariantViolationException]) {
              throw violationException
            } else {
              throw s
            }
        } finally {
          GlutenWriterColumnarRules.injectSparkLocalProperty(spark, None)
        }
      }
      committer.addedStatuses.toSeq ++ committer.changeFiles
    } else {
      // TODO: support native delta parquet write
      // 1. insert FakeRowAdaptor
      // 2. DeltaInvariantCheckerExec transform
      // 3. DeltaTaskStatisticsTracker collect null count / min values / max values
      // 4. set the parameters 'staticPartitionWriteOnly', 'isNativeApplicable',
      //    'nativeFormat' in the LocalProperty of the sparkcontext
      super.writeFiles(inputData, writeOptions, additionalConstraints)
    }
  }

  private def shouldOptimizeWrite(
      writeOptions: Option[DeltaOptions],
      sessionConf: SQLConf): Boolean = {
    writeOptions
      .flatMap(_.optimizeWrite)
      .getOrElse(TransactionalWrite.shouldOptimizeWrite(metadata, sessionConf))
  }

  override protected def getCommitter(outputPath: Path): DelayedCommitProtocol =
    new CHDelayedCommitProtocol("delta", outputPath.toString, None, deltaDataSubdir)

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {

    if (isOptimize)
      throw new UnsupportedOperationException("Optimize is not supported for ClickHouse")

    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val fileFormat = deltaLog.fileFormat(protocol, metadata) // TODO support changing formats.

    // Iceberg spec requires partition columns in data files
    val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata)
    // Retain only a minimal selection of Spark writer options to avoid any potential
    // compatibility issues
    val options = (writeOptions match {
      case None => Map.empty[String, String]
      case Some(writeOptions) =>
        writeOptions.options.filterKeys {
          key =>
            key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
            key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
        }.toMap
    }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)

    val (normalQueryExecution, output, generatedColumnConstraints, _) =
      normalizeData(deltaLog, writeOptions, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val logicalPlan = normalQueryExecution.optimizedPlan
    val write =
      WriteFiles(logicalPlan, fileFormat, partitioningColumns, None, options, Map.empty)

    val queryExecution = new QueryExecution(spark, write)
    val committer = getCommitter(outputPath)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    //    val (optionalStatsTracker, _) =
    //      getOptionalStatsTrackerAndStatsCollection(output, outputPath, partitionSchema, data)
    val optionalStatsTracker: Option[DeltaJobStatisticsTracker] = None

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val physicalPlan = materializeAdaptiveSparkPlan(queryExecution.executedPlan)
      // convertEmptyToNullIfNeeded(queryExecution.executedPlan, partitioningColumns, constraints)
      /*      val checkInvariants = DeltaInvariantCheckerExec(empty2NullPlan, constraints)
      // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
      // evenly-balanced data files already.
      val physicalPlan =
        if (
          !isOptimize &&
          shouldOptimizeWrite(writeOptions, spark.sessionState.conf)
        ) {
          DeltaOptimizedWriterExec(checkInvariants, metadata.partitionColumns, deltaLog)
        } else {
          checkInvariants
        }*/
      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      try {
        DeltaFileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTracker.toSeq
            ++ statsTrackers,
          options = options
        )
      } catch {
        case InnerInvariantViolationException(violationException) =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          throw violationException
      }
    }

    var resultFiles =
      (if (optionalStatsTracker.isDefined) {
         committer.addedStatuses.map {
           a =>
             a.copy(stats =
               optionalStatsTracker.map(_.recordedStats(a.toPath.getName)).getOrElse(a.stats))
         }
       } else {
         committer.addedStatuses
       })
        .filter {
          // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
          // may be fixed in the future) are the MERGE command when you delete with empty source, or
          // empty target, or on disjoint tables. This is hard to catch before the write without
          // collecting the DF ahead of time. Instead, we can return only the AddFiles that
          // a) actually add rows, or
          // b) don't have any stats so we don't know the number of rows at all
          case a: AddFile => a.numLogicalRecords.forall(_ > 0)
          case _ => true
        }

    // add [[AddFile.Tags.ICEBERG_COMPAT_VERSION.name]] tags to addFiles
    if (IcebergCompatV2.isEnabled(metadata)) {
      resultFiles = resultFiles.map {
        addFile =>
          val tags = if (addFile.tags != null) addFile.tags else Map.empty[String, String]
          addFile.copy(tags = tags + (AddFile.Tags.ICEBERG_COMPAT_VERSION.name -> "2"))
      }
    }

    if (resultFiles.nonEmpty && !isOptimize) registerPostCommitHook(AutoCompact)

    resultFiles.toSeq ++ committer.changeFiles
  }

  private def materializeAdaptiveSparkPlan(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
    case p: SparkPlan => p
  }
}
