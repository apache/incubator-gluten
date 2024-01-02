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
package org.apache.spark.sql.execution.datasources.v1.clickhouse

import io.glutenproject.execution.ColumnarToRowExecBase

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{FileAction, Metadata}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.MergeTreeCommitProtocol
import org.apache.spark.sql.delta.schema.{InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.{ProjectExec, QueryExecution, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FakeRowAdaptor, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.{runtimeMirror, typeOf, TermName}

/** Reference to the 'TransactionalWrite' of the delta */
object MergeTreeDeltaTxnWriter extends Logging {

  def performCDCPartition(
      txn: OptimisticTransaction,
      inputData: Dataset[_]): (DataFrame, StructType) = {
    // If this is a CDC write, we need to generate the CDC_PARTITION_COL in order to properly
    // dispatch rows between the main table and CDC event records. This is a virtual partition
    // and will be stripped out later in [[DelayedCommitProtocolEdge]].
    // Note that the ordering of the partition schema is relevant - CDC_PARTITION_COL must
    // come first in order to ensure CDC data lands in the right place.
    if (
      CDCReader.isCDCEnabledOnTable(txn.metadata) &&
      inputData.schema.fieldNames.contains(CDCReader.CDC_TYPE_COLUMN_NAME)
    ) {
      val augmentedData = inputData.withColumn(
        CDCReader.CDC_PARTITION_COL,
        col(CDCReader.CDC_TYPE_COLUMN_NAME).isNotNull)
      val partitionSchema = StructType(
        StructField(
          CDCReader.CDC_PARTITION_COL,
          StringType) +: txn.metadata.physicalPartitionSchema)
      (augmentedData, partitionSchema)
    } else {
      (inputData.toDF(), txn.metadata.physicalPartitionSchema)
    }
  }

  def makeOutputNullable(output: Seq[Attribute]): Seq[Attribute] = {
    output.map {
      case ref: AttributeReference =>
        val nullableDataType = SchemaUtils.typeAsNullable(ref.dataType)
        ref.copy(dataType = nullableDataType, nullable = true)(ref.exprId, ref.qualifier)
      case attr => attr.withNullability(true)
    }
  }

  def checkPartitionColumns(
      partitionSchema: StructType,
      output: Seq[Attribute],
      colsDropped: Boolean): Unit = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map {
      col =>
        // schema is already normalized, therefore we can do an equality check
        output
          .find(f => f.name == col.name)
          .getOrElse(
            throw DeltaErrors.partitionColumnNotFoundException(col.name, output)
          )
    }
    if (partitionColumns.nonEmpty && partitionColumns.length == output.length) {
      throw DeltaErrors.nonPartitionColumnAbsentException(colsDropped)
    }
  }

  def mapColumnAttributes(
      metadata: Metadata,
      output: Seq[Attribute],
      mappingMode: DeltaColumnMappingMode): Seq[Attribute] = {
    DeltaColumnMapping.createPhysicalAttributes(output, metadata.schema, mappingMode)
  }

  def normalizeData(
      txn: OptimisticTransaction,
      metadata: Metadata,
      deltaLog: DeltaLog,
      data: Dataset[_]): (QueryExecution, Seq[Attribute], Seq[Constraint], Set[String]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val enforcesDefaultExprs =
      ColumnWithDefaultExprUtils.tableHasDefaultExpr(txn.protocol, metadata)
    val (dataWithDefaultExprs, generatedColumnConstraints, trackHighWaterMarks) =
      if (enforcesDefaultExprs) {
        ColumnWithDefaultExprUtils.addDefaultExprsOrReturnConstraints(
          deltaLog,
          // We need the original query execution if this is a streaming query, because
          // `normalizedData` may add a new projection and change its type.
          data.queryExecution,
          metadata.schema,
          normalizedData
        )
      } else {
        (normalizedData, Nil, Set[String]())
      }
    val cleanedData = SchemaUtils.dropNullTypeColumns(dataWithDefaultExprs)
    val queryExecution = if (cleanedData.schema != dataWithDefaultExprs.schema) {
      // This must be batch execution as DeltaSink doesn't accept NullType in micro batch DataFrame.
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else if (enforcesDefaultExprs) {
      dataWithDefaultExprs.queryExecution
    } else {
      assert(
        normalizedData == dataWithDefaultExprs,
        "should not change data when there is no generate column")
      // Ideally, we should use `normalizedData`. But it may use `QueryExecution` rather than
      // `IncrementalExecution`. So we use the input `data` and leverage the `nullableOutput`
      // below to fix the column names.
      data.queryExecution
    }
    val nullableOutput = makeOutputNullable(cleanedData.queryExecution.analyzed.output)
    val columnMapping = metadata.columnMappingMode
    // Check partition column errors
    checkPartitionColumns(
      metadata.partitionSchema,
      nullableOutput,
      nullableOutput.length < data.schema.size
    )
    // Rewrite column physical names if using a mapping mode
    val mappedOutput =
      if (columnMapping == NoMapping) nullableOutput
      else {
        mapColumnAttributes(metadata, nullableOutput, columnMapping)
      }
    (queryExecution, mappedOutput, generatedColumnConstraints, trackHighWaterMarks)
  }

  def getPartitioningColumns(
      partitionSchema: StructType,
      output: Seq[Attribute]): Seq[Attribute] = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map {
      col =>
        // schema is already normalized, therefore we can do an equality check
        // we have already checked for missing columns, so the fields must exist
        output.find(f => f.name == col.name).get
    }
    partitionColumns
  }

  def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (
      !SparkSession.active.conf
        .get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)
    ) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        Alias(FileFormatWriter.Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) {
      plan match {
        case adaptor: FakeRowAdaptor =>
          adaptor.withNewChildren(Seq(ProjectExec(projectList, adaptor.child)))
        case p: SparkPlan => p
      }
    } else plan
  }

  def setOptimisticTransactionHasWritten(txn: OptimisticTransaction): Unit = {
    val txnRuntimeMirror = runtimeMirror(classOf[OptimisticTransaction].getClassLoader)
    val txnInstanceMirror = txnRuntimeMirror.reflect(txn)
    val txnHasWritten = typeOf[OptimisticTransaction].member(TermName("hasWritten_$eq")).asMethod
    val txnHasWrittenMirror = txnInstanceMirror.reflectMethod(txnHasWritten)
    txnHasWrittenMirror(true)
  }

  /** Reference to the 'TransactionalWrite.writeFiles' of the delta */
  // scalastyle:off argcount
  def writeFiles(
      txn: OptimisticTransaction,
      inputData: Dataset[_],
      deltaOptions: Option[DeltaOptions],
      writeOptions: Map[String, String],
      database: String,
      tableName: String,
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      clickhouseTableConfigs: Map[String, String],
      partitionColumns: Seq[String],
      bucketSpec: Option[BucketSpec],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    // use reflect to set the protected field: hasWritten
    setOptimisticTransactionHasWritten(txn)

    val deltaLog = txn.deltaLog
    val metadata = txn.metadata

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(txn, inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _) =
      normalizeData(txn, metadata, deltaLog, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = new MergeTreeCommitProtocol("delta-mergetree", outputPath.toString, None)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    // val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
    //   partitionSchema, data)
    val (optionalStatsTracker, _) = (None, None)

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val queryPlan = queryExecution.executedPlan
      val newQueryPlan = queryPlan match {
        // if the child is columnar, we can just wrap&transfer the columnar data
        case c2r: ColumnarToRowExecBase =>
          FakeRowAdaptor(c2r.child)
        // If the child is aqe, we make aqe "support columnar",
        // then aqe itself will guarantee to generate columnar outputs.
        // So FakeRowAdaptor will always consumes columnar data,
        // thus avoiding the case of c2r->aqe->r2c->writer
        case aqe: AdaptiveSparkPlanExec =>
          FakeRowAdaptor(
            AdaptiveSparkPlanExec(
              aqe.inputPlan,
              aqe.context,
              aqe.preprocessingRules,
              aqe.isSubquery,
              supportsColumnar = true
            ))
        case other => queryPlan.withNewChildren(Array(FakeRowAdaptor(other)))
      }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        // registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions.filterKeys {
        key =>
          key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
          key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
      }.toMap

      try {
        MergeTreeFileFormatWriter.write(
          sparkSession = spark,
          plan = newQueryPlan,
          fileFormat = new DeltaMergeTreeFileFormat(
            metadata,
            database,
            tableName,
            output,
            orderByKeyOption,
            primaryKeyOption,
            clickhouseTableConfigs,
            partitionColumns),
          // formats.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          orderByKeyOption = orderByKeyOption,
          primaryKeyOption = primaryKeyOption,
          partitionColumns = partitioningColumns,
          bucketSpec = bucketSpec,
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
      }
    }

    // val resultFiles = committer.addedStatuses.map { a =>
    //   a.copy(stats = optionalStatsTracker.map(
    //    _.recordedStats(new Path(new URI(a.path)).getName)).getOrElse(a.stats))
    /* val resultFiles = committer.addedStatuses.filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    } */

    committer.addedStatuses.toSeq ++ committer.changeFiles
  }
  // scalastyle:on argcount
}
