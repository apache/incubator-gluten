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
package org.apache.spark.sql.execution.datasources.v1.clickhouse.commands

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeltaCommand}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.delta.constraints.Constraints.Check
import org.apache.spark.sql.delta.constraints.Invariants.ArbitraryExpression
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeDeltaTxnWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

case class WriteMergeTreeToDelta(
    deltaLog: DeltaLog,
    mode: SaveMode,
    options: DeltaOptions,
    writeOptions: Map[String, String],
    sqlConf: SQLConf,
    database: String,
    tableName: String,
    orderByKeyOption: Option[Seq[String]],
    primaryKeyOption: Option[Seq[String]],
    clickhouseTableConfigs: Map[String, String],
    partitionColumns: Seq[String],
    bucketSpec: Option[BucketSpec],
    data: DataFrame,
    info: LogicalWriteInfo,
    schemaInCatalog: Option[StructType] = None)
  extends LeafRunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty

  lazy val configuration: Map[String, String] =
    deltaLog.unsafeVolatileSnapshot.metadata.configuration

  override def run(sparkSession: SparkSession): Seq[Row] = {
    deltaLog.withNewTransaction {
      txn =>
        // If this batch has already been executed within this query, then return.
        var skipExecution = hasBeenExecuted(txn)
        if (skipExecution) {
          return Seq.empty
        }

        val actions = write(txn, sparkSession)
        val operation = DeltaOperations.Write(
          mode,
          Option(partitionColumns),
          options.replaceWhere,
          options.userMetadata)
        txn.commit(actions, operation)
    }
    Seq.empty
  }

  // TODO: replace the method below with `CharVarcharUtils.replaceCharWithVarchar`, when 3.3 is out.
  import org.apache.spark.sql.types.{ArrayType, CharType, DataType, MapType, VarcharType}

  private def replaceCharWithVarchar(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharWithVarchar(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharWithVarchar(kt), replaceCharWithVarchar(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map {
        field => field.copy(dataType = replaceCharWithVarchar(field.dataType))
      })
    case CharType(length) => VarcharType(length)
    case _ => dt
  }

  /**
   * Replace where operationMetrics need to be recorded separately.
   *
   * @param newFiles
   *   \- AddFile and AddCDCFile added by write job
   * @param deleteActions
   *   \- AddFile, RemoveFile, AddCDCFile added by Delete job
   */
  private def registerReplaceWhereMetrics(
      spark: SparkSession,
      txn: OptimisticTransaction,
      newFiles: Seq[Action],
      deleteActions: Seq[Action]): Unit = {
    var numFiles = 0L
    var numCopiedRows = 0L
    var numOutputBytes = 0L
    var numNewRows = 0L
    var numAddedChangedFiles = 0L
    var hasRowLevelMetrics = true

    newFiles.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numNewRows += a.numLogicalRecords.get
        }
      case cdc: AddCDCFile =>
        numAddedChangedFiles += 1
      case _ =>
    }

    deleteActions.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numCopiedRows += a.numLogicalRecords.get
        }
      case cdc: AddCDCFile =>
        numAddedChangedFiles += 1
      // Remove metrics will be handled by the delete command.
      case _ =>
    }

    var sqlMetrics = Map(
      "numFiles" -> new SQLMetric("number of files written", numFiles),
      "numOutputBytes" -> new SQLMetric("number of output bytes", numOutputBytes),
      "numAddedChangeFiles" -> new SQLMetric("number of change files added", numAddedChangedFiles)
    )
    if (hasRowLevelMetrics) {
      sqlMetrics ++= Map(
        "numOutputRows" -> new SQLMetric("number of rows added", numNewRows + numCopiedRows),
        "numCopiedRows" -> new SQLMetric("number of copied rows", numCopiedRows)
      )
    } else {
      // this will get filtered out in DeltaOperations.WRITE transformMetrics
      sqlMetrics ++= Map(
        "numOutputRows" -> new SQLMetric("number of rows added", 0L),
        "numCopiedRows" -> new SQLMetric("number of copied rows", 0L)
      )
    }
    txn.registerSQLMetrics(spark, sqlMetrics)
  }

  def write(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {
    import org.apache.spark.sql.delta.implicits._
    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw DeltaErrors.pathAlreadyExistsException(deltaLog.dataPath)
      } else if (mode == SaveMode.Ignore) {
        return Nil
      } else if (mode == SaveMode.Overwrite) {
        deltaLog.assertRemovable()
      }
    }
    val rearrangeOnly = options.rearrangeOnly
    // Delta does not support char padding and we should only have varchar type. This does not
    // change the actual behavior, but makes DESC TABLE to show varchar instead of char.
    val dataSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      replaceCharWithVarchar(CharVarcharUtils.getRawSchema(data.schema)).asInstanceOf[StructType])
    var finalSchema = schemaInCatalog.getOrElse(dataSchema)
    updateMetadata(
      data.sparkSession,
      txn,
      finalSchema,
      partitionColumns,
      configuration,
      isOverwriteOperation,
      rearrangeOnly)

    val replaceOnDataColsEnabled =
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED)

    val useDynamicPartitionOverwriteMode = {
      if (txn.metadata.partitionColumns.isEmpty) {
        // We ignore dynamic partition overwrite mode for non-partitioned tables
        false
      } else if (options.replaceWhere.nonEmpty) {
        if (options.partitionOverwriteModeInOptions && options.isDynamicPartitionOverwriteMode) {
          // replaceWhere and dynamic partition overwrite conflict because they both specify which
          // data to overwrite. We throw an error when:
          // 1. replaceWhere is provided in a DataFrameWriter option
          // 2. partitionOverwriteMode is set to "dynamic" in a DataFrameWriter option
          throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
        } else {
          // If replaceWhere is provided, we do not use dynamic partition overwrite, even if it's
          // enabled in the spark session configuration, since generally query-specific configs take
          // precedence over session configs
          false
        }
      } else options.isDynamicPartitionOverwriteMode
    }

    // Validate partition predicates
    var containsDataFilters = false
    val replaceWhere = options.replaceWhere.flatMap {
      replace =>
        val parsed = parsePredicates(sparkSession, replace)
        if (replaceOnDataColsEnabled) {
          // Helps split the predicate into separate expressions
          val (metadataPredicates, dataFilters) = DeltaTableUtils.splitMetadataAndDataPredicates(
            parsed.head,
            txn.metadata.partitionColumns,
            sparkSession)
          if (rearrangeOnly && dataFilters.nonEmpty) {
            throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters.mkString(","))
          }
          containsDataFilters = dataFilters.nonEmpty
          Some(metadataPredicates ++ dataFilters)
        } else if (mode == SaveMode.Overwrite) {
          verifyPartitionPredicates(sparkSession, txn.metadata.partitionColumns, parsed)
          Some(parsed)
        } else {
          None
        }
    }

    if (txn.readVersion < 0) {
      // Initialize the log path
      deltaLog.createLogDirectory()
    }

    val (newFiles, addFiles, deletedFiles) = (mode, replaceWhere) match {
      case (SaveMode.Overwrite, Some(predicates)) if !replaceOnDataColsEnabled =>
        // fall back to match on partition cols only when replaceArbitrary is disabled.
        val newFiles = txn.writeFiles(data, Some(options))
        val addFiles = newFiles.collect { case a: AddFile => a }
        // Check to make sure the files we wrote out were actually valid.
        val matchingFiles = DeltaLog
          .filterFileList(txn.metadata.partitionSchema, addFiles.toDF(sparkSession), predicates)
          .as[AddFile]
          .collect()
        val invalidFiles = addFiles.toSet -- matchingFiles
        if (invalidFiles.nonEmpty) {
          val badPartitions = invalidFiles
            .map(_.partitionValues)
            .map {
              _.map { case (k, v) => s"$k=$v" }.mkString("/")
            }
            .mkString(", ")
          throw DeltaErrors.replaceWhereMismatchException(options.replaceWhere.get, badPartitions)
        }
        (newFiles, addFiles, txn.filterFiles(predicates).map(_.remove))
      case (SaveMode.Overwrite, Some(condition)) if txn.snapshot.version >= 0 =>
        val constraints = extractConstraints(sparkSession, condition)

        val removedFileActions = removeFiles(sparkSession, txn, condition)
        val cdcExistsInRemoveOp = removedFileActions.exists(_.isInstanceOf[AddCDCFile])

        // The above REMOVE will not produce explicit CDF data when persistent DV is enabled.
        // Therefore here we need to decide whether to produce explicit CDF for INSERTs, because
        // the CDF protocol requires either (i) all CDF data are generated explicitly as AddCDCFile,
        // or (ii) all CDF data can be deduced from [[AddFile]] and [[RemoveFile]].
        val dataToWrite =
          if (
            containsDataFilters && CDCReader.isCDCEnabledOnTable(txn.metadata) &&
            sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_WITH_CDF_ENABLED) &&
            cdcExistsInRemoveOp
          ) {
            var dataWithDefaultExprs = data

            // pack new data and cdc data into an array of structs and unpack them into rows
            // to share values in outputCols on both branches, avoiding re-evaluating
            // non-deterministic expression twice.
            val outputCols = dataWithDefaultExprs.schema.map(SchemaUtils.fieldToColumn(_))
            val insertCols = outputCols :+
              lit(CDCReader.CDC_TYPE_INSERT).as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val insertDataCols = outputCols :+
              new Column(CDCReader.CDC_TYPE_NOT_CDC)
                .as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val packedInserts = array(
              struct(insertCols: _*),
              struct(insertDataCols: _*)
            ).expr

            dataWithDefaultExprs
              .select(explode(new Column(packedInserts)).as("packedData"))
              .select((dataWithDefaultExprs.schema.map(_.name) :+ CDCReader.CDC_TYPE_COLUMN_NAME)
                .map(n => col(s"packedData.`$n`").as(n)): _*)
          } else {
            data
          }
        val newFiles =
          try txn.writeFiles(dataToWrite, Some(options), constraints)
          catch {
            case e: InvariantViolationException =>
              throw DeltaErrors.replaceWhereMismatchException(options.replaceWhere.get, e)
          }
        (newFiles, newFiles.collect { case a: AddFile => a }, removedFileActions)
      case (SaveMode.Overwrite, None) =>
        val newFiles = txn.writeFiles(data, Some(options))
        val addFiles = newFiles.collect { case a: AddFile => a }
        val deletedFiles = if (useDynamicPartitionOverwriteMode) {
          // with dynamic partition overwrite for any partition that is being written to all
          // existing data in that partition will be deleted.
          // the selection what to delete is on the next two lines
          val updatePartitions = addFiles.map(_.partitionValues).toSet
          txn.filterFiles(updatePartitions).map(_.remove)
        } else {
          txn.filterFiles().map(_.remove)
        }
        (newFiles, addFiles, deletedFiles)
      case _ =>
        val newFiles = MergeTreeDeltaTxnWriter
          .writeFiles(
            txn,
            data,
            Some(options),
            writeOptions,
            database,
            tableName,
            orderByKeyOption,
            primaryKeyOption,
            clickhouseTableConfigs,
            partitionColumns,
            bucketSpec,
            Seq.empty)
        (newFiles, newFiles.collect { case a: AddFile => a }, Nil)
    }

    // Need to handle replace where metrics separately.
    if (
      replaceWhere.nonEmpty && replaceOnDataColsEnabled &&
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_METRICS_ENABLED)
    ) {
      registerReplaceWhereMetrics(sparkSession, txn, newFiles, deletedFiles)
    }

    val fileActions = if (rearrangeOnly) {
      val changeFiles = newFiles.collect { case c: AddCDCFile => c }
      if (changeFiles.nonEmpty) {
        throw DeltaErrors.unexpectedChangeFilesFound(changeFiles.mkString("\n"))
      }
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map {
          case add: AddFile => add.copy(dataChange = !rearrangeOnly)
          case remove: RemoveFile => remove.copy(dataChange = !rearrangeOnly)
          case other => throw DeltaErrors.illegalFilesFound(other.toString)
        }
    } else {
      newFiles ++ deletedFiles
    }
    var setTxns = createSetTransaction()
    setTxns.toSeq ++ fileActions
  }

  private def extractConstraints(
      sparkSession: SparkSession,
      expr: Seq[Expression]): Seq[Constraint] = {
    if (!sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED)) {
      Seq.empty
    } else {
      expr.flatMap {
        e =>
          // While writing out the new data, we only want to enforce constraint on expressions
          // with UnresolvedAttribute, that is, containing column name. Because we parse a
          // predicate string without analyzing it, if there's a column name, it has to be
          // unresolved.
          e.collectFirst {
            case _: UnresolvedAttribute =>
              val arbitraryExpression = ArbitraryExpression(e)
              Check(arbitraryExpression.name, arbitraryExpression.expression)
          }
      }
    }
  }

  private def removeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): Seq[Action] = {
    val relation = LogicalRelation(
      txn.deltaLog.createRelation(snapshotToUseOpt = Some(txn.snapshot)))
    val processedCondition = condition.reduceOption(And)
    val command = spark.sessionState.analyzer.execute(
      DeleteFromTable(relation, processedCondition.getOrElse(Literal.TrueLiteral)))
    spark.sessionState.analyzer.checkAnalysis(command)
    command.asInstanceOf[DeleteCommand].performDelete(spark, txn.deltaLog, txn)
  }

  /**
   * Returns true if there is information in the spark session that indicates that this write, which
   * is part of a streaming query and a batch, has already been successfully written.
   */
  private def hasBeenExecuted(txn: OptimisticTransaction): Boolean = {
    val txnVersion = options.txnVersion
    val txnAppId = options.txnAppId
    for (v <- txnVersion; a <- txnAppId) {
      val currentVersion = txn.txnVersion(a)
      if (currentVersion >= v) {
        logInfo(
          s"Transaction write of version $v for application id $a " +
            s"has already been committed in Delta table id ${txn.deltaLog.tableId}. " +
            s"Skipping this write.")
        return true
      }
    }
    false
  }

  /**
   * Returns SetTransaction if a valid app ID and version are present. Otherwise returns an empty
   * list.
   */
  private def createSetTransaction(): Option[SetTransaction] = {
    val txnVersion = options.txnVersion
    val txnAppId = options.txnAppId
    for (v <- txnVersion; a <- txnAppId) {
      return Some(SetTransaction(a, v, Some(deltaLog.clock.getTimeMillis())))
    }
    None
  }
}
