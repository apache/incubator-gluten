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

package org.apache.spark.sql.delta.commands

import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.skipping.MultiDimClustering
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.files.SQLMetricsReporting
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{AnalysisException, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.types._
import org.apache.spark.util.{SystemClock, ThreadUtils}

/**
 * Gluten overwrite Delta:
 *
 * This file is copied from Delta 3.2.0. It is modified in:
 *   1. getDeltaTable supports to get ClickHouseTableV2
 *   2. runOptimizeBinJobClickhouse
 *   3. groupFilesIntoBinsClickhouse
 */

/** Base class defining abstract optimize command */
abstract class OptimizeTableCommandBase extends RunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType)(),
    AttributeReference("metrics", Encoders.product[OptimizeMetrics].schema)())

  /**
   * Validates ZOrderBy columns
   * - validates that partitions columns are not used in `unresolvedZOrderByCols`
   * - validates that we already collect stats for all the columns used in `unresolvedZOrderByCols`
   *
   * @param spark [[SparkSession]] to use
   * @param txn the [[OptimisticTransaction]] being used to optimize
   * @param unresolvedZOrderByCols Seq of [[UnresolvedAttribute]] corresponding to zOrderBy columns
   */
  def validateZorderByColumns(
      spark: SparkSession,
      txn: OptimisticTransaction,
      unresolvedZOrderByCols: Seq[UnresolvedAttribute]): Unit = {
    if (unresolvedZOrderByCols.isEmpty) return
    val metadata = txn.snapshot.metadata
    val partitionColumns = metadata.partitionColumns.toSet
    val dataSchema =
      StructType(metadata.schema.filterNot(c => partitionColumns.contains(c.name)))
    val df = spark.createDataFrame(new java.util.ArrayList[Row](), dataSchema)
    val checkColStat = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK)
    val statCollectionSchema = txn.snapshot.statCollectionLogicalSchema
    val colsWithoutStats = ArrayBuffer[String]()

    unresolvedZOrderByCols.foreach { colAttribute =>
      val colName = colAttribute.name
      if (checkColStat) {
        try {
          SchemaUtils.findColumnPosition(colAttribute.nameParts, statCollectionSchema)
        } catch {
          case e: AnalysisException if e.getMessage.contains("Couldn't find column") =>
            colsWithoutStats.append(colName)
        }
      }
      val isNameEqual = spark.sessionState.conf.resolver
      if (partitionColumns.find(isNameEqual(_, colName)).nonEmpty) {
        throw DeltaErrors.zOrderingOnPartitionColumnException(colName)
      }
      if (df.queryExecution.analyzed.resolve(colAttribute.nameParts, isNameEqual).isEmpty) {
        throw DeltaErrors.zOrderingColumnDoesNotExistException(colName)
      }
    }
    if (checkColStat && colsWithoutStats.nonEmpty) {
      throw DeltaErrors.zOrderingOnColumnWithNoStatsException(
        colsWithoutStats.toSeq, spark)
    }
  }
}

object OptimizeTableCommand {
  /**
   * Alternate constructor that converts a provided path or table identifier into the
   * correct child LogicalPlan node. If both path and tableIdentifier are specified (or
   * if both are None), this method will throw an exception. If a table identifier is
   * specified, the child LogicalPlan will be an [[UnresolvedTable]] whereas if a path
   * is specified, it will be an [[UnresolvedPathBasedDeltaTable]].
   *
   * Note that the returned OptimizeTableCommand will have an *unresolved* child table
   * and hence, the command needs to be analyzed before it can be executed.
   */
  def apply(
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      userPartitionPredicates: Seq[String],
      optimizeContext: DeltaOptimizeContext = DeltaOptimizeContext())(
      zOrderBy: Seq[UnresolvedAttribute]): OptimizeTableCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, "OPTIMIZE")
    OptimizeTableCommand(plan, userPartitionPredicates, optimizeContext)(zOrderBy)
  }
}

/**
 * The `optimize` command implementation for Spark SQL. Example SQL:
 * {{{
 *    OPTIMIZE ('/path/to/dir' | delta.table) [WHERE part = 25];
 * }}}
 */
case class OptimizeTableCommand(
    override val child: LogicalPlan,
    userPartitionPredicates: Seq[String],
    optimizeContext: DeltaOptimizeContext
)(val zOrderBy: Seq[UnresolvedAttribute])
  extends OptimizeTableCommandBase with RunnableCommand with UnaryNode {

  override val otherCopyArgs: Seq[AnyRef] = zOrderBy :: Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): OptimizeTableCommand =
    copy(child = newChild)(zOrderBy)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // --- modified start
    val table = OptimizeTableCommandOverwrites.getDeltaTable(child, "OPTIMIZE")
    // --- modified end

    val txn = table.startTransaction()
    if (txn.readVersion == -1) {
      throw DeltaErrors.notADeltaTableException(table.deltaLog.dataPath.toString)
    }

    if (ClusteredTableUtils.isSupported(txn.protocol)) {
      if (userPartitionPredicates.nonEmpty) {
        throw DeltaErrors.clusteringWithPartitionPredicatesException(userPartitionPredicates)
      }
      if (zOrderBy.nonEmpty) {
        throw DeltaErrors.clusteringWithZOrderByException(zOrderBy)
      }
    }

    val partitionColumns = txn.snapshot.metadata.partitionColumns
    // Parse the predicate expression into Catalyst expression and verify only simple filters
    // on partition columns are present

    val partitionPredicates = userPartitionPredicates.flatMap { predicate =>
        val predicates = parsePredicates(sparkSession, predicate)
        verifyPartitionPredicates(
          sparkSession,
          partitionColumns,
          predicates)
        predicates
    }

    validateZorderByColumns(sparkSession, txn, zOrderBy)
    val zOrderByColumns = zOrderBy.map(_.name).toSeq

    new OptimizeExecutor(
      sparkSession,
      txn,
      partitionPredicates,
      zOrderByColumns,
      isAutoCompact = false,
      optimizeContext
    ).optimize()
  }
}

/**
 * Stored all runtime context information that can control the execution of optimize.
 *
 * @param reorg The REORG operation that triggered the rewriting task, if any.
 * @param minFileSize Files which are smaller than this threshold will be selected for compaction.
 *                    If not specified, [[DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE]] will be used.
 *                    This parameter must be set to `0` when [[reorg]] is set.
 * @param maxDeletedRowsRatio Files with a ratio of soft-deleted rows to the total rows larger than
 *                            this threshold will be rewritten by the OPTIMIZE command. If not
 *                            specified, [[DeltaSQLConf.DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO]]
 *                            will be used. This parameter must be set to `0` when [[reorg]] is set.
 */
case class DeltaOptimizeContext(
    reorg: Option[DeltaReorgOperation] = None,
    minFileSize: Option[Long] = None,
    maxFileSize: Option[Long] = None,
    maxDeletedRowsRatio: Option[Double] = None) {
  if (reorg.nonEmpty) {
    require(
      minFileSize.contains(0L) && maxDeletedRowsRatio.contains(0d),
      "minFileSize and maxDeletedRowsRatio must be 0 when running REORG TABLE.")
  }
}

/**
 * Optimize job which compacts small files into larger files to reduce
 * the number of files and potentially allow more efficient reads.
 *
 * @param sparkSession Spark environment reference.
 * @param txn The transaction used to optimize this table
 * @param partitionPredicate List of partition predicates to select subset of files to optimize.
 */
class OptimizeExecutor(
    sparkSession: SparkSession,
    txn: OptimisticTransaction,
    partitionPredicate: Seq[Expression],
    zOrderByColumns: Seq[String],
    isAutoCompact: Boolean,
    optimizeContext: DeltaOptimizeContext)
  extends DeltaCommand with SQLMetricsReporting with Serializable {

  /**
   * In which mode the Optimize command is running. There are three valid modes:
   * 1. Compaction
   * 2. ZOrder
   * 3. Clustering
   */
  private val optimizeStrategy =
    OptimizeTableStrategy(sparkSession, txn.snapshot, optimizeContext, zOrderByColumns)

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = new SystemClock().getTimeMillis()

  private val isClusteredTable = ClusteredTableUtils.isSupported(txn.snapshot.protocol)

  private val isMultiDimClustering =
    optimizeStrategy.isInstanceOf[ClusteringStrategy] ||
    optimizeStrategy.isInstanceOf[ZOrderStrategy]

  private val clusteringColumns: Seq[String] = {
    if (zOrderByColumns.nonEmpty) {
      zOrderByColumns
    } else if (isClusteredTable) {
      ClusteringColumnInfo.extractLogicalNames(txn.snapshot)
    } else {
      Nil
    }
  }

  def optimize(): Seq[Row] = {
    recordDeltaOperation(txn.deltaLog, "delta.optimize") {

      // --- modified start
      val isMergeTreeFormat = ClickHouseConfig
        .isMergeTreeFormatEngine(txn.deltaLog.unsafeVolatileMetadata.configuration)
      // --- modified end

      val minFileSize = optimizeContext.minFileSize.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE))
      val maxFileSize = optimizeContext.maxFileSize.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE))
      val maxDeletedRowsRatio = optimizeContext.maxDeletedRowsRatio.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO))

      val candidateFiles = txn.filterFiles(partitionPredicate, keepNumRecords = true)
      val partitionSchema = txn.metadata.partitionSchema

      val filesToProcess = optimizeContext.reorg match {
        case Some(reorgOperation) => reorgOperation.filterFilesToReorg(txn.snapshot, candidateFiles)
        case None => filterCandidateFileList(minFileSize, maxDeletedRowsRatio, candidateFiles)
      }
      // --- modified start
      val maxThreads =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS)
      val (updates, jobs) = if (isMergeTreeFormat) {
        val partitionsToCompact = filesToProcess
          .groupBy(file => (file.asInstanceOf[AddMergeTreeParts].bucketNum, file.partitionValues))
          .toSeq
        val jobs = OptimizeTableCommandOverwrites
          .groupFilesIntoBinsClickhouse(partitionsToCompact, maxFileSize)
        val updates = ThreadUtils.parmap(jobs, "OptimizeJob", maxThreads) {
          partitionBinGroup =>
          // --- modified start
            OptimizeTableCommandOverwrites.runOptimizeBinJobClickhouse(
            txn,
            partitionBinGroup._1._2,
            partitionBinGroup._1._1,
            partitionBinGroup._2,
            maxFileSize)
          // --- modified end
        }.flatten
        // uniform the jobs type
        (updates, jobs.map(v => (v._1._2 ++ Map("bucketNum" -> v._1.toString()), v._2)))
      } else {
        val partitionsToCompact = filesToProcess.groupBy(_.partitionValues).toSeq

        val jobs = groupFilesIntoBins(partitionsToCompact)

        val updates = ThreadUtils.parmap(jobs, "OptimizeJob", maxThreads) { partitionBinGroup =>
          runOptimizeBinJob(txn, partitionBinGroup._1, partitionBinGroup._2, maxFileSize)
        }.flatten
        (updates, jobs)
      }
      // --- modified end

      val addedFiles = updates.collect { case a: AddFile => a }
      val removedFiles = updates.collect { case r: RemoveFile => r }
      val removedDVs = filesToProcess.filter(_.deletionVector != null).map(_.deletionVector).toSeq
      if (addedFiles.size > 0) {
        val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles, removedDVs)
        commitAndRetry(txn, getOperation(), updates, metrics) { newTxn =>
          val newPartitionSchema = newTxn.metadata.partitionSchema
          val candidateSetOld = candidateFiles.map(_.path).toSet
          val candidateSetNew = newTxn.filterFiles(partitionPredicate).map(_.path).toSet

          // As long as all of the files that we compacted are still part of the table,
          // and the partitioning has not changed it is valid to continue to try
          // and commit this checkpoint.
          if (candidateSetOld.subsetOf(candidateSetNew) && partitionSchema == newPartitionSchema) {
            true
          } else {
            val deleted = candidateSetOld -- candidateSetNew
            logWarning(s"The following compacted files were delete " +
              s"during checkpoint ${deleted.mkString(",")}. Aborting the compaction.")
            false
          }
        }
      }

      val optimizeStats = OptimizeStats()
      optimizeStats.addedFilesSizeStats.merge(addedFiles)
      optimizeStats.removedFilesSizeStats.merge(removedFiles)
      optimizeStats.numPartitionsOptimized = jobs.map(j => j._1).distinct.size
      optimizeStats.numBatches = jobs.size
      optimizeStats.totalConsideredFiles = candidateFiles.size
      optimizeStats.totalFilesSkipped = optimizeStats.totalConsideredFiles - removedFiles.size
      optimizeStats.totalClusterParallelism = sparkSession.sparkContext.defaultParallelism
      val numTableColumns = txn.snapshot.metadata.schema.size
      optimizeStats.numTableColumns = numTableColumns
      optimizeStats.numTableColumnsWithStats =
        DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(txn.snapshot.metadata)
          .min(numTableColumns)
      if (removedDVs.size > 0) {
        optimizeStats.deletionVectorStats = Some(DeletionVectorStats(
          numDeletionVectorsRemoved = removedDVs.size,
          numDeletionVectorRowsRemoved = removedDVs.map(_.cardinality).sum))
      }

      optimizeStrategy.updateOptimizeStats(optimizeStats, removedFiles, jobs)

      return Seq(Row(txn.deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
    }
  }

  /**
   * Helper method to prune the list of selected files based on fileSize and ratio of
   * deleted rows according to the deletion vector in [[AddFile]].
   */
  private def filterCandidateFileList(
      minFileSize: Long, maxDeletedRowsRatio: Double, files: Seq[AddFile]): Seq[AddFile] = {

    // Select all files in case of multi-dimensional clustering
    if (isMultiDimClustering) return files

    def shouldCompactBecauseOfDeletedRows(file: AddFile): Boolean = {
      // Always compact files with DVs but without numRecords stats.
      // This may be overly aggressive, but it fixes the problem in the long-term,
      // as the compacted files will have stats.
      (file.deletionVector != null && file.numPhysicalRecords.isEmpty) ||
          file.deletedToPhysicalRecordsRatio.getOrElse(0d) > maxDeletedRowsRatio
    }

    // Select files that are small or have too many deleted rows
    files.filter(
      addFile => addFile.size < minFileSize || shouldCompactBecauseOfDeletedRows(addFile))
  }

  /**
   * Utility methods to group files into bins for optimize.
   *
   * @param partitionsToCompact List of files to compact group by partition.
   *                            Partition is defined by the partition values (partCol -> partValue)
   * @return Sequence of bins. Each bin contains one or more files from the same
   *         partition and targeted for one output file.
   */
  private def groupFilesIntoBins(
      partitionsToCompact: Seq[(Map[String, String], Seq[AddFile])])
  : Seq[(Map[String, String], Seq[AddFile])] = {
    val maxBinSize = optimizeStrategy.maxBinSize
    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        val preparedFiles = optimizeStrategy.prepareFilesPerPartition(files)
        preparedFiles.foreach { file =>
          // Generally, a bin is a group of existing files, whose total size does not exceed the
          // desired maxBinSize. The output file size depends on the mode:
          // 1. Compaction: Files in a bin will be coalesced into a single output file.
          // 2. ZOrder:  all files in a partition will be read by the
          //    same job, the data will be range-partitioned and
          //    numFiles = totalFileSize / maxFileSize will be produced.
          // 3. Clustering: Files in a bin belongs to one ZCUBE, the data will be
          //    range-partitioned and numFiles = totalFileSize / maxFileSize.
          if (file.size + currentBinSize > maxBinSize) {
            bins += currentBin.toVector
            currentBin.clear()
            currentBin += file
            currentBinSize = file.size
          } else {
            currentBin += file
            currentBinSize += file.size
          }
        }

        if (currentBin.nonEmpty) {
          bins += currentBin.toVector
        }

        bins.filter { bin =>
          bin.size > 1 || // bin has more than one file or
          bin.size == 1 && optimizeContext.reorg.nonEmpty || // always rewrite files during reorg
          isMultiDimClustering // multi-clustering
        }.map(b => (partition, b))
    }
  }

  /**
   * Utility method to run a Spark job to compact the files in given bin
   *
   * @param txn [[OptimisticTransaction]] instance in use to commit the changes to DeltaLog.
   * @param partition Partition values of the partition that files in [[bin]] belongs to.
   * @param bin List of files to compact into one large file.
   * @param maxFileSize Targeted output file size in bytes
   */
  private def runOptimizeBinJob(
      txn: OptimisticTransaction,
      partition: Map[String, String],
      bin: Seq[AddFile],
      maxFileSize: Long): Seq[FileAction] = {
    val baseTablePath = txn.deltaLog.dataPath

    var input = txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))
    input = RowTracking.preserveRowTrackingColumns(input, txn.snapshot)
    val repartitionDF = if (isMultiDimClustering) {
      val totalSize = bin.map(_.size).sum
      val approxNumFiles = Math.max(1, totalSize / maxFileSize).toInt
      MultiDimClustering.cluster(
        input,
        approxNumFiles,
        clusteringColumns,
        optimizeStrategy.curve)
    } else {
      val useRepartition = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED)
      if (useRepartition) {
        input.repartition(numPartitions = 1)
      } else {
        input.coalesce(numPartitions = 1)
      }
    }

    val partitionDesc = partition.toSeq.map(entry => entry._1 + "=" + entry._2).mkString(",")

    val partitionName = if (partition.isEmpty) "" else s" in partition ($partitionDesc)"
    val description = s"$baseTablePath<br/>Optimizing ${bin.size} files" + partitionName
    sparkSession.sparkContext.setJobGroup(
      sparkSession.sparkContext.getLocalProperty(SPARK_JOB_GROUP_ID),
      description)

    val binInfo = optimizeStrategy.initNewBin
    val addFiles = txn.writeFiles(repartitionDF, None, isOptimize = true, Nil).collect {
      case a: AddFile => optimizeStrategy.tagAddFile(a, binInfo)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass}. File compaction job output" +
              s"should only have AddFiles")
    }
    val removeFiles = bin.map(f => f.removeWithTimestamp(operationTimestamp, dataChange = false))
    val updates = addFiles ++ removeFiles
    updates
  }

  /**
   * Attempts to commit the given actions to the log. In the case of a concurrent update,
   * the given function will be invoked with a new transaction to allow custom conflict
   * detection logic to indicate it is safe to try again, by returning `true`.
   *
   * This function will continue to try to commit to the log as long as `f` returns `true`,
   * otherwise throws a subclass of [[ConcurrentModificationException]].
   */
  private def commitAndRetry(
      txn: OptimisticTransaction,
      optimizeOperation: Operation,
      actions: Seq[Action],
      metrics: Map[String, SQLMetric])(f: OptimisticTransaction => Boolean): Unit = {
    try {
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(actions, optimizeOperation,
        RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot))
    } catch {
      case e: ConcurrentModificationException =>
        val newTxn = txn.deltaLog.startTransaction(txn.catalogTable)
        if (f(newTxn)) {
          logInfo("Retrying commit after checking for semantic conflicts with concurrent updates.")
          commitAndRetry(newTxn, optimizeOperation, actions, metrics)(f)
        } else {
          logWarning("Semantic conflicts detected. Aborting operation.")
          throw e
        }
    }
  }

  /** Create the appropriate [[Operation]] object for txn commit history */
  private def getOperation(): Operation = {
    if (optimizeContext.reorg.nonEmpty) {
      DeltaOperations.Reorg(partitionPredicate)
    } else {
      DeltaOperations.Optimize(
        predicate = partitionPredicate,
        zOrderBy = zOrderByColumns,
        auto = isAutoCompact,
        clusterBy = if (isClusteredTable) Option(clusteringColumns).filter(_.nonEmpty) else None)
    }
  }

  /** Create a map of SQL metrics for adding to the commit history. */
  private def createMetrics(
      sparkContext: SparkContext,
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile],
      removedDVs: Seq[DeletionVectorDescriptor]): Map[String, SQLMetric] = {

    def setAndReturnMetric(description: String, value: Long) = {
      val metric = createMetric(sparkContext, description)
      metric.set(value)
      metric
    }

    def totalSize(actions: Seq[FileAction]): Long = {
      var totalSize = 0L
      actions.foreach { file =>
        val fileSize = file match {
          case addFile: AddFile => addFile.size
          case removeFile: RemoveFile => removeFile.size.getOrElse(0L)
          case default =>
            throw new IllegalArgumentException(s"Unknown FileAction type: ${default.getClass}")
        }
        totalSize += fileSize
      }
      totalSize
    }

    val (deletionVectorRowsRemoved, deletionVectorBytesRemoved) =
      removedDVs.map(dv => (dv.cardinality, dv.sizeInBytes.toLong))
        .reduceLeftOption((dv1, dv2) => (dv1._1 + dv2._1, dv1._2 + dv2._2))
        .getOrElse((0L, 0L))

    val dvMetrics: Map[String, SQLMetric] = Map(
      "numDeletionVectorsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vectors removed",
          removedDVs.size),
      "numDeletionVectorRowsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vector rows removed",
          deletionVectorRowsRemoved),
      "numDeletionVectorBytesRemoved" ->
        setAndReturnMetric(
          "total number of bytes of removed deletion vectors",
          deletionVectorBytesRemoved))

    val sizeStats = FileSizeStatsWithHistogram.create(addedFiles.map(_.size).sorted)
    Map[String, SQLMetric](
      "minFileSize" -> setAndReturnMetric("minimum file size", sizeStats.get.min),
      "p25FileSize" -> setAndReturnMetric("25th percentile file size", sizeStats.get.p25),
      "p50FileSize" -> setAndReturnMetric("50th percentile file size", sizeStats.get.p50),
      "p75FileSize" -> setAndReturnMetric("75th percentile file size", sizeStats.get.p75),
      "maxFileSize" -> setAndReturnMetric("maximum file size", sizeStats.get.max),
      "numAddedFiles" -> setAndReturnMetric("total number of files added.", addedFiles.size),
      "numRemovedFiles" -> setAndReturnMetric("total number of files removed.", removedFiles.size),
      "numAddedBytes" -> setAndReturnMetric("total number of bytes added", totalSize(addedFiles)),
      "numRemovedBytes" ->
        setAndReturnMetric("total number of bytes removed", totalSize(removedFiles))
    ) ++ dvMetrics
  }
}
