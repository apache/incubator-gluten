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
package org.apache.gluten.execution

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.{PaimonLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, DynamicPruningExpression, EqualTo, Expression, GreaterThan, LessThan, Like, Literal, Not, Or}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.{ChangelogProducer, MergeEngine}
import org.apache.paimon.spark.{PaimonBaseScan, PaimonInputPartition, PaimonScan}
import org.apache.paimon.spark.schema.PaimonMetadataColumn.SUPPORTED_METADATA_COLUMNS
import org.apache.paimon.spark.source.PaimonConfig
import org.apache.paimon.table.{DataTable, FileStoreTable}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.DecimalType

import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable

abstract class AbstractPaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  )
  with BasicScanExecTransformer
  with Logging {

  @transient private lazy val shim: PaimonSparkShim = new PaimonSparkShimImpl()

  protected lazy val tableProperties: HashMap[String, String] = {
    // If this is a Paimon scan, append key-value pairs
    val map = HashMap.newBuilder[String, String]
    scan match {
      case paimonScan: org.apache.paimon.spark.PaimonScan =>
        val t = paimonScan.table
        val coreOptions = new CoreOptions(t.options())
        coreOptions.toMap.forEach((key, value) => map += (key -> value))

        // if enabled, the scan will not be counted as paimon scan
        if (SQLConf.get.getConf(PaimonConfig.PAIMON_NATIVE_SPLIT_ENABLED)) {
          map += ("isPaimon" -> "true")
        }

        val primaryKeys = t.primaryKeys()
        if (!primaryKeys.isEmpty) {
          map += ("primary-key" -> String.join(",", primaryKeys))
        }

      case _ => // Not a paimon scan
    }
    map.result()
  }

  private lazy val coreOptions: CoreOptions = scan match {
    case scan: PaimonScan =>
      scan.table match {
        case dataTable: DataTable =>
          dataTable.coreOptions()
        case _ =>
          throw new GlutenNotSupportException("Only support Paimon DataTable.")
      }
    case _ =>
      throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  override def getPartitionSchema: StructType = scan match {
    case paimonScan: PaimonScan =>
      val partitionKeys = paimonScan.table.partitionKeys()
      StructType(scan.readSchema().filter(field => partitionKeys.contains(field.name)))
    case _ =>
      throw new GlutenNotSupportException("Only support PaimonScan.")
  }

  /** Returns the actual schema of this data source scan. */
  override def getDataSchema: StructType = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
    scan.asInstanceOf[PaimonBaseScan].readSchema())

  override lazy val fileFormat: ReadFileFormat = {
    val formatStr = coreOptions.fileFormatString()
    if ("parquet".equalsIgnoreCase(formatStr)) {
      ReadFileFormat.ParquetReadFormat
    } else if ("orc".equalsIgnoreCase(formatStr)) {
      ReadFileFormat.OrcReadFormat
    } else {
      ReadFileFormat.UnknownFormat
    }
  }

  override def doValidateInternal(): ValidationResult = {
    val result = AbstractPaimonScanTransformer.supportsBatchScan(scan)
    if (result.ok()) {
      return super.doValidateInternal()
    }
    result
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException()

  override def getSplitInfosWithIndex: Seq[SplitInfo] = getSplitInfosFromPartitions(getPartitions)

  override def getSplitInfosFromPartitions(partitions: Seq[InputPartition]): Seq[SplitInfo] = {
    // if useRawSplit is true, the scan will execute like a normal hive scan
    var useHiveSplit = true
    if (SQLConf.get.getConf(PaimonConfig.PAIMON_NATIVE_SPLIT_ENABLED)) {
      useHiveSplit = false
    }
    val paimonScan = scan.asInstanceOf[PaimonScan]
    val primaryKeys = paimonScan.table.primaryKeys()

    val partitionComputer = scan match {
      case paimonScan: PaimonScan => shim.getInternalPartitionComputer(paimonScan)
      case _ => throw new GlutenNotSupportException("Only support PaimonScan.")
    }
    getPartitions.zipWithIndex.map {
      case (p, index) =>
        p match {
          case partition: PaimonInputPartition =>
            val paths = mutable.ListBuffer.empty[String]
            val starts = mutable.ListBuffer.empty[JLong]
            val lengths = mutable.ListBuffer.empty[JLong]
            val partitionColumns = mutable.ListBuffer.empty[JMap[String, String]]
            val buckets = mutable.ListBuffer.empty[JInteger]
            val firstRowIds = mutable.ListBuffer.empty[JLong]
            val maxSequenceNumbers = mutable.ListBuffer.empty[JLong]
            val splitGroups = mutable.ListBuffer.empty[JInteger]
            val allRawConvertible = partition.splits.forall(_.convertToRawFiles().isPresent)
            partition.splits.zipWithIndex.map {
              case (split: DataSplit, splitIdx) =>
                if (!split.beforeFiles().isEmpty) {
                  throw new UnsupportedOperationException("Do not support before files")
                }
                val partitionRow =
                  partitionComputer.generatePartValues(shim.getSplitPartition(split))
                val fileMetas = split.dataFiles().asScala
                // zipping should associate the correct file metadata as rawFiles uses dataFiles() as the stream
                // source when computing the RawFiles list.
                val bucket = split.bucket()

                paths ++= fileMetas.map(
                  file => {
                    val bucketPath = shim.getBucketPath(split, file)
                    bucketPath + "/" + file.fileName()
                  })
                starts ++= mutable.ArrayBuffer.fill(fileMetas.size)(0)
                lengths ++= fileMetas.map(file => JLong.valueOf(file.fileSize()))
                partitionColumns ++= mutable.ArrayBuffer.fill(fileMetas.size)(partitionRow)
                buckets ++= fileMetas.map(_ => JInteger.valueOf(bucket))

                firstRowIds ++= fileMetas
                  .map(_.firstRowId())
                  .map(id => JLong.valueOf(if (id == null) 0L else id.toLong))
                maxSequenceNumbers ++= fileMetas.map(
                  file => JLong.valueOf(file.maxSequenceNumber()))
                splitGroups ++= fileMetas.map(_ => JInteger.valueOf(splitIdx))
              case (split, _) =>
                throw new UnsupportedOperationException(
                  f"paimon split type: '${split.getClass.getName}' is not supported")
            }
            val preferredLoc =
              SoftAffinity.getFilePartitionLocations(paths.toArray, partition.preferredLocations())
            PaimonLocalFilesBuilder.makePaimonLocalFiles(
              index,
              paths.asJava,
              starts.asJava,
              lengths.asJava,
              partitionColumns.asJava,
              fileFormat,
              preferredLoc.toList.asJava,
              new JHashMap[String, String](),
              buckets.asJava,
              firstRowIds.asJava,
              maxSequenceNumbers.asJava,
              splitGroups.asJava,
              useHiveSplit,
              primaryKeys,
              allRawConvertible
            )
          case _ =>
            throw new GlutenNotSupportException("Only support paimon SparkInputPartition.")
        }
    }
  }

  override protected[this] def supportsBatchScan(scan: Scan): Boolean =
    AbstractPaimonScanTransformer.supportsBatchScan(scan).ok()

  /** Returns the filters that can be pushed down to native file scan */
  override def filterExprs(): Seq[Expression] = {
    scan match {
      case paimonScan: PaimonScan =>
        val primaryKeys = paimonScan.table.primaryKeys()

        def isAllowedExpr(expr: Expression): Boolean = expr match {
          // Equality comparisons with pk and literal
          case EqualTo(attr: Attribute, lit: Literal) => primaryKeys.contains(attr.name)
          case EqualTo(lit: Literal, attr: Attribute) => primaryKeys.contains(attr.name)
          case GreaterThan(attr: Attribute, lit: Literal) => primaryKeys.contains(attr.name)
          case GreaterThan(lit: Literal, attr: Attribute) => primaryKeys.contains(attr.name)
          case LessThan(attr: Attribute, lit: Literal) => primaryKeys.contains(attr.name)
          case LessThan(lit: Literal, attr: Attribute) => primaryKeys.contains(attr.name)

          // Like operator with pk and literal
          case Like(attr: Attribute, lit: Literal, _) => primaryKeys.contains(attr.name)
          case Like(lit: Literal, attr: Attribute, _) => primaryKeys.contains(attr.name)

          // Logical combinators
          case And(left, right) => isAllowedExpr(left) && isAllowedExpr(right)
          case Or(left, right) => isAllowedExpr(left) && isAllowedExpr(right)
          case Not(child) => isAllowedExpr(child)

          // Allow alias wrappers
          case Alias(child, _) => isAllowedExpr(child)

          // Otherwise reject
          case _ => false
        }

        pushdownFilters match {
          case filters if !primaryKeys.isEmpty =>
            val result = filters.filter(isAllowedExpr) // keep only allowed ones
            result
          case filters => filters
        }

      case _ => pushdownFilters
    }
  }
}

object AbstractPaimonScanTransformer {
  def apply(batchScan: BatchScanExec): PaimonScanTransformer = {
    PaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan)
    )
  }

  def supportsBatchScan(scan: Scan): ValidationResult = {
    if (!SQLConf.get.getConf(PaimonConfig.PAIMON_NATIVE_SOURCE_ENABLED)) {
      return ValidationResult.failed("[Paimon Fallback]: The paimon native source is not enabled.")
    }
    scan match {
      case paimonScan: PaimonScan =>
        val table = paimonScan.table

        val coreOptions = new CoreOptions(table.options())
        val partitionKeys = table.partitionKeys()
        val partitionType = table.rowType().project(partitionKeys)

        // skip system table
        if (!table.isInstanceOf[FileStoreTable]) {
          return ValidationResult.failed(
            f"[Paimon Fallback]: The table is not fileStoreTable: ${table.getClass}")
        }
        // skip decimal type as partition type
        if (partitionType.getFieldTypes.toArray.exists(d => d.isInstanceOf[DecimalType])) {
          return ValidationResult.failed(
            "[Paimon Fallback]: Not support decimal type as partition column")
        }

        // skip if query contains metadata columns and is not parquet format
        val isAllParquet = table
          .asInstanceOf[FileStoreTable]
          .coreOptions()
          .fileFormatPerLevel()
          .values()
          .stream()
          .allMatch(fmt => fmt.equalsIgnoreCase(CoreOptions.FILE_FORMAT_PARQUET)) ||
          paimonScan.coreOptions
            .fileFormatString()
            .equalsIgnoreCase(CoreOptions.FILE_FORMAT_PARQUET)
        val schemaCols = scan.readSchema().fields.map(_.name).toSet
        val schemaHasMetadataCols =
          SUPPORTED_METADATA_COLUMNS.exists(col => schemaCols.contains(col))
        if (schemaHasMetadataCols && !isAllParquet) {
          return ValidationResult.failed(
            "[Paimon Fallback]: Metadata column queries are only supported with parquet files.")
        }

        // todo support deletion vector
        if (coreOptions.deletionVectorsEnabled()) {
          return ValidationResult.failed(
            "[Paimon Fallback]: The scan with deletion vector is not supported")
        }

        val splits = paimonScan.getOriginSplits
        if (!table.primaryKeys().isEmpty && splits.forall(_.convertToRawFiles().isPresent)) {
          // all the splits are raw convertable
          return ValidationResult.succeeded
        }

        // MOR
        if (coreOptions.changelogProducer() == ChangelogProducer.LOOKUP) {
          ValidationResult.failed("[Paimon Fallback] LOOKUP ChangelogProducer is not supported")
        } else if (!table.primaryKeys().isEmpty) {
          if (SQLConf.get.getConf(PaimonConfig.PAIMON_NATIVE_MOR_ENABLED)) {
            // partial update and aggregate engines are supported with flags, deduplicate engine is supported without a flag
            val supported = coreOptions.mergeEngine() == MergeEngine.DEDUPLICATE ||
              (coreOptions.mergeEngine() == MergeEngine.AGGREGATE && SQLConf.get.getConf(
                PaimonConfig.PAIMON_NATIVE_MOR_AGGREGATE_ENGINE_ENABLED)) ||
              (coreOptions.mergeEngine() == MergeEngine.PARTIAL_UPDATE && SQLConf.get.getConf(
                PaimonConfig.PAIMON_NATIVE_MOR_PARTIAL_UPDATE_ENGINE_ENABLED))
            if (supported) {
              return ValidationResult.succeeded
            }
            ValidationResult.failed(s"""
                            [Paimon Fallback] the configured Paimon merge engine ${coreOptions.mergeEngine()}
                             is either not supported or does not have its corresponding configuration flag enabled.""".stripMargin)
          } else {
            ValidationResult.failed("[Paimon Fallback]: The paimon mor source is not enabled.")
          }
        } else {
          ValidationResult.succeeded
        }
      case s =>
        ValidationResult.failed(f"[Paimon Fallback] Scan is not a PaimonScan. Got ${s.getClass}")
    }
  }
}

case class PaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends AbstractPaimonScanTransformer(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  ) {

  override def doCanonicalize(): AbstractPaimonScanTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output)
    )
  }
}
