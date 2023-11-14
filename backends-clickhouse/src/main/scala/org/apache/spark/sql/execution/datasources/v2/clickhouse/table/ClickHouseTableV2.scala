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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.table

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaColumnMapping, DeltaErrors, DeltaFileFormat, DeltaLog, DeltaTableIdentifier, DeltaTableUtils, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.{ClickHouseConfig, ClickHouseLog, DeltaLogAdapter}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.{ClickHouseScanBuilder, ClickHouseWriteBuilder, DeltaMergeTreeFileFormat}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path
import org.sparkproject.guava.cache.{CacheBuilder, CacheLoader}

import java.{util => ju}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

/**
 * The data source V2 representation of a ClickHouse table that exists.
 *
 * @param path
 *   The path to the table
 * @param tableIdentifier
 *   The table identifier for this table
 */
case class ClickHouseTableV2(
    spark: SparkSession,
    path: Path,
    catalogTable: Option[CatalogTable] = None,
    tableIdentifier: Option[String] = None,
    timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
    options: Map[String, String] = Map.empty,
    cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table
  with SupportsWrite
  with SupportsRead
  with V2TableWithV1Fallback
  with DeltaFileFormat
  with DeltaLogging {

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = ClickHouseLog.forTable(spark, rootPath, options)

  lazy val snapshot: Snapshot = {
    timeTravelSpec
      .map {
        spec =>
          val (version, accessType) =
            DeltaTableUtils.resolveTimeTravelVersion(spark.sessionState.conf, deltaLog, spec)
          val source = spec.creationSource.getOrElse("unknown")
          recordDeltaEvent(
            deltaLog,
            s"delta.timeTravel.$source",
            data = Map(
              "tableVersion" -> DeltaLogAdapter.snapshot(deltaLog).version,
              "queriedVersion" -> version,
              "accessType" -> accessType)
          )
          deltaLog.getSnapshotAt(version)
      }
      .getOrElse(updateSnapshot())
  }

  protected def metadata: Metadata = if (snapshot == null) Metadata() else snapshot.metadata

  private lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString, options)
    }
  }

  private lazy val timeTravelSpec: Option[DeltaTimeTravelSpec] = {
    if (timeTravelOpt.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    timeTravelOpt.orElse(timeTravelByPath)
  }

  private lazy val tableSchema: StructType =
    DeltaColumnMapping.dropColumnMappingMetadata(
      ColumnWithDefaultExprUtils.removeDefaultExpressions(snapshot.schema))

  def getTableIdentifierIfExists: Option[TableIdentifier] =
    tableIdentifier.map(spark.sessionState.sqlParser.parseTableIdentifier)

  override def name(): String =
    catalogTable
      .map(_.identifier.unquotedString)
      .orElse(tableIdentifier)
      .getOrElse(s"clickhouse.`${deltaLog.dataPath}`")

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    snapshot.metadata.partitionColumns.map {
      col => new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): ju.Map[String, String] = {
    val base = snapshot.getProperties
    base.put(TableCatalog.PROP_PROVIDER, ClickHouseConfig.NAME)
    base.put(TableCatalog.PROP_LOCATION, CatalogUtils.URIToString(path.toUri))
    Option(snapshot.metadata.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    // this reports whether the table is an external or managed catalog table as
    // the old DescribeTable command would
    catalogTable.foreach(table => base.put("Type", table.tableType.name))
    base.asJava
  }

  override def capabilities(): ju.Set[TableCapability] =
    Set(
      ACCEPT_ANY_SCHEMA, //
      BATCH_READ,
      BATCH_WRITE,
      V1_BATCH_WRITE,
      OVERWRITE_BY_FILTER,
      TRUNCATE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new ClickHouseWriteBuilder(spark, this, deltaLog, info)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ClickHouseScanBuilder(spark, this, tableSchema, options)
  }

  lazy val bucketOption: Option[BucketSpec] = {
    val tableProperties = properties()
    if (tableProperties.containsKey("numBuckets")) {
      val numBuckets = tableProperties.get("numBuckets").toInt
      val bucketColumnNames: Seq[String] =
        tableProperties.get("bucketColumnNames").split(",").toSeq
      val sortColumnNames: Seq[String] =
        tableProperties.get("sortColumnNames").split(",").toSeq
      Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames))
    } else {
      None
    }
  }

  /** Return V1Table. */
  override def v1Table: CatalogTable = {
    if (catalogTable.isEmpty) {
      throw new IllegalStateException("v1Table call is not expected with path based DeltaTableV2")
    }
    if (timeTravelSpec.isDefined) {
      catalogTable.get.copy(stats = None)
    } else {
      catalogTable.get
    }
  }

  /**
   * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
   * paths.
   */
  def toBaseRelation: BaseRelation = {
    snapshot
    if (!deltaLog.tableExists) {
      val id = catalogTable
        .map(ct => DeltaTableIdentifier(table = Some(ct.identifier)))
        .getOrElse(DeltaTableIdentifier(path = Some(path.toString)))
      throw DeltaErrors.notADeltaTableException(id)
    }
    val partitionPredicates =
      DeltaDataSource.verifyAndCreatePartitionFilters(path.toString, snapshot, partitionFilters)

    createV1Relation(partitionPredicates, Some(snapshot), timeTravelSpec.isDefined, cdcOptions)
  }

  /** Create ClickHouseFileIndex and HadoopFsRelation for DS V1. */
  def createV1Relation(
      partitionFilters: Seq[Expression] = Nil,
      snapshotToUseOpt: Option[Snapshot] = None,
      isTimeTravelQuery: Boolean = false,
      cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty): BaseRelation = {
    val snapshotToUse = snapshotToUseOpt.getOrElse(DeltaLogAdapter.snapshot(deltaLog))
    if (snapshotToUse.version < 0) {
      // A negative version here means the dataPath is an empty directory. Read query should error
      // out in this case.
      throw DeltaErrors.pathNotExistsException(deltaLog.dataPath.toString)
    }
    val fileIndex =
      ClickHouseFileIndex(spark, deltaLog, deltaLog.dataPath, this, snapshotToUse, partitionFilters)
    new HadoopFsRelation(
      fileIndex,
      partitionSchema =
        DeltaColumnMapping.dropColumnMappingMetadata(snapshotToUse.metadata.partitionSchema),
      // We pass all table columns as `dataSchema` so that Spark will preserve the partition column
      // locations. Otherwise, for any partition columns not in `dataSchema`, Spark would just
      // append them to the end of `dataSchema`
      dataSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        ColumnWithDefaultExprUtils.removeDefaultExpressions(
          SchemaUtils.dropNullTypeColumns(snapshotToUse.metadata.schema))),
      bucketSpec = bucketOption,
      fileFormat(snapshotToUse.metadata),
      // `metadata.format.options` is not set today. Even if we support it in future, we shouldn't
      // store any file system options since they may contain credentials. Hence, it will never
      // conflict with `DeltaLog.options`.
      snapshotToUse.metadata.format.options ++ options
    )(
      spark
    ) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        // Insert MergeTree data through DataSource V1
      }
    }
  }

  /** Check the passed in options and existing timeTravelOpt, set new time travel by options. */
  def withOptions(options: Map[String, String]): ClickHouseTableV2 = {
    val ttSpec = DeltaDataSource.getTimeTravelVersion(options)
    if (timeTravelOpt.nonEmpty && ttSpec.nonEmpty) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    if (timeTravelOpt.isEmpty && ttSpec.nonEmpty) {
      copy(timeTravelOpt = ttSpec)
    } else {
      this
    }
  }

  /** Refresh table to load latest snapshot */
  def refresh(): Unit = {
    updateSnapshot(true)
  }

  def updateSnapshot(forceUpdate: Boolean = false): Snapshot = {
    val needToUpdate = forceUpdate || ClickHouseTableV2.isSnapshotStale
    if (needToUpdate) {
      val snapshotUpdated = deltaLog.update()
      ClickHouseTableV2.fileStatusCache.invalidate(this.rootPath)
      ClickHouseTableV2.lastUpdateTimestamp = System.currentTimeMillis()
      snapshotUpdated
    } else {
      DeltaLogAdapter.snapshot(deltaLog)
    }
  }

  def listFiles(
      partitionFilters: Seq[Expression] = Seq.empty[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[AddMergeTreeParts] = {
    // TODO: Refresh cache after writing data.
    val allParts = ClickHouseTableV2.fileStatusCache.get(this.rootPath)
    allParts
  }

  override def fileFormat(metadata: Metadata): FileFormat = {
    new DeltaMergeTreeFileFormat(metadata)
  }
}

object ClickHouseTableV2 extends Logging {
  val fileStatusCacheLoader: CacheLoader[Path, Seq[AddMergeTreeParts]] =
    new CacheLoader[Path, Seq[AddMergeTreeParts]]() {
      @throws[Exception]
      override def load(tablePath: Path): Seq[AddMergeTreeParts] = {
        getTableParts(tablePath)
      }
    }
  private val fileStatusCache = CacheBuilder.newBuilder
    .maximumSize(1000)
    .expireAfterAccess(3600L, TimeUnit.SECONDS)
    .recordStats
    .build[Path, Seq[AddMergeTreeParts]](fileStatusCacheLoader)
  protected val stalenessLimit: Long = SparkSession.active.sessionState.conf
    .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
  protected var lastUpdateTimestamp: Long = -1L

  def isSnapshotStale: Boolean = {
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
    System.currentTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

  def getTableParts(tablePath: Path): Seq[AddMergeTreeParts] = {
    implicit val enc: Encoder[AddFile] = SingleAction.addFileEncoder
    val start = System.currentTimeMillis()
    val snapshot = DeltaLogAdapter.snapshot(ClickHouseLog.forTable(SparkSession.active, tablePath))
    val allParts = DeltaLog
      .filterFileList(snapshot.metadata.partitionSchema, snapshot.allFiles.toDF(), Seq.empty)
      .as[AddFile]
      .collect()
      .map(AddFileTags.partsMapToParts)
      .sortWith(
        (a, b) => {
          if (a.bucketNum.nonEmpty) {
            (Integer.parseInt(a.bucketNum) < Integer.parseInt(b.bucketNum)) ||
            (a.minBlockNumber < b.minBlockNumber)
          } else {
            a.minBlockNumber < b.minBlockNumber
          }
        })
      .toSeq
    logInfo(
      s"Get ${allParts.size} parts from path ${tablePath.toString} " +
        (System.currentTimeMillis() - start))
    allParts
  }
}
