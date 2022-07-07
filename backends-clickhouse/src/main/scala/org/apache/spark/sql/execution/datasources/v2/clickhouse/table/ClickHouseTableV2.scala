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

import java.{util => ju}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.sparkproject.guava.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTableUtils, DeltaTimeTravelSpec, GeneratedColumn, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.{ClickHouseConfig, ClickHouseLog}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.{ClickHouseScanBuilder, ClickHouseWriteBuilder}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
  * The data source V2 representation of a ClickHouse table that exists.
  *
  * @param path            The path to the table
  * @param tableIdentifier The table identifier for this table
  */
case class ClickHouseTableV2(
                              spark: SparkSession,
                              path: Path,
                              catalogTable: Option[CatalogTable] = None,
                              tableIdentifier: Option[String] = None,
                              timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
                              options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table
    with SupportsWrite
    with SupportsRead
    with V2TableWithV1Fallback
    with DeltaLogging {

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = ClickHouseLog.forTable(spark, rootPath)
  lazy val snapshot: Snapshot = {
    timeTravelSpec.map { spec =>
      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, deltaLog, spec)
      val source = spec.creationSource.getOrElse("unknown")
      recordDeltaEvent(deltaLog, s"delta.timeTravel.$source", data = Map(
        "tableVersion" -> deltaLog.snapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      deltaLog.getSnapshotAt(version)
    }.getOrElse(updateSnapshot())
  }
  private lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString)
    }
  }
  private lazy val timeTravelSpec: Option[DeltaTimeTravelSpec] = {
    if (timeTravelOpt.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    timeTravelOpt.orElse(timeTravelByPath)
  }
  private lazy val tableSchema: StructType =
    GeneratedColumn.removeGenerationExpressions(snapshot.schema)

  def getTableIdentifierIfExists: Option[TableIdentifier] = tableIdentifier.map(
    spark.sessionState.sqlParser.parseTableIdentifier)

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .orElse(tableIdentifier)
    .getOrElse(s"clickhouse.`${deltaLog.dataPath}`")

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    snapshot.metadata.partitionColumns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
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

  override def capabilities(): ju.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    BATCH_WRITE, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new ClickHouseWriteBuilder(spark, this, deltaLog, info)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ClickHouseScanBuilder(spark, this, tableSchema, options)
  }

  /**
    * Return V1Table.
    */
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
    if (!deltaLog.tableExists) {
      val id = catalogTable.map(ct => DeltaTableIdentifier(table = Some(ct.identifier)))
        .getOrElse(DeltaTableIdentifier(path = Some(path.toString)))
      throw DeltaErrors.notADeltaTableException(id)
    }
    val partitionPredicates = DeltaDataSource.verifyAndCreatePartitionFilters(
      path.toString, snapshot, partitionFilters)

    createV1Relation(
      partitionPredicates, Some(snapshot), timeTravelSpec.isDefined, options)
  }

  /**
    * Create ClickHouseFileIndex and HadoopFsRelation for DS V1.
    */
  def createV1Relation(
                        partitionFilters: Seq[Expression] = Nil,
                        snapshotToUseOpt: Option[Snapshot] = None,
                        isTimeTravelQuery: Boolean = false,
                        cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty
                      ): BaseRelation = {
    val snapshotToUse = snapshotToUseOpt.getOrElse(deltaLog.snapshot)
    if (snapshotToUse.version < 0) {
      // A negative version here means the dataPath is an empty directory. Read query should error
      // out in this case.
      throw DeltaErrors.pathNotExistsException(deltaLog.dataPath.toString)
    }
    val fileIndex = ClickHouseFileIndex(
      spark, deltaLog, deltaLog.dataPath, this, snapshotToUse, partitionFilters)
    var bucketSpec: Option[BucketSpec] = None
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = GeneratedColumn.removeGenerationExpressions(
        SchemaUtils.dropNullTypeColumns(snapshotToUse.metadata.schema)
      ),
      bucketSpec = bucketSpec,
      snapshotToUse.fileFormat,
      snapshotToUse.metadata.format.options)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        // Insert MergeTree data through DataSource V1
      }
    }
  }

  /**
    * Check the passed in options and existing timeTravelOpt, set new time travel by options.
    */
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

  /**
    * Refresh table to load latest snapshot
    */
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
      deltaLog.snapshot
    }
  }

  def listFiles(partitionFilters: Seq[Expression] = Seq.empty[Expression],
                partitionColumnPrefixes: Seq[String] = Nil): Seq[AddMergeTreeParts] = {
    // TODO: Refresh cache after writing data.
    val allParts = ClickHouseTableV2.fileStatusCache.get(this.rootPath)
    allParts
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
  val fileStatusCache = CacheBuilder.newBuilder
    .maximumSize(1000)
    .expireAfterAccess(3600L, TimeUnit.SECONDS)
    .recordStats.build[Path, Seq[AddMergeTreeParts]](fileStatusCacheLoader)
  protected val stalenessLimit = SparkSession.active.sessionState.conf.getConf(
    DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
  protected var lastUpdateTimestamp: Long = -1L

  def isSnapshotStale: Boolean = {
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      System.currentTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

  def getTableParts(tablePath: Path): Seq[AddMergeTreeParts] = {
    implicit val enc = SingleAction.addFileEncoder
    val start = System.currentTimeMillis()
    val snapshot = ClickHouseLog.forTable(SparkSession.active, tablePath).snapshot
    val allParts = DeltaLog.filterFileList(
      snapshot.metadata.partitionSchema,
      snapshot.allFiles.toDF(),
      Seq.empty).as[AddFile].collect().map(AddFileTags.partsMapToParts(_))
      .sortWith(_.minBlockNumber < _.minBlockNumber).toSeq
    logInfo(s"Get ${allParts.size} parts from path ${tablePath.toString} " +
      (System.currentTimeMillis() - start))
    allParts
  }
}
