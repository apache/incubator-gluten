/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.clickhouse.table

import java.util.concurrent.TimeUnit

import java.{util => ju}
import org.sparkproject.guava.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.v2.clickhouse.{ClickHouseConfig, ClickHouseLog}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.ClickHouseScanBuilder

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTableUtils, DeltaTimeTravelSpec, GeneratedColumn, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The data source V2 representation of a ClickHouse table that exists.
 *
 * @param path The path to the table
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
  with DeltaLogging {

  private lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString)
    }
  }

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = ClickHouseLog.forTable(spark, rootPath)

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

  def getTableIdentifierIfExists: Option[TableIdentifier] = tableIdentifier.map(
    spark.sessionState.sqlParser.parseTableIdentifier)

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .orElse(tableIdentifier)
    .getOrElse(s"clickhouse.`${deltaLog.dataPath}`")

  private lazy val timeTravelSpec: Option[DeltaTimeTravelSpec] = {
    if (timeTravelOpt.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    timeTravelOpt.orElse(timeTravelByPath)
  }

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

  private lazy val tableSchema: StructType =
    GeneratedColumn.removeGenerationExpressions(snapshot.schema)

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
    BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    throw new UnsupportedOperationException("Do not support write data now.")
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ClickHouseScanBuilder(spark, this, tableSchema, options)
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

    deltaLog.createRelation(
      partitionPredicates, Some(snapshot), timeTravelSpec.isDefined, options)
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

  def listFiles(partitionFilters: Seq[Expression] = Seq.empty[Expression],
                partitionColumnPrefixes: Seq[String] = Nil): Seq[AddMergeTreeParts] = {
    val allParts = ClickHouseTableV2.fileStatusCache.get(this.rootPath)
    allParts
  }
}

object ClickHouseTableV2 {
  protected var lastUpdateTimestamp: Long = -1L
  protected val stalenessLimit = SparkSession.active.sessionState.conf.getConf(
    DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)

  def isSnapshotStale: Boolean = {
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      System.currentTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

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

  def getTableParts(tablePath: Path): Seq[AddMergeTreeParts] = {
    implicit val enc = SingleAction.addFileEncoder
    val start = System.currentTimeMillis()
    val snapshot = ClickHouseLog.forTable(SparkSession.active, tablePath).snapshot
    val allParts = DeltaLog.filterFileList(
      snapshot.metadata.partitionSchema,
      snapshot.allFiles.toDF(),
      Seq.empty).as[AddFile].collect().map(AddFileTags.partsMapToParts(_))
      .sortWith(_.minBlockNumber < _.minBlockNumber).toSeq
    println(s"Get all parts from path ${tablePath.toString} " +
      (System.currentTimeMillis() - start))
    allParts
  }
}