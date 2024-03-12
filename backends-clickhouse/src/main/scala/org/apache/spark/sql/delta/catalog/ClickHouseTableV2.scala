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
package org.apache.spark.sql.delta.catalog
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaColumnMapping, DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2.deltaLog2Table
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.utils.MergeTreePartsPartitionsUtil
import org.apache.spark.sql.execution.datasources.v2.clickhouse.{ClickHouseConfig, DeltaLogAdapter}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import java.{util => ju}

class ClickHouseTableV2(
    override val spark: SparkSession,
    override val path: Path,
    override val catalogTable: Option[CatalogTable] = None,
    override val tableIdentifier: Option[String] = None,
    override val timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
    override val options: Map[String, String] = Map.empty,
    override val cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends DeltaTableV2(
    spark,
    path,
    catalogTable,
    tableIdentifier,
    timeTravelOpt,
    options,
    cdcOptions) {
  protected def getMetadata: Metadata = if (snapshot == null) Metadata() else snapshot.metadata

  lazy val (rootPath, partitionFilters, timeTravelByPath) = {
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

  override def name(): String =
    catalogTable
      .map(_.identifier.unquotedString)
      .orElse(tableIdentifier)
      .getOrElse(s"clickhouse.`${deltaLog.dataPath}`")

  override def properties(): ju.Map[String, String] = {
    val ret = super.properties()
    ret.put(TableCatalog.PROP_PROVIDER, ClickHouseConfig.NAME)
    ret
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoDeltaBuilder(deltaLog, info.options)
  }

  lazy val dataBaseName = catalogTable
    .map(_.identifier.database.getOrElse("default"))
    .getOrElse("default")

  lazy val tableName = catalogTable
    .map(_.identifier.table)
    .getOrElse("")

  lazy val bucketOption: Option[BucketSpec] = {
    val tableProperties = properties()
    if (tableProperties.containsKey("numBuckets")) {
      val numBuckets = tableProperties.get("numBuckets").trim.toInt
      val bucketColumnNames: Seq[String] =
        tableProperties.get("bucketColumnNames").split(",").map(_.trim).toSeq
      val sortColumnNames: Seq[String] = if (tableProperties.containsKey("sortColumnNames")) {
        tableProperties.get("sortColumnNames").split(",").map(_.trim).toSeq
      } else Seq.empty[String]
      Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames))
    } else {
      None
    }
  }

  lazy val lowCardKeyOption: Option[Seq[String]] = {
    val tableProperties = properties()
    if (tableProperties.containsKey("lowCardKey")) {
      if (tableProperties.get("lowCardKey").nonEmpty) {
        val lowCardKeys = tableProperties.get("lowCardKey").split(",").map(_.trim).toSeq
        lowCardKeys.foreach(
          s => {
            if (s.contains(".")) {
              throw new IllegalStateException(
                s"lowCardKey $s can not contain '.' (not support nested column yet)")
            }
          })
        Some(lowCardKeys.map(s => s.toLowerCase()))
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val orderByKeyOption: Option[Seq[String]] = {
    if (bucketOption.isDefined && bucketOption.get.sortColumnNames.nonEmpty) {
      val orderByKes = bucketOption.get.sortColumnNames
      val invalidKeys = orderByKes.intersect(partitionColumns)
      if (invalidKeys.nonEmpty) {
        throw new IllegalStateException(
          s"partition cols $invalidKeys can not be in the order by keys.")
      }
      Some(orderByKes)
    } else {
      val tableProperties = properties()
      if (tableProperties.containsKey("orderByKey")) {
        if (tableProperties.get("orderByKey").nonEmpty) {
          val orderByKes = tableProperties.get("orderByKey").split(",").map(_.trim).toSeq
          val invalidKeys = orderByKes.intersect(partitionColumns)
          if (invalidKeys.nonEmpty) {
            throw new IllegalStateException(
              s"partition cols $invalidKeys can not be in the order by keys.")
          }
          Some(orderByKes)
        } else {
          None
        }
      } else {
        None
      }
    }
  }

  lazy val primaryKeyOption: Option[Seq[String]] = {
    if (orderByKeyOption.isDefined) {
      val tableProperties = properties()
      if (tableProperties.containsKey("primaryKey")) {
        if (tableProperties.get("primaryKey").nonEmpty) {
          val primaryKeys = tableProperties.get("primaryKey").split(",").map(_.trim).toSeq
          if (!orderByKeyOption.get.mkString(",").startsWith(primaryKeys.mkString(","))) {
            throw new IllegalStateException(
              s"Primary key $primaryKeys must be a prefix of the sorting key")
          }
          Some(primaryKeys)
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val partitionColumns = snapshot.metadata.partitionColumns

  lazy val clickhouseTableConfigs: Map[String, String] = {
    val tableProperties = properties()
    val configs = scala.collection.mutable.Map[String, String]()
    configs += ("storage_policy" -> tableProperties.getOrDefault("storage_policy", "default"))
    configs.toMap
  }

  /**
   * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
   * paths.
   */
  override def toBaseRelation: BaseRelation = {
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
      new TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, snapshotToUse, partitionFilters)
    val fileFormat: DeltaMergeTreeFileFormat = getFileFormat(getMetadata)
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
      fileFormat,
      // `metadata.format.options` is not set today. Even if we support it in future, we shouldn't
      // store any file system options since they may contain credentials. Hence, it will never
      // conflict with `DeltaLog.options`.
      snapshotToUse.metadata.format.options ++ options
    )(
      spark
    ) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        throw new UnsupportedOperationException()
//        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        // Insert MergeTree data through DataSource V1
      }
    }
  }

  def getFileFormat(meta: Metadata): DeltaMergeTreeFileFormat = {
    new DeltaMergeTreeFileFormat(
      meta,
      dataBaseName,
      tableName,
      Seq.empty[Attribute],
      orderByKeyOption,
      lowCardKeyOption,
      primaryKeyOption,
      clickhouseTableConfigs,
      partitionColumns)
  }
  def cacheThis(): Unit = {
    deltaLog2Table.put(deltaLog, this)
  }

  cacheThis()
}

class TempClickHouseTableV2(
    override val spark: SparkSession,
    override val catalogTable: Option[CatalogTable] = None)
  extends ClickHouseTableV2(spark, null, catalogTable) {
  import collection.JavaConverters._
  override def properties(): ju.Map[String, String] = catalogTable.get.properties.asJava
  override lazy val partitionColumns: Seq[String] = catalogTable.get.partitionColumnNames
  override def cacheThis(): Unit = {}
}

object ClickHouseTableV2 extends Logging {
  private val deltaLog2Table =
    new scala.collection.concurrent.TrieMap[DeltaLog, ClickHouseTableV2]()
  // for CTAS use
  val temporalThreadLocalCHTable = new ThreadLocal[ClickHouseTableV2]()

  def getTable(deltaLog: DeltaLog): ClickHouseTableV2 = {
    if (deltaLog2Table.contains(deltaLog)) {
      deltaLog2Table(deltaLog)
    } else if (temporalThreadLocalCHTable.get() != null) {
      temporalThreadLocalCHTable.get()
    } else {
      throw new IllegalStateException("Can not find ClickHouseTableV2")
    }
  }

  def clearCache(): Unit = {
    deltaLog2Table.clear()
    temporalThreadLocalCHTable.remove()
  }

  def partsPartitions(
      deltaLog: DeltaLog,
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    val tableV2 = ClickHouseTableV2.getTable(deltaLog)

    MergeTreePartsPartitionsUtil.getMergeTreePartsPartitions(
      relation,
      selectedPartitions,
      output,
      bucketedScan,
      tableV2.spark,
      tableV2,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)

  }
}
