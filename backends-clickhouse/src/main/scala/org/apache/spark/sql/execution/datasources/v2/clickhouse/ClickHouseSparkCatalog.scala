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

package org.apache.spark.sql.execution.datasources.v2.clickhouse

import java.util

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.delta.DeltaTableIdentifier.gluePermissionError
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.execution.datasources.v2.clickhouse.commands.CreateClickHouseTableCommand
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.execution.datasources.v2.clickhouse.utils.{CHDataSourceUtils, ScanMergeTreePartsUtils}
import org.apache.spark.sql.types.StructType

class ClickHouseSparkCatalog extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SupportsPathIdentifier
  with Logging {

  val spark = SparkSession.active

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    if (CHDataSourceUtils.isClickHouseDataSourceName(getProvider(properties))) {
      createClickHouseTable(
        ident,
        schema,
        partitions,
        properties,
        Map.empty,
        sourceQuery = None,
        TableCreationModes.Create)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }
  }

  /**
    * Creates a ClickHouse table
    *
    * @param ident              The identifier of the table
    * @param schema             The schema of the table
    * @param partitions         The partition transforms for the table
    * @param allTableProperties The table properties that configure the behavior of the table or
    *                           provide information about the table
    * @param writeOptions       Options specific to the write during table creation or replacement
    * @param sourceQuery        A query if this CREATE request came from a CTAS or RTAS
    * @param operation          The specific table creation mode, whether this is a
    *                           Create/Replace/Create or
    *                           Replace
    */
  private def createClickHouseTable(
                                     ident: Identifier,
                                     schema: StructType,
                                     partitions: Array[Transform],
                                     allTableProperties: util.Map[String, String],
                                     writeOptions: Map[String, String],
                                     sourceQuery: Option[DataFrame],
                                     operation: TableCreationModes.CreationMode): Table = {
    val tableProperties = ClickHouseConfig.validateConfigurations(allTableProperties)
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    var newSchema = schema
    var newPartitionColumns = partitionColumns
    var newBucketSpec = maybeBucketSpec

    val isByPath = isPathIdentifier(ident)
    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    val locUriOpt = location.map(CatalogUtils.stringToURI)
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = locUriOpt)
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val id = TableIdentifier(ident.name(), ident.namespace().lastOption)
    val loc = new Path(locUriOpt.getOrElse(spark.sessionState.catalog.defaultTablePath(id)))
    val commentOpt = Option(allTableProperties.get("comment"))

    val tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(ClickHouseConfig.ALT_NAME),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tableProperties,
      comment = commentOpt)

    val withDb = verifyTableAndSolidify(tableDesc, None)
    ParquetSchemaConverter.checkFieldNames(tableDesc.schema)

    // TODO: Generate WriteClickHouseTableCommand
    // val writer = sourceQuery.map { df =>
    // }

    CreateClickHouseTableCommand(
      withDb,
      getExistingTableIfExists(tableDesc),
      operation.mode,
      operation = operation,
      tableByPath = isByPath).run(spark)

    logInfo(s"create table ${ident.toString} successfully.")
    val loadedNewTable = loadTable(ident)
    logInfo(s"scanning table ${ident.toString} data ...")
    loadedNewTable match {
      case v: ClickHouseTableV2 =>
        // TODO: remove this operation after implementing write mergetree into table
        ScanMergeTreePartsUtils.scanMergeTreePartsToAddFile(spark.sessionState.newHadoopConf(), v)
        v.refresh()
      case _ =>
    }
    loadedNewTable
  }

  // Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col


      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw new UnsupportedOperationException(s"Partitioning by expressions")
    }

    (identityCols, bucketSpec)
  }

  /** Performs checks on the parameters provided for table creation for a ClickHouse table. */
  private def verifyTableAndSolidify(
                                      tableDesc: CatalogTable,
                                      query: Option[LogicalPlan]): CatalogTable = {

    if (tableDesc.bucketSpec.isDefined) {
      throw new UnsupportedOperationException("Do not support Bucketing")
    }

    val schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // Delta is case insensitive

    val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = tableDesc.properties)
  }

  /** Checks if a table already exists for the provided identifier. */
  private def getExistingTableIfExists(table: CatalogTable): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathIdentifier(table)) return None
    val tableExists = catalog.tableExists(table.identifier)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table.identifier)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(
          s"${table.identifier} is a view. You may not write data into a view.")
      }
      if (!CHDataSourceUtils.isClickHouseTable(oldTable.provider)) {
        throw new AnalysisException(s"${table.identifier} is not a ClickHouse table. Please drop " +
          s"this table first if you would like to recreate it.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider")).getOrElse(ClickHouseConfig.NAME)
  }

  override def invalidateTable(ident: Identifier): Unit = {
    try {
      loadTable(ident) match {
        case v: ClickHouseTableV2 =>
          ScanMergeTreePartsUtils.scanMergeTreePartsToAddFile(spark.sessionState.newHadoopConf(),
            v)
          v.refresh()
      }
      super.invalidateTable(ident)
    }
    catch {
      case ignored: NoSuchTableException =>
      // ignore if the table doesn't exist, it is not cached
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if CHDataSourceUtils.isDeltaTable(v1.catalogTable) =>
          ClickHouseTableV2(
            spark,
            new Path(v1.catalogTable.location),
            catalogTable = Some(v1.catalogTable),
            tableIdentifier = Some(ident.toString))
        case o => o
      }
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
        if isPathIdentifier(ident) =>
        newDeltaPathTable(ident)
      case e: AnalysisException if gluePermissionError(e) && isPathIdentifier(ident) =>
        logWarning("Received an access denied error from Glue. Assuming this " +
          s"identifier ($ident) is path based.", e)
        newDeltaPathTable(ident)
    }
  }

  private def newDeltaPathTable(ident: Identifier): ClickHouseTableV2 = {
    ClickHouseTableV2(spark, new Path(ident.name()))
  }

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
                           properties: util.Map[String, String]): StagedTable = {
    throw new UnsupportedOperationException("Do not support stageCreate currently.")
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
                            properties: util.Map[String, String]): StagedTable = {
    throw new UnsupportedOperationException("Do not support stageReplace currently.")
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType,
                                    partitions: Array[Transform],
                                    properties: util.Map[String, String]): StagedTable = {
    throw new UnsupportedOperationException("Do not support stageCreateOrReplace currently.")
  }
}

/**
  * A trait for handling table access through clickhouse.`/some/path`. This is a stop-gap solution
  * until PathIdentifiers are implemented in Apache Spark.
  */
trait SupportsPathIdentifier extends TableCatalog {
  self: ClickHouseSparkCatalog =>

  protected lazy val catalog: SessionCatalog = spark.sessionState.catalog

  override def tableExists(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)) {
      val path = new Path(ident.name())
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(path) && fs.listStatus(path).nonEmpty
    } else {
      super.tableExists(ident)
    }
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasClickHouseNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  private def hasClickHouseNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 &&
      CHDataSourceUtils.isClickHouseDataSourceName(ident.namespace().head)
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(Identifier.of(table.identifier.database.toArray, table.identifier.table))
  }
}
