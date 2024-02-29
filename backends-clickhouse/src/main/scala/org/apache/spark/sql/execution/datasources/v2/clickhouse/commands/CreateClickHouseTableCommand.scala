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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.commands

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.v2.clickhouse.DeltaLogAdapter
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Single entry point for all write or declaration operations for Delta tables accessed through the
 * table name.
 *
 * @param table
 *   The table identifier for the Delta table
 * @param existingTableOpt
 *   The existing table for the same identifier if exists
 * @param mode
 *   The save mode when writing data. Relevant when the query is empty or set to Ignore with `CREATE
 *   TABLE IF NOT EXISTS`.
 * @param query
 *   The query to commit into the Delta table if it exist. This can come from
 *   - CTAS
 *   - saveAsTable
 */
case class CreateClickHouseTableCommand(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan] = None,
    operation: TableCreationModes.CreationMode = TableCreationModes.Create,
    tableByPath: Boolean = false,
    override val output: Seq[Attribute] = Nil)
  extends LeafRunnableCommand
  with DeltaLogging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.identifier.database.isDefined, "Database should've been fixed at analysis")
    // There is a subtle race condition here, where the table can be created by someone else
    // while this command is running. Nothing we can do about that though :(
    val tableExists = existingTableOpt.isDefined
    if (mode == SaveMode.Ignore && tableExists) {
      // Early exit on ignore
      return Nil
    } else if (mode == SaveMode.ErrorIfExists && tableExists) {
      throw new AnalysisException(s"Table ${table.identifier.quotedString} already exists.")
    }

    val tableWithLocation = if (tableExists) {
      val existingTable = existingTableOpt.get
      table.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          val tableName = table.identifier.quotedString
          throw new AnalysisException(
            s"The location of the existing table $tableName is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${table.location}`.")
        case _ =>
      }
      table.copy(storage = existingTable.storage, tableType = existingTable.tableType)
    } else if (table.storage.locationUri.isEmpty) {
      // We are defining a new managed table
      assert(table.tableType == CatalogTableType.MANAGED)
      val loc = sparkSession.sessionState.catalog.defaultTablePath(table.identifier)
      table.copy(storage = table.storage.copy(locationUri = Some(loc)))
    } else {
      // We are defining a new external table
      assert(table.tableType == CatalogTableType.EXTERNAL)
      table
    }

    val isManagedTable = tableWithLocation.tableType == CatalogTableType.MANAGED
    val tableLocation = new Path(tableWithLocation.location)
    val fs = tableLocation.getFileSystem(sparkSession.sessionState.newHadoopConf())
    val deltaLog = DeltaLog.forTable(sparkSession, tableLocation)
    val options = new DeltaOptions(table.storage.properties, sparkSession.sessionState.conf)
    var result: Seq[Row] = Nil

    recordDeltaOperation(deltaLog, "delta.ddl.createTable") {
      val txn = deltaLog.startTransaction()
      if (query.isDefined) {
        // TODO: implement writing clickhouse data
        Seq.empty[Row]
      } else {
        def createTransactionLogOrVerify(): Unit = {
          if (isManagedTable) {
            // When creating a managed table, the table path should not exist or is empty, or
            // users would be surprised to see the data, or see the data directory being dropped
            // after the table is dropped.
            // assertPathEmpty(sparkSession, tableWithLocation)
          }

          // This is either a new table, or, we never defined the schema of the table. While it is
          // unexpected that `txn.metadata.schema` to be empty when txn.readVersion >= 0, we still
          // guard against it, in case of checkpoint corruption bugs.
          val noExistingMetadata = txn.readVersion == -1 || txn.metadata.schema.isEmpty
          if (noExistingMetadata) {
            assertTableSchemaDefined(fs, tableLocation, tableWithLocation, sparkSession)
            // assertPathEmpty(sparkSession, tableWithLocation)
            // This is a user provided schema.
            // Doesn't come from a query, Follow nullability invariants.
            val newMetadata = getProvidedMetadata(table, table.schema.json)
            txn.updateMetadataForNewTable(newMetadata)

            val op = getOperation(newMetadata, isManagedTable, None)
            txn.commit(Nil, op)
          } else {
            val newProperties = DeltaConfigs.mergeGlobalConfigs(
              sparkSession.sessionState.conf,
              tableWithLocation.properties)
            verifyTableMetadata(txn, tableWithLocation, newProperties)
          }
        }
        // We are defining a table using the Create or Replace Table statements.
        operation match {
          case TableCreationModes.Create =>
            require(!tableExists, "Can't recreate a table when it exists")
            createTransactionLogOrVerify()

          case TableCreationModes.CreateOrReplace if !tableExists =>
            // If the table doesn't exist, CREATE OR REPLACE must provide a schema
            if (tableWithLocation.schema.isEmpty) {
              throw DeltaErrors.schemaNotProvidedException
            }
            createTransactionLogOrVerify()
          case _ =>
          // TODO:
        }
      }

      // We would have failed earlier on if we couldn't ignore the existence of the table
      // In addition, we just might using saveAsTable to append to the table, so ignore the creation
      // if it already exists.
      // Note that someone may have dropped and recreated the table in a separate location in the
      // meantime... Unfortunately we can't do anything there at the moment, because Hive sucks.
      logInfo(s"Table is path-based table: $tableByPath. Update catalog with mode: $operation")
      updateCatalog(sparkSession, tableWithLocation, DeltaLogAdapter.snapshot(deltaLog), txn)

      result
    }
  }

  private def getProvidedMetadata(table: CatalogTable, schemaString: String): Metadata = {
    Metadata(
      format = Format(table.properties("engine")),
      description = table.comment.orNull,
      schemaString = schemaString,
      partitionColumns = table.partitionColumnNames,
      configuration = table.properties
    )
  }

  private def assertTableSchemaDefined(
      fs: FileSystem,
      path: Path,
      table: CatalogTable,
      sparkSession: SparkSession): Unit = {
    // Users did not specify the schema. We expect the schema exists in Delta.
    if (table.schema.isEmpty) {
      if (table.tableType == CatalogTableType.EXTERNAL) {
        if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
          throw DeltaErrors.createExternalTableWithoutLogException(
            path,
            table.identifier.quotedString,
            sparkSession)
        } else {
          throw DeltaErrors.createExternalTableWithoutSchemaException(
            path,
            table.identifier.quotedString,
            sparkSession)
        }
      } else {
        throw DeltaErrors.createManagedTableWithoutSchemaException(
          table.identifier.quotedString,
          sparkSession)
      }
    }
  }

  /**
   * Verify against our transaction metadata that the user specified the right metadata for the
   * table.
   */
  private def verifyTableMetadata(
      txn: OptimisticTransaction,
      tableDesc: CatalogTable,
      properties: Map[String, String]): Unit = {
    val existingMetadata = txn.metadata
    val path = new Path(tableDesc.location)

    // The delta log already exists. If they give any configuration, we'll make sure it all matches.
    // Otherwise we'll just go with the metadata already present in the log.
    // The schema compatibility checks will be made in `WriteIntoDelta` for CreateTable
    // with a query
    if (txn.readVersion > -1) {
      if (tableDesc.schema.nonEmpty) {
        // We check exact alignment on create table if everything is provided
        val differences = SchemaUtils.reportDifferences(existingMetadata.schema, tableDesc.schema)
        if (differences.nonEmpty) {
          throw DeltaErrors.createTableWithDifferentSchemaException(
            path,
            tableDesc.schema,
            existingMetadata.schema,
            differences)
        }
      }

      // If schema is specified, we must make sure the partitioning matches, even the partitioning
      // is not specified.
      if (
        tableDesc.schema.nonEmpty &&
        tableDesc.partitionColumnNames != existingMetadata.partitionColumns
      ) {
        throw DeltaErrors.createTableWithDifferentPartitioningException(
          path,
          tableDesc.partitionColumnNames,
          existingMetadata.partitionColumns)
      }

      if (properties.nonEmpty && properties != existingMetadata.configuration) {
        throw DeltaErrors.createTableWithDifferentPropertiesException(
          path,
          tableDesc.properties,
          existingMetadata.configuration)
      }
    }
  }

  /**
   * Based on the table creation operation, and parameters, we can resolve to different operations.
   * A lot of this is needed for legacy reasons in Databricks Runtime.
   *
   * @param metadata
   *   The table metadata, which we are creating or replacing
   * @param isManagedTable
   *   Whether we are creating or replacing a managed table
   * @param options
   *   Write options, if this was a CTAS/RTAS
   */
  private def getOperation(
      metadata: Metadata,
      isManagedTable: Boolean,
      options: Option[DeltaOptions]): DeltaOperations.Operation = operation match {
    // This is legacy saveAsTable behavior in Databricks Runtime
    case TableCreationModes.Create if existingTableOpt.isDefined && query.isDefined =>
      DeltaOperations.Write(mode, Option(table.partitionColumnNames), options.get.replaceWhere)

    // DataSourceV2 table creation
    case TableCreationModes.Create =>
      DeltaOperations.CreateTable(metadata, isManagedTable, query.isDefined)

    // DataSourceV2 table replace
    case TableCreationModes.Replace =>
      DeltaOperations.ReplaceTable(metadata, isManagedTable, orCreate = false, query.isDefined)

    // Legacy saveAsTable with Overwrite mode
    case TableCreationModes.CreateOrReplace if options.exists(_.replaceWhere.isDefined) =>
      DeltaOperations.Write(mode, Option(table.partitionColumnNames), options.get.replaceWhere)

    // New DataSourceV2 saveAsTable with overwrite mode behavior
    case TableCreationModes.CreateOrReplace =>
      DeltaOperations.ReplaceTable(metadata, isManagedTable, orCreate = true, query.isDefined)
  }

  /**
   * Similar to getOperation, here we disambiguate the catalog alterations we need to do based on
   * the table operation, and whether we have reached here through legacy code or DataSourceV2 code
   * paths.
   */
  private def updateCatalog(
      spark: SparkSession,
      table: CatalogTable,
      snapshot: Snapshot,
      txn: OptimisticTransaction): Unit = {
    val cleaned = cleanupTableDefinition(table, snapshot)
    operation match {
      case _ if tableByPath => // do nothing with the metastore if this is by path
      case TableCreationModes.Create =>
        spark.sessionState.catalog.createTable(
          cleaned,
          ignoreIfExists = existingTableOpt.isDefined,
          validateLocation = false)
      case TableCreationModes.Replace | TableCreationModes.CreateOrReplace
          if existingTableOpt.isDefined =>
        spark.sessionState.catalog.alterTable(table)
      case TableCreationModes.Replace =>
        val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
        throw new CannotReplaceMissingTableException(ident)
      case TableCreationModes.CreateOrReplace =>
        spark.sessionState.catalog
          .createTable(cleaned, ignoreIfExists = false, validateLocation = false)
    }
  }

  /** Clean up the information we pass on to store in the catalog. */
  private def cleanupTableDefinition(table: CatalogTable, snapshot: Snapshot): CatalogTable = {
    // These actually have no effect on the usability of Delta, but feature flagging legacy
    // behavior for now
    val storageProps =
      if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
        // Legacy behavior
        table.storage
      } else {
        table.storage.copy(properties = Map.empty)
      }

    table.copy(
      schema = new StructType(),
      properties = Map.empty,
      partitionColumnNames = Nil,
      // Remove write specific options when updating the catalog
      storage = storageProps,
      tracksPartitionsInCatalog = true
    )
  }

  private def assertPathEmpty(sparkSession: SparkSession, tableWithLocation: CatalogTable): Unit = {
    val path = new Path(tableWithLocation.location)
    val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
    // Verify that the table location associated with CREATE TABLE doesn't have any data. Note that
    // we intentionally diverge from this behavior w.r.t regular datasource tables (that silently
    // overwrite any previous data)
    if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
      throw new AnalysisException(
        s"Cannot create table ('${tableWithLocation.identifier}')." +
          s" The associated location ('${tableWithLocation.location}') is not empty but " +
          s"it's not a ClickHouse table")
    }
  }

}
