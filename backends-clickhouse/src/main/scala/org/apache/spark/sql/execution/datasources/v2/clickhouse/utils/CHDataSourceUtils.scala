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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.DeltaTableIdentifier.gluePermissionError
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import org.apache.hadoop.fs.Path

import java.util.Locale

import scala.util.Try

object CHDataSourceUtils extends Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isClickHouseTable(table: CatalogTable): Boolean =
    CHDataSourceUtils.isClickHouseTable(table.provider)

  /** Check whether this table is a ClickHouse table based on information from the Catalog. */
  def isClickHouseTable(provider: Option[String]): Boolean = {
    provider.exists(isClickHouseDataSourceName)
  }

  def isClickHouseDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == ClickHouseConfig.NAME ||
    name.toLowerCase(Locale.ROOT) == ClickHouseConfig.ALT_NAME
  }

  /**
   * Check whether the provided table name is a Clickhouse table based on information from the
   * Catalog.
   */
  def isClickHouseTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTempView(tableName)
    val tableExists = {
      (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
      catalog.tableExists(tableName)
    }
    tableIsNotTemporaryTable &&
    tableExists &&
    isClickHouseTable(catalog.getTableMetadata(tableName))
  }

  /** Check the specified table identifier represents a Clickhouse path. */
  def isClickhousePath(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog

    def tableIsTemporaryTable = catalog.isTempView(identifier)

    def tableExists: Boolean = {
      try {
        catalog.databaseExists(identifier.database.get) && catalog.tableExists(identifier)
      } catch {
        case e: AnalysisException if gluePermissionError(e) =>
          logWarning(
            "Received an access denied error from Glue. Will check to see if this " +
              s"identifier ($identifier) is path based.",
            e)
          false
      }
    }

    spark.sessionState.conf.runSQLonFile &&
    isClickHouseTable(identifier.database) &&
    !tableIsTemporaryTable &&
    !tableExists &&
    new Path(identifier.table).isAbsolute
  }

  /** Find the root of a Delta table from the provided path. */
  def findClickHouseTableRoot(
      spark: SparkSession,
      path: Path,
      options: Map[String, String] = Map.empty): Option[Path] = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConfWithOptions(options))
    var currentPath = path
    while (
      currentPath != null && currentPath.getName != ClickHouseConfig.METADATA_DIR &&
      currentPath.getName != "_samples"
    ) {
      val deltaLogPath = new Path(currentPath, ClickHouseConfig.METADATA_DIR)
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent
    }
    None
  }

  // Ensure ClickHouseTableV2 table exists
  def ensureClickHouseTableV2(
      tableId: Option[TableIdentifier],
      sparkSession: SparkSession): Unit = {
    if (tableId.isEmpty) {
      throw new UnsupportedOperationException("Current command requires table identifier.")
    }
    // If user comes into this function without previously triggering loadTable
    // (which creates ClickhouseTableV2), we have to load the table manually
    // Notice: Multi-catalog case is not well considered!
    val table = sparkSession.sessionState.catalogManager.currentCatalog.asTableCatalog.loadTable(
      Identifier.of(
        Array(
          tableId.get.database.getOrElse(
            sparkSession.sessionState.catalogManager.currentNamespace.head)),
        tableId.get.table)
    )
    table.name()
  }
}
