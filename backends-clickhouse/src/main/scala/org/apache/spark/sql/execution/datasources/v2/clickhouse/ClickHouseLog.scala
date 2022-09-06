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

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableIdentifier}
import org.apache.spark.util.{Clock, SystemClock}

object ClickHouseLog {

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath.getAbsolutePath,
      ClickHouseConfig.METADATA_DIR), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String, clock: Clock): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath, ClickHouseConfig.METADATA_DIR), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath.getAbsolutePath, ClickHouseConfig.METADATA_DIR), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath, ClickHouseConfig.METADATA_DIR), clock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, table: CatalogTable): DeltaLog = {
    forTable(spark, table, new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, deltaTable: DeltaTableIdentifier): DeltaLog = {
    if (deltaTable.path.isDefined) {
      forTable(spark, deltaTable.path.get)
    } else {
      forTable(spark, deltaTable.table.get)
    }
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath, ClickHouseConfig.METADATA_DIR), new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, tableName: TableIdentifier): DeltaLog = {
    forTable(spark, tableName, new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, tableName: TableIdentifier, clock: Clock): DeltaLog = {
    if (DeltaTableIdentifier.isDeltaPath(spark, tableName)) {
      forTable(spark, new Path(tableName.table))
    } else {
      forTable(spark, spark.sessionState.catalog.getTableMetadata(tableName), clock)
    }
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath, ClickHouseConfig.METADATA_DIR), new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, table: CatalogTable, clock: Clock): DeltaLog = {
    DeltaLog.apply(spark, new Path(new Path(table.location),
      ClickHouseConfig.METADATA_DIR), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, options: Map[String, String]): DeltaLog = {
    DeltaLog.apply(spark, new Path(dataPath, ClickHouseConfig.METADATA_DIR),
      options, new SystemClock)
  }

  // TODO: use the default path "_delta_log" as metadata path
  def clearCache(): Unit = {
    DeltaLog.clearCache()
  }
}
