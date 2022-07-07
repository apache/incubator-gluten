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

import java.util.Locale

import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

object CHDataSourceUtils {

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(table: CatalogTable): Boolean =
    CHDataSourceUtils.isClickHouseTable(table.provider)

  /** Check whether this table is a ClickHouse table based on information from the Catalog. */
  def isClickHouseTable(provider: Option[String]): Boolean = {
    provider.exists(isClickHouseDataSourceName)
  }

  def isClickHouseDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == ClickHouseConfig.NAME ||
      name.toLowerCase(Locale.ROOT) == ClickHouseConfig.ALT_NAME
  }

  /** Find the root of a Delta table from the provided path. */
  def findClickHouseTableRoot(
                               spark: SparkSession,
                               path: Path,
                               options: Map[String, String] = Map.empty): Option[Path] = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConfWithOptions(options))
    var currentPath = path
    while (currentPath != null && currentPath.getName != ClickHouseConfig.METADATA_DIR) {
      val deltaLogPath = new Path(currentPath, ClickHouseConfig.METADATA_DIR)
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent
    }
    None
  }
}
