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

import org.apache.gluten.expression.ConverterUtils.normalizeColName

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.clickhouse.utils.MergeTreeDeltaUtil
import org.apache.spark.sql.execution.datasources.mergetree.{StorageMeta, TablePropertiesReader}

import org.apache.hadoop.fs.Path

trait ClickHouseTableV2Base extends TablePropertiesReader {

  def deltaProperties: Map[String, String]

  def deltaCatalog: Option[CatalogTable]

  def deltaPath: Path

  def deltaSnapshot: Snapshot

  def configuration: Map[String, String] = deltaProperties

  override lazy val partitionColumns: Seq[String] =
    deltaSnapshot.metadata.partitionColumns.map(normalizeColName)

  lazy val dataBaseName: String = deltaCatalog
    .map(_.identifier.database.getOrElse(StorageMeta.DEFAULT_CREATE_TABLE_DATABASE))
    .getOrElse(StorageMeta.DEFAULT_PATH_BASED_DATABASE)

  lazy val tableName: String = deltaCatalog
    .map(_.identifier.table)
    .getOrElse(deltaPath.toUri.getPath)

  lazy val clickhouseTableConfigs: Map[String, String] = {
    Map("storage_policy" -> deltaProperties.getOrElse("storage_policy", "default"))
  }

  def primaryKey(): String = MergeTreeDeltaUtil.columnsToStr(primaryKeyOption)

  def orderByKey(): String = orderByKeyOption match {
    case Some(keys) => keys.map(normalizeColName).mkString(",")
    case None => "tuple()"
  }

  def lowCardKey(): String = MergeTreeDeltaUtil.columnsToStr(lowCardKeyOption)
  def minmaxIndexKey(): String = MergeTreeDeltaUtil.columnsToStr(minmaxIndexKeyOption)
  def bfIndexKey(): String = MergeTreeDeltaUtil.columnsToStr(bfIndexKeyOption)
  def setIndexKey(): String = MergeTreeDeltaUtil.columnsToStr(setIndexKeyOption)
}
