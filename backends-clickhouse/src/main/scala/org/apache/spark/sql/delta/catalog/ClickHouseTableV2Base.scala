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
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.clickhouse.utils.MergeTreeDeltaUtil
import org.apache.spark.sql.execution.datasources.mergetree.TablePropertiesReader

import org.apache.hadoop.fs.Path

trait ClickHouseTableV2Base extends TablePropertiesReader {

  val DEFAULT_DATABASE = "clickhouse_db"

  def deltaProperties: Map[String, String]

  def deltaCatalog: Option[CatalogTable]

  def deltaPath: Path

  def deltaSnapshot: Snapshot

  def configuration: Map[String, String] = deltaProperties

  def metadata: Metadata = deltaSnapshot.metadata

  lazy val dataBaseName: String = deltaCatalog
    .map(_.identifier.database.getOrElse("default"))
    .getOrElse(DEFAULT_DATABASE)

  lazy val tableName: String = deltaCatalog
    .map(_.identifier.table)
    .getOrElse(deltaPath.toUri.getPath)

  lazy val clickhouseTableConfigs: Map[String, String] = {
    val (orderByKey0, primaryKey0) = MergeTreeDeltaUtil.genOrderByAndPrimaryKeyStr(
      orderByKeyOption,
      primaryKeyOption
    )
    Map(
      "storage_policy" -> deltaProperties.getOrElse("storage_policy", "default"),
      "storage_db" -> dataBaseName,
      "storage_table" -> tableName,
      "storage_orderByKey" -> orderByKey0,
      "storage_lowCardKey" -> lowCardKeyOption.map(MergeTreeDeltaUtil.columnsToStr).getOrElse(""),
      "storage_minmaxIndexKey" -> minmaxIndexKeyOption
        .map(MergeTreeDeltaUtil.columnsToStr)
        .getOrElse(""),
      "storage_bfIndexKey" -> bfIndexKeyOption.map(MergeTreeDeltaUtil.columnsToStr).getOrElse(""),
      "storage_setIndexKey" -> setIndexKeyOption.map(MergeTreeDeltaUtil.columnsToStr).getOrElse(""),
      "storage_primaryKey" -> primaryKey0
    )
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
