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

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.expression.ConverterUtils.normalizeColName

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.utils.MergeTreeDeltaUtil

import org.apache.hadoop.fs.Path

import java.{util => ju}

trait ClickHouseTableV2Base {

  def deltaProperties(): ju.Map[String, String]

  def deltaCatalog(): Option[CatalogTable]

  def deltaPath(): Path

  def deltaSnapshot(): Snapshot

  lazy val dataBaseName = deltaCatalog
    .map(_.identifier.database.getOrElse("default"))
    .getOrElse("clickhouse")

  lazy val tableName = deltaCatalog
    .map(_.identifier.table)
    .getOrElse(deltaPath.toUri.getPath)

  lazy val bucketOption: Option[BucketSpec] = {
    val tableProperties = deltaProperties
    if (tableProperties.containsKey("numBuckets")) {
      val numBuckets = tableProperties.get("numBuckets").trim.toInt
      val bucketColumnNames: Seq[String] =
        getCommaSeparatedColumns("bucketColumnNames").getOrElse(Seq.empty[String])
      val sortColumnNames: Seq[String] =
        getCommaSeparatedColumns("orderByKey").getOrElse(Seq.empty[String])
      Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames))
    } else {
      None
    }
  }

  lazy val lowCardKeyOption: Option[Seq[String]] = {
    getCommaSeparatedColumns("lowCardKey")
  }

  lazy val minmaxIndexKeyOption: Option[Seq[String]] = {
    getCommaSeparatedColumns("minmaxIndexKey")
  }

  lazy val bfIndexKeyOption: Option[Seq[String]] = {
    getCommaSeparatedColumns("bloomfilterIndexKey")
  }

  lazy val setIndexKeyOption: Option[Seq[String]] = {
    getCommaSeparatedColumns("setIndexKey")
  }

  private def getCommaSeparatedColumns(keyName: String) = {
    val tableProperties = deltaProperties
    if (tableProperties.containsKey(keyName)) {
      if (tableProperties.get(keyName).nonEmpty) {
        val keys = tableProperties
          .get(keyName)
          .split(",")
          .map(n => ConverterUtils.normalizeColName(n.trim))
          .toSeq
        keys.foreach(
          s => {
            if (s.contains(".")) {
              throw new IllegalStateException(
                s"$keyName $s can not contain '.' (not support nested column yet)")
            }
          })
        Some(keys)
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val orderByKeyOption: Option[Seq[String]] = {
    if (bucketOption.isDefined && bucketOption.get.sortColumnNames.nonEmpty) {
      val orderByKeys = bucketOption.get.sortColumnNames.map(normalizeColName).toSeq
      val invalidKeys = orderByKeys.intersect(partitionColumns)
      if (invalidKeys.nonEmpty) {
        throw new IllegalStateException(
          s"partition cols $invalidKeys can not be in the order by keys.")
      }
      Some(orderByKeys)
    } else {
      val orderByKeys = getCommaSeparatedColumns("orderByKey")
      if (orderByKeys.isDefined) {
        val invalidKeys = orderByKeys.get.intersect(partitionColumns)
        if (invalidKeys.nonEmpty) {
          throw new IllegalStateException(
            s"partition cols $invalidKeys can not be in the order by keys.")
        }
        orderByKeys
      } else {
        None
      }
    }
  }

  lazy val primaryKeyOption: Option[Seq[String]] = {
    if (orderByKeyOption.isDefined) {
      val primaryKeys = getCommaSeparatedColumns("primaryKey")
      if (
        primaryKeys.isDefined && !orderByKeyOption.get
          .mkString(",")
          .startsWith(primaryKeys.get.mkString(","))
      ) {
        throw new IllegalStateException(
          s"Primary key $primaryKeys must be a prefix of the sorting key")
      }
      primaryKeys
    } else {
      None
    }
  }

  lazy val partitionColumns = deltaSnapshot.metadata.partitionColumns.map(normalizeColName).toSeq

  lazy val clickhouseTableConfigs: Map[String, String] = {
    val tableProperties = deltaProperties()
    val configs = scala.collection.mutable.Map[String, String]()
    configs += ("storage_policy" -> tableProperties.getOrDefault("storage_policy", "default"))
    configs.toMap
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
