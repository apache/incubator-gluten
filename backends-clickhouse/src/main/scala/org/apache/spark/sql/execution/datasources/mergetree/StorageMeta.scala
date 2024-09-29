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
package org.apache.spark.sql.execution.datasources.mergetree

import org.apache.gluten.expression.ConverterUtils.normalizeColName

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.clickhouse.utils.MergeTreeDeltaUtil

import org.apache.hadoop.fs.Path

/** Reserved table property for MergeTree table. */
object StorageMeta {
  val Provider: String = "clickhouse"
  val DEFAULT_FILE_FORMAT: String = "write.format.default"
  val DEFAULT_FILE_FORMAT_DEFAULT: String = "mergetree"

  // Storage properties
  val DefaultStorageDB: String = "default"
  val STORAGE_DB: String = "storage_db"
  val STORAGE_TABLE: String = "storage_table"
  val STORAGE_SNAPSHOT_ID: String = "storage_snapshot_id"
  val STORAGE_PATH: String = "storage_path"

  val SERIALIZER_HEADER: String = "MergeTree;"

  def withMoreStorageInfo(
      metadata: Metadata,
      snapshotId: String,
      deltaPath: Path,
      database: String,
      tableName: String): Metadata = {
    val newOptions =
      metadata.configuration ++ Seq(
        STORAGE_DB -> database,
        STORAGE_SNAPSHOT_ID -> snapshotId,
        STORAGE_TABLE -> tableName,
        STORAGE_PATH -> deltaPath.toString
      )
    metadata.copy(configuration = newOptions)
  }
}

trait TablePropertiesReader {

  def configuration: Map[String, String]

  /** delta */
  def metadata: Metadata

  private def getCommaSeparatedColumns(keyName: String): Option[Seq[String]] = {
    configuration.get(keyName).map {
      v =>
        val keys = v.split(",").map(n => normalizeColName(n.trim)).toSeq
        keys.foreach {
          s =>
            if (s.contains(".")) {
              throw new IllegalStateException(
                s"$keyName $s can not contain '.' (not support nested column yet)")
            }
        }
        keys
    }
  }

  lazy val bucketOption: Option[BucketSpec] = {
    val tableProperties = configuration
    if (tableProperties.contains("numBuckets")) {
      val numBuckets = tableProperties("numBuckets").trim.toInt
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

  lazy val partitionColumns: Seq[String] =
    metadata.partitionColumns.map(normalizeColName)

  lazy val orderByKeyOption: Option[Seq[String]] = {
    val orderByKeys =
      if (bucketOption.exists(_.sortColumnNames.nonEmpty)) {
        bucketOption.map(_.sortColumnNames.map(normalizeColName))
      } else {
        getCommaSeparatedColumns("orderByKey")
      }
    orderByKeys
      .map(_.intersect(partitionColumns))
      .filter(_.nonEmpty)
      .foreach {
        invalidKeys =>
          throw new IllegalStateException(
            s"partition cols $invalidKeys can not be in the order by keys.")
      }
    orderByKeys
  }

  lazy val primaryKeyOption: Option[Seq[String]] = {
    orderByKeyOption.map(_.mkString(",")).flatMap {
      orderBy =>
        val primaryKeys = getCommaSeparatedColumns("primaryKey")
        primaryKeys
          .map(_.mkString(","))
          .filterNot(orderBy.startsWith)
          .foreach(
            primaryKey =>
              throw new IllegalStateException(
                s"Primary key $primaryKey must be a prefix of the sorting key $orderBy"))
        primaryKeys
    }
  }

  lazy val writeConfiguration: Map[String, String] = {
    val (orderByKey0, primaryKey0) = MergeTreeDeltaUtil.genOrderByAndPrimaryKeyStr(
      orderByKeyOption,
      primaryKeyOption
    )
    Map(
      "storage_policy" -> configuration.getOrElse("storage_policy", "default"),
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
}
