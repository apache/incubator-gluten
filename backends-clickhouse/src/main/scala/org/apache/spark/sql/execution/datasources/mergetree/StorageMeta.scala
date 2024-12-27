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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util.Locale

/** Reserved table property for MergeTree table. */
object StorageMeta {

  // Storage properties
  val DEFAULT_PATH_BASED_DATABASE: String = "clickhouse_db"
  val DEFAULT_CREATE_TABLE_DATABASE: String = "default"
  val DEFAULT_ORDER_BY_KEY = "tuple()"
  val STORAGE_PREFIX: String = "storage_"
  val DB: String = prefixOf("db")
  val TABLE: String = prefixOf("table")
  val SNAPSHOT_ID: String = prefixOf("snapshot_id")
  val STORAGE_PATH: String = prefixOf("path")
  val POLICY: String = prefixOf("policy")
  val ORDER_BY_KEY: String = prefixOf("orderByKey")
  val LOW_CARD_KEY: String = prefixOf("lowCardKey")
  val MINMAX_INDEX_KEY: String = prefixOf("minmaxIndexKey")
  val BF_INDEX_KEY: String = prefixOf("bfIndexKey")
  val SET_INDEX_KEY: String = prefixOf("setIndexKey")
  val PRIMARY_KEY: String = prefixOf("primaryKey")
  val SERIALIZER_HEADER: String = "MergeTree;"

  private def prefixOf(key: String): String = s"$STORAGE_PREFIX$key"

  def withStorageID(
      metadata: Metadata,
      database: String,
      tableName: String,
      snapshotId: String): Metadata = {
    val moreOptions = Seq(DB -> database, SNAPSHOT_ID -> snapshotId, TABLE -> tableName)
    withMoreOptions(metadata, moreOptions)
  }

  private def withMoreOptions(metadata: Metadata, newOptions: Seq[(String, String)]): Metadata = {
    metadata.copy(configuration = metadata.configuration ++ newOptions)
  }

  def normalizeRelativePath(relativePath: String): String = {
    val table_uri = URI.create(relativePath)
    if (table_uri.getPath.startsWith("/")) {
      table_uri.getPath.substring(1)
    } else table_uri.getPath
  }
}

/** all properties start with 'storage_' */
trait StorageConfigProvider {
  val storageConf: Map[String, String]
}

trait TablePropertiesReader {

  def configuration: Map[String, String]

  protected def rawPartitionColumns: Seq[String]

  protected val tableSchema: StructType

  private lazy val columnNameMapping =
    tableSchema.fieldNames.map(n => n.toLowerCase(Locale.ROOT) -> n).toMap

  private def normalizeColName(columnName: String): String = {
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    if (caseSensitive) {
      columnName
    } else {
      columnNameMapping.getOrElse(columnName.toLowerCase(Locale.ROOT), columnName)
    }
  }

  private def getCommaSeparatedColumns(keyName: String): Seq[String] = {
    val keys = configuration
      .get(keyName)
      .map(_.split(",").map(k => normalizeColName(k.trim)).toSeq)
      .getOrElse(Nil)
    keys.filter(_.contains(".")).foreach {
      s =>
        throw new IllegalStateException(
          s"Column $s can not contain '.' (not support nested column yet)")
    }
    keys
  }

  lazy val partitionColumns: Seq[String] = {
    rawPartitionColumns.map(normalizeColName)
  }

  lazy val bucketOption: Option[BucketSpec] = {
    val tableProperties = configuration
    if (tableProperties.contains("numBuckets")) {
      val numBuckets = tableProperties("numBuckets").trim.toInt
      val bucketColumnNames: Seq[String] =
        getCommaSeparatedColumns("bucketColumnNames")
      val sortColumnNames: Seq[String] =
        getCommaSeparatedColumns("orderByKey")
      Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames))
    } else {
      None
    }
  }

  private lazy val orderByKeys: Seq[String] = {
    val orderBys =
      if (bucketOption.exists(_.sortColumnNames.nonEmpty)) {
        bucketOption.map(_.sortColumnNames).getOrElse(Nil)
      } else {
        getCommaSeparatedColumns("orderByKey")
      }
    val invalidKey = orderBys.intersect(partitionColumns).mkString(",")
    if (invalidKey.nonEmpty) {
      throw new IllegalStateException(
        s"partition column(s) $invalidKey can not be in the order by keys.")
    }
    orderBys
  }

  private lazy val primaryKeys: Seq[String] = {
    val orderBys = orderByKeys.mkString(",")
    val primaryKeys = getCommaSeparatedColumns("primaryKey")
    primaryKeys.zip(orderByKeys).foreach {
      case (primaryKey, orderBy) =>
        if (orderBy != primaryKey) {
          throw new IllegalStateException(
            s"Primary key $primaryKey must be a prefix of the sorting key $orderBys")
        }
    }
    primaryKeys
  }

  lazy val lowCardKey: String = getCommaSeparatedColumns("lowCardKey").mkString(",")

  lazy val minmaxIndexKey: String = getCommaSeparatedColumns("minmaxIndexKey").mkString(",")

  lazy val bfIndexKey: String = getCommaSeparatedColumns("bloomfilterIndexKey").mkString(",")

  lazy val setIndexKey: String = getCommaSeparatedColumns("setIndexKey").mkString(",")

  lazy val primaryKey: String = primaryKeys.mkString(",")

  lazy val orderByKey: String =
    if (orderByKeys.nonEmpty) orderByKeys.mkString(",") else StorageMeta.DEFAULT_ORDER_BY_KEY
}
