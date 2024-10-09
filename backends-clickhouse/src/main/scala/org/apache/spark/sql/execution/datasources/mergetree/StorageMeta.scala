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

import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.delta.actions.Metadata

import java.net.URI

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

  // TODO: remove this method
  def genOrderByAndPrimaryKeyStr(
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]]): (String, String) = {

    val orderByKey = columnsToStr(orderByKeyOption, DEFAULT_ORDER_BY_KEY)
    val primaryKey = if (orderByKey == DEFAULT_ORDER_BY_KEY) "" else columnsToStr(primaryKeyOption)
    (orderByKey, primaryKey)
  }

  def columnsToStr(option: Option[Seq[String]], default: String = ""): String =
    option
      .filter(_.nonEmpty)
      .map(keys => keys.map(ConverterUtils.normalizeColName).mkString(","))
      .getOrElse(default)
}

/** all properties start with 'storage_' */
trait StorageConfigProvider {
  val storageConf: Map[String, String]
}

trait TablePropertiesReader {

  def configuration: Map[String, String]

  val partitionColumns: Seq[String]

  private def getCommaSeparatedColumns(keyName: String): Option[Seq[String]] = {
    configuration.get(keyName).map {
      v =>
        val keys = v.split(",").map(n => ConverterUtils.normalizeColName(n.trim)).toSeq
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

  lazy val orderByKeyOption: Option[Seq[String]] = {
    val orderByKeys =
      if (bucketOption.exists(_.sortColumnNames.nonEmpty)) {
        bucketOption.map(_.sortColumnNames.map(ConverterUtils.normalizeColName))
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
}
