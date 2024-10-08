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

  // Storage properties
  val DEFAULT_PATH_BASED_DATABASE: String = "clickhouse_db"
  val DEFAULT_CREATE_TABLE_DATABASE: String = "default"
  val DB: String = "storage_db"
  val TABLE: String = "storage_table"
  val SNAPSHOT_ID: String = "storage_snapshot_id"
  val STORAGE_PATH: String = "storage_path"
  val POLICY: String = "storage_policy"
  val ORDER_BY_KEY: String = "storage_orderByKey"
  val LOW_CARD_KEY: String = "storage_lowCardKey"
  val MINMAX_INDEX_KEY: String = "storage_minmaxIndexKey"
  val BF_INDEX_KEY: String = "storage_bfIndexKey"
  val SET_INDEX_KEY: String = "storage_setIndexKey"
  val PRIMARY_KEY: String = "storage_primaryKey"
  val SERIALIZER_HEADER: String = "MergeTree;"

  def withMoreStorageInfo(
      metadata: Metadata,
      snapshotId: String,
      deltaPath: Path,
      database: String,
      tableName: String): Metadata = {
    val moreOptions = Seq(
      DB -> database,
      SNAPSHOT_ID -> snapshotId,
      TABLE -> tableName,
      STORAGE_PATH -> deltaPath.toString)
    withMoreOptions(metadata, moreOptions)
  }

  private def withMoreOptions(metadata: Metadata, newOptions: Seq[(String, String)]): Metadata = {
    metadata.copy(configuration = metadata.configuration ++ newOptions)
  }
}

trait WriteConfiguration {
  val writeConfiguration: Map[String, String]
}

trait TablePropertiesReader extends WriteConfiguration {

  def configuration: Map[String, String]

  val partitionColumns: Seq[String]

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
      StorageMeta.POLICY -> configuration.getOrElse(StorageMeta.POLICY, "default"),
      StorageMeta.ORDER_BY_KEY -> orderByKey0,
      StorageMeta.LOW_CARD_KEY -> lowCardKeyOption
        .map(MergeTreeDeltaUtil.columnsToStr)
        .getOrElse(""),
      StorageMeta.MINMAX_INDEX_KEY -> minmaxIndexKeyOption
        .map(MergeTreeDeltaUtil.columnsToStr)
        .getOrElse(""),
      StorageMeta.BF_INDEX_KEY -> bfIndexKeyOption
        .map(MergeTreeDeltaUtil.columnsToStr)
        .getOrElse(""),
      StorageMeta.SET_INDEX_KEY -> setIndexKeyOption
        .map(MergeTreeDeltaUtil.columnsToStr)
        .getOrElse(""),
      StorageMeta.PRIMARY_KEY -> primaryKey0
    )
  }
}
