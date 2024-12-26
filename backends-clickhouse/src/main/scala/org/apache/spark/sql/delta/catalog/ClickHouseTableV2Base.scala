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

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogUtils}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.mergetree.{StorageMeta, TablePropertiesReader}

import org.apache.hadoop.fs.Path

trait ClickHouseTableV2Base extends TablePropertiesReader {

  def deltaProperties: Map[String, String]

  def deltaCatalog: Option[CatalogTable]

  def deltaPath: Path

  def deltaSnapshot: Snapshot

  def configuration: Map[String, String] = deltaProperties

  override protected def rawPartitionColumns: Seq[String] =
    deltaSnapshot.metadata.partitionColumns

  lazy val dataBaseName: String = deltaCatalog
    .map(_.identifier.database.getOrElse(StorageMeta.DEFAULT_CREATE_TABLE_DATABASE))
    .getOrElse(StorageMeta.DEFAULT_PATH_BASED_DATABASE)

  lazy val tableName: String = deltaCatalog
    .map(_.identifier.table)
    .getOrElse(deltaPath.toUri.getPath)

  lazy val clickhouseTableConfigs: Map[String, String] = {
    Map(StorageMeta.POLICY -> configuration.getOrElse(StorageMeta.POLICY, "default"))
  }

  def normalizedBucketSpec(tableCols: Seq[String], resolver: Resolver): Option[BucketSpec] = {
    if (deltaCatalog.isDefined) {
      bucketOption
    } else {
      // if deltaCatalog is not defined, it means saving to the path instead of saving to table
      bucketOption.map {
        bucketSpec => CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
      }
    }
  }
}
