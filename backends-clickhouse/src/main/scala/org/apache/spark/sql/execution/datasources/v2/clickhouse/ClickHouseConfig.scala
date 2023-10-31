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

import org.apache.spark.sql.catalyst.catalog.BucketSpec

import java.util

import scala.collection.JavaConverters.mapAsScalaMapConverter

object ClickHouseConfig {

  // MergeTree DataSource name
  val NAME = "clickhouse"
  val ALT_NAME = "clickhouse"
  val METADATA_DIR = "_delta_log"
  val DEFAULT_ENGINE = "MergeTree"

  // Whether to use MergeTree DataSource V2 API, default is false, fall back to V1.
  val USE_DATASOURCE_V2 = "spark.gluten.sql.columnar.backend.ch.use.v2"
  val DEFAULT_USE_DATASOURCE_V2 = "false"

  val CLICKHOUSE_WORKER_ID = "spark.gluten.sql.columnar.backend.ch.worker.id"

  val CLICKHOUSE_WAREHOUSE_DIR = "spark.gluten.sql.columnar.backend.ch.warehouse.dir"

  /** Create a mergetree configurations and returns the normalized key -> value map. */
  def createMergeTreeConfigurations(
      allProperties: util.Map[String, String],
      buckets: Option[BucketSpec]): Map[String, String] = {
    val configurations = scala.collection.mutable.Map[String, String]()
    allProperties.asScala.foreach(configurations += _)
    if (!configurations.contains("metadata_path")) {
      configurations += ("metadata_path" -> METADATA_DIR)
    }
    if (!configurations.contains("engine")) {
      configurations += ("engine" -> DEFAULT_ENGINE)
    } else {
      val engineValue = configurations.get("engine")
      if (!engineValue.equals(DEFAULT_ENGINE) && !engineValue.equals("parquet")) {
        configurations += ("engine" -> DEFAULT_ENGINE)
      }
    }
    if (!configurations.contains("primary_key")) {
      configurations += ("primary_key" -> "")
    }
    if (!configurations.contains("sampling_key")) {
      configurations += ("sampling_key" -> "")
    }
    if (!configurations.contains("storage_policy")) {
      configurations += ("storage_policy" -> "default")
    }
    if (!configurations.contains("is_distribute")) {
      configurations += ("is_distribute" -> "true")
    }

    if (buckets.isDefined) {
      val bucketSpec = buckets.get
      configurations += ("numBuckets" -> bucketSpec.numBuckets.toString)
      configurations += ("bucketColumnNames" -> bucketSpec.bucketColumnNames.mkString(","))
      configurations += ("sortColumnNames" -> bucketSpec.sortColumnNames.mkString(","))
    }
    configurations.toMap
  }
}
