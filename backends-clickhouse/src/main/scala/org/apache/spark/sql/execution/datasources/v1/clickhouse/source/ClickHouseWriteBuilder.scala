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
package org.apache.spark.sql.execution.datasources.v1.clickhouse.source

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v1.clickhouse.commands.WriteMergeTreeToDelta
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

class ClickHouseWriteBuilder(
    spark: SparkSession,
    table: ClickHouseTableV2,
    deltaLog: DeltaLog,
    info: LogicalWriteInfo)
  extends WriteBuilder
  with SupportsOverwrite
  with SupportsTruncate
  with SupportsDynamicOverwrite {

  private var forceOverwrite = false

  private val writeOptions = info.options()

  lazy val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw DeltaErrors.replaceWhereUsedInOverwrite()
    }
    options.put("replaceWhere", DeltaSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    options.put(
      DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION,
      DeltaOptions.PARTITION_OVERWRITE_MODE_DYNAMIC)
    forceOverwrite = true
    this
  }

  def querySchema: StructType = info.schema()

  def queryId: String = info.queryId()

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession

          // TODO: Get the config from WriteIntoDelta's txn.
          WriteMergeTreeToDelta(
            deltaLog,
            if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(options.toMap, session.sessionState.conf),
            options.toMap, // the options in DeltaOptions is protected
            session.sessionState.conf,
            table.dataBaseName,
            table.tableName,
            table.orderByKeyOption,
            table.primaryKeyOption,
            table.clickhouseTableConfigs,
            table.partitionColumns,
            table.bucketOption,
            data,
            info
          ).run(session)

          table.refresh()
          // TODO: Push this to Apache Spark
          // Re-cache all cached plans(including this relation itself, if it's cached) that refer
          // to this data source relation. This is the behavior for InsertInto
          session.sharedState.cacheManager
            .recacheByPlan(session, LogicalRelation(deltaLog.createRelation()))
        }
      }
    }
  }
}
