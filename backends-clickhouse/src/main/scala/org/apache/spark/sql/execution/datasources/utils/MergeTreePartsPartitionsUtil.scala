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
package org.apache.spark.sql.execution.datasources.utils

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.execution.GlutenMergeTreePartition
import io.glutenproject.utils.SparkResourcesUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2

import scala.collection.mutable.ArrayBuffer

object MergeTreePartsPartitionsUtil extends Logging {

  def getPartsPartitions(
      sparkSession: SparkSession,
      table: ClickHouseTableV2): Seq[InputPartition] = {
    val partsFiles = table.listFiles()

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = table.snapshot.metadata.configuration.get("engine").get
    // TODO: remove `substring`
    val tablePath = table.deltaLog.dataPath.toString.substring(6)
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          currentMinPartsNum,
          currentMaxPartsNum + 1)
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
      currentFileCnt = 0L
    }

    val totalCores = SparkResourcesUtil.getTotalCores(sparkSession.sessionState.conf)
    val fileCntPerPartition = math.ceil((partsFiles.size * 1.0) / totalCores).toInt
    val fileCntThreshold = sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    if (fileCntThreshold > 0 && fileCntPerPartition > fileCntThreshold) {
      // generate `Seq[InputPartition]` by file count
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentFileCnt >= fileCntPerPartition) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentFileCnt += 1
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    } else {
      // generate `Seq[InputPartition]` by file size
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      logInfo(
        s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
          s"open cost is considered as scanning $openCostInBytes bytes.")
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentSize += parts.bytesOnDisk + openCostInBytes
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    }
    closePartition()
    partitions
  }
}
