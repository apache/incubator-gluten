/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.clickhouse.source

import java.util.OptionalLong

import scala.collection.mutable.ArrayBuffer

import io.glutenproject.execution.NativeMergeTreePartition
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ClickHouseScan(
                           sparkSession: SparkSession,
                           @transient table: ClickHouseTableV2,
                           dataSchema: StructType,
                           readDataSchema: StructType,
                           pushedFilters: Array[Filter],
                           options: CaseInsensitiveStringMap,
                           partitionFilters: Seq[Expression] = Seq.empty,
                           dataFilters: Seq[Expression] = Seq.empty) extends FileScan {

  override def isSplitable(path: Path): Boolean = false

  override def readPartitionSchema: StructType = new StructType()

  override def fileIndex: PartitioningAwareFileIndex = null

  override def withFilters(partitionFilters: Seq[Expression],
                           dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def toBatch: Batch = this

  protected def getSnapshot(): Snapshot = table.updateSnapshot()

  def getPartsPartitions(
                          sparkSession: SparkSession,
                          partsFiles: Seq[AddMergeTreeParts],
                          maxSplitBytes: Long): Seq[InputPartition] = {
    val partitions = new ArrayBuffer[InputPartition]
    val database = table.catalogTable.get.identifier.database.get
    val tableName = table.catalogTable.get.identifier.table
    val engine = table.snapshot.metadata.configuration.get("engine").get
    val tablePath = table.deltaLog.dataPath.toString.substring(6)
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = NativeMergeTreePartition(partitions.size, engine, database, tableName,
          tablePath, currentMinPartsNum, currentMaxPartsNum + 1)
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // Assign files to partitions using "Next Fit Decreasing"
    partsFiles.foreach { parts =>
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
    closePartition()
    partitions.toSeq
  }

  protected def partsPartitions: Seq[InputPartition] = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val allParts = table.listFiles()
    getPartsPartitions(sparkSession, allParts, defaultMaxSplitBytes)
  }

  override def planInputPartitions(): Array[InputPartition] = partsPartitions.toArray

  override def createReaderFactory(): PartitionReaderFactory = {
    new ClickHousePartitionReaderFactory()
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = OptionalLong.empty()

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def getMetaData(): Map[String, String] = {
    Map.empty[String, String]
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: ClickHouseScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()
}
