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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Cast, Expression, GenericInternalRow, Literal, Predicate}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.utils.MergeTreePartsPartitionsUtil
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.{FileStatus, Path}

import java.util.Objects

abstract class ClickHouseFileIndexBase(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    table: ClickHouseTableV2,
    snapshotAtAnalysis: Snapshot,
    partitionFilters: Seq[Expression] = Nil,
    isTimeTravelQuery: Boolean = false)
  extends TahoeFileIndex(spark, deltaLog, path) {

  override val sizeInBytes: Long = table.listFiles().map(_.bytesOnDisk).sum

  def getSnapshot: Snapshot = {
    getSnapshotToScan
  }

  protected def getSnapshotToScan: Snapshot = {
    if (isTimeTravelQuery) snapshotAtAnalysis else deltaLog.update(stalenessAcceptable = true)
  }

  override def inputFiles: Array[String] = {
    table.listFiles().map(_.path).toArray
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    val partitionColumns = partitionSchema
    val allParts = table
      .listFiles()
      .map(
        parts => {
          val rowValues: Array[Any] = partitionColumns.map {
            p =>
              val colName = DeltaColumnMapping.getPhysicalName(p)
              val partValue = Literal(parts.partitionValues.get(colName).orNull)
              Cast(partValue, p.dataType, Option(timeZone), ansiEnabled = false).eval()
          }.toArray
          val fileStats = new FileStatus(
            /* length */ parts.bytesOnDisk,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ parts.modificationTime,
            new Path(parts.name)
          )
          PartitionDirectory(new GenericInternalRow(rowValues), Seq(fileStats))
        })

    // partition filters
    val ret = if (partitionFilters.nonEmpty) {
      val predicate = partitionFilters.reduce(And)

      val boundPredicate = Predicate.create(
        predicate.transform {
          case a: AttributeReference =>
            val index = partitionColumns.indexWhere(a.name == _.name)
            BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil
      )
      allParts.filter(p => boundPredicate.eval(p.values))
    } else {
      allParts
    }
    ret
  }

  def partsPartitions(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] =
    MergeTreePartsPartitionsUtil.getMergeTreePartsPartitions(
      relation,
      selectedPartitions,
      output,
      bucketedScan,
      spark,
      table,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)

  override def refresh(): Unit = {}

  override def equals(that: Any): Boolean = that match {
    case t: ClickHouseFileIndex =>
      t.path == path && t.deltaLog.isSameLogAs(deltaLog) &&
      t.versionToUse == versionToUse && t.partitionFilters == partitionFilters
    case _ => false
  }

  /** Provides the version that's being used as part of the scan if this is a time travel query. */
  def versionToUse: Option[Long] =
    if (isTimeTravelQuery) Some(snapshotAtAnalysis.version) else None

  override def hashCode: scala.Int = {
    Objects.hashCode(path, deltaLog.tableId -> deltaLog.dataPath, versionToUse, partitionFilters)
  }

  override def partitionSchema: StructType = snapshotAtAnalysis.metadata.partitionSchema
}
