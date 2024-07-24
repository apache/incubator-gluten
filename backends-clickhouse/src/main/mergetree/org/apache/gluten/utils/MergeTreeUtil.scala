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
package org.apache.gluten.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.{ClickhouseSnapshot, DeltaLog, DeltaLogFileIndex}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v1.{CHMergeTreeWriterInjects, GlutenMergeTreeWriterInjects}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.util.collection.BitSet

object MergeTreeUtil {
  def checkMergeTreeFileFormat(relation: HadoopFsRelation): Boolean = {
    relation.location.isInstanceOf[TahoeFileIndex] &&
      relation.fileFormat.isInstanceOf[DeltaMergeTreeFileFormat]
  }

  def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = {
    scanExec.relation.location.isInstanceOf[DeltaLogFileIndex]
  }

  def ifMergeTree(relation: HadoopFsRelation): Boolean = {
    relation.location match {
      case _: TahoeFileIndex
        if relation.fileFormat
          .isInstanceOf[DeltaMergeTreeFileFormat] => true
      case _ => false
    }
  }

  def partsPartitions( relation: HadoopFsRelation,
                       selectedPartitions: Array[PartitionDirectory],
                       output: Seq[Attribute],
                       bucketedScan: Boolean,
                       optionalBucketSet: Option[BitSet],
                       optionalNumCoalescedBuckets: Option[Int],
                       disableBucketedScan: Boolean,
                       filterExprs: Seq[Expression]): Seq[InputPartition] = {
    ClickHouseTableV2
      .partsPartitions(
        relation.location.asInstanceOf[TahoeFileIndex].deltaLog,
        relation,
        selectedPartitions,
        output,
        bucketedScan,
        optionalBucketSet,
        optionalNumCoalescedBuckets,
        disableBucketedScan,
        filterExprs
      )
  }

  def injectMergeTreeWriter(): Unit = {
    GlutenMergeTreeWriterInjects.setInstance(new CHMergeTreeWriterInjects())
  }

  def cleanup(): Unit = {
    ClickhouseSnapshot.clearAllFileStatusCache
    DeltaLog.clearCache()
  }
}
