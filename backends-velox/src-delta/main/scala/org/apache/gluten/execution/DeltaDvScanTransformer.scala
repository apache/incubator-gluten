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
package org.apache.gluten.execution

import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.{DeltaLocalFilesNode, LocalFilesNode, SplitInfo}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.{DeltaDvShim, DeltaParquetFileFormat}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConverters._

case class DeltaDvScanTransformer(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanExecTransformerBase(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan
  ) {
  // FIXME: The file format should be Delta. Class inheritance seems to be abused
  //  and the design is not clean enough so we are mixing the code of legacy
  //  formats and lake formats in a dirty way.
  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  override def getSplitInfosFromPartitions(partitions: Seq[InputPartition]): Seq[SplitInfo] = {
    val parquetSplitInfos = super.getSplitInfosFromPartitions(partitions)
    partitions.zip(parquetSplitInfos).map {
      case (p: InputPartition, l: LocalFilesNode) =>
        val dvInfos = DeltaDvShim.toDvInfos(relation.fileFormat.asInstanceOf[DeltaParquetFileFormat], p.asInstanceOf[FilePartition])
        // Adds a constant is_row_deleted column indicating all rows that are returned by the scan should be kept.
        // This is because we have already pushed the DV filter into the Velox scan.
        val metadataColumnsWithIsRowDeleted = l.getMetadataColumns.asScala.map(m => (m.asScala + ("__delta_internal_is_row_deleted" -> "0")).asJava).asJava
        new DeltaLocalFilesNode(
          l.getIndex,
          l.getPaths,
          l.getStarts,
          l.getLengths,
          l.getFileSizes,
          l.getModificationTimes,
          l.getPartitionColumns,
          metadataColumnsWithIsRowDeleted,
          l.getFileFormat,
          l.preferredLocations,
          l.getFileReadProperties,
          l.getOtherMetadataColumns,
          dvInfos.asJava
        )
    }
  }

  override def doCanonicalize(): DeltaDvScanTransformer = {
    DeltaDvScanTransformer(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan
    )
  }
}

object DeltaDvScanTransformer {
  def apply(scanExec: FileSourceScanExec): DeltaDvScanTransformer = {
    new DeltaDvScanTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }
}
