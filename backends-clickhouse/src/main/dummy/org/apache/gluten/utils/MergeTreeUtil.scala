package org.apache.gluten.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.util.collection.BitSet

object MergeTreeUtil {
  def checkMergeTreeFileFormat(relation: HadoopFsRelation): Boolean = false

  def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = false

  def ifMergeTree(relation: HadoopFsRelation): Boolean = false

  def partsPartitions(relation: HadoopFsRelation,
                      selectedPartitions: Array[PartitionDirectory],
                      output: Seq[Attribute],
                      bucketedScan: Boolean,
                      optionalBucketSet: Option[BitSet],
                      optionalNumCoalescedBuckets: Option[Int],
                      disableBucketedScan: Boolean,
                      filterExprs: Seq[Expression]): Seq[InputPartition] = Nil

  def injectMergeTreeWriter(): Unit = {}

  def cleanup(): Unit = {}

}
