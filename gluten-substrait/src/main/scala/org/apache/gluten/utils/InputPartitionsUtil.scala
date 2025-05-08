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

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

case class InputPartitionsUtil(
    relation: HadoopFsRelation,
    requiredSchema: StructType,
    selectedPartitions: Array[PartitionDirectory],
    output: Seq[Attribute],
    bucketedScan: Boolean,
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    disableBucketedScan: Boolean)
  extends Logging {

  def genInputPartitionSeq(): Seq[InputPartition] = {
    if (bucketedScan) {
      genBucketedInputPartitionSeq()
    } else {
      genNonBuckedInputPartitionSeq()
    }
  }

  private def genNonBuckedInputPartitionSeq(): Seq[InputPartition] = {
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        filePath => {
          BucketingUtils.getBucketId(filePath.getName) match {
            case Some(id) => bucketSet.get(id)
            case None =>
              // Do not prune the file if bucket file name is invalid
              true
          }
        }
      case _ =>
        _ => true
    }

    val splitFiles = selectedPartitions
      .flatMap {
        partition =>
          SparkShimLoader.getSparkShims.getFileStatus(partition).flatMap {
            file =>
              // getPath() is very expensive so we only want to call it once in this block:
              val filePath = file._1.getPath
              if (shouldProcess(filePath)) {
                val isSplitable =
                  SparkShimLoader.getSparkShims.isFileSplittable(relation, filePath, requiredSchema)
                SparkShimLoader.getSparkShims.splitFiles(
                  sparkSession = relation.sparkSession,
                  file = file._1,
                  filePath = filePath,
                  isSplitable = isSplitable,
                  maxSplitBytes = maxSplitBytes,
                  partitionValues = partition.values,
                  metadata = file._2
                )
              } else {
                Seq.empty
              }
          }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
  }

  private def genBucketedInputPartitionSeq(): Seq[InputPartition] = {
    val bucketSpec = relation.bucketSpec.get
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      SparkShimLoader.getSparkShims.filesGroupedToBuckets(selectedPartitions)

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter(f => bucketSet.get(f._1))
    } else {
      filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
      .map {
        numCoalescedBuckets =>
          logInfo(s"Coalescing to $numCoalescedBuckets buckets")
          val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
          Seq.tabulate(numCoalescedBuckets) {
            bucketId =>
              val partitionedFiles = coalescedBuckets
                .get(bucketId)
                .map {
                  _.values.flatten.toArray
                }
                .getOrElse(Array.empty)
              FilePartition(bucketId, partitionedFiles)
          }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) {
          bucketId =>
            FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }
  }

  private def toAttribute(colName: String): Option[Attribute] = {
    output.find(_.name == colName)
  }
}
