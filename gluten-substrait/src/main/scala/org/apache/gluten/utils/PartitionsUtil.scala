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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PartitionsUtil.regeneratePartition

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import scala.collection.mutable

case class PartitionsUtil(
    relation: HadoopFsRelation,
    requiredSchema: StructType,
    selectedPartitions: Array[PartitionDirectory],
    output: Seq[Attribute],
    bucketedScan: Boolean,
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    disableBucketedScan: Boolean)
  extends Logging {

  def genPartitionSeq(): Seq[Partition] = {
    if (bucketedScan) {
      genBucketedPartitionSeq()
    } else {
      genNonBuckedPartitionSeq()
    }
  }

  private def genNonBuckedPartitionSeq(): Seq[Partition] = {
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

    val inputPartitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    regeneratePartition(inputPartitions, GlutenConfig.get.smallFileThreshold)
  }

  private def genBucketedPartitionSeq(): Seq[Partition] = {
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

object PartitionsUtil {

  /**
   * Regenerate the partitions by balancing the number of files per partition and total size per
   * partition.
   */
  def regeneratePartition(
      inputPartitions: Seq[FilePartition],
      smallFileThreshold: Double): Seq[FilePartition] = {

    // Flatten and sort descending by file size.
    val filesSorted: Seq[(PartitionedFile, Long)] =
      inputPartitions
        .flatMap(_.files)
        .map(f => (f, f.length))
        .sortBy(_._2)(Ordering.Long.reverse)

    val partitions = Array.fill(inputPartitions.size)(mutable.ArrayBuffer.empty[PartitionedFile])

    def addToBucket(
        heap: mutable.PriorityQueue[(Long, Int, Int)],
        file: PartitionedFile,
        sz: Long): Unit = {
      val (load, numFiles, idx) = heap.dequeue()
      partitions(idx) += file
      heap.enqueue((load + sz, numFiles + 1, idx))
    }

    // First by load, then by numFiles.
    val heapByFileSize =
      mutable.PriorityQueue.empty[(Long, Int, Int)](
        Ordering
          .by[(Long, Int, Int), (Long, Int)] {
            case (load, numFiles, _) =>
              (load, numFiles)
          }
          .reverse
      )

    if (smallFileThreshold > 0) {
      val smallFileTotalSize = filesSorted.map(_._2).sum * smallFileThreshold
      // First by numFiles, then by load.
      val heapByFileNum =
        mutable.PriorityQueue.empty[(Long, Int, Int)](
          Ordering
            .by[(Long, Int, Int), (Int, Long)] {
              case (load, numFiles, _) =>
                (numFiles, load)
            }
            .reverse
        )

      inputPartitions.indices.foreach(i => heapByFileNum.enqueue((0L, 0, i)))

      var numSmallFiles = 0
      var smallFileSize = 0L
      // Enqueue small files to the least number of files and the least load.
      filesSorted.reverse.takeWhile(f => f._2 + smallFileSize <= smallFileTotalSize).foreach {
        case (file, sz) =>
          addToBucket(heapByFileNum, file, sz)
          numSmallFiles += 1
          smallFileSize += sz
      }

      // Move buckets from heapByFileNum to heapByFileSize.
      while (heapByFileNum.nonEmpty) {
        heapByFileSize.enqueue(heapByFileNum.dequeue())
      }

      // Finally, enqueue remaining files.
      filesSorted.take(filesSorted.size - numSmallFiles).foreach {
        case (file, sz) =>
          addToBucket(heapByFileSize, file, sz)
      }
    } else {
      inputPartitions.indices.foreach(i => heapByFileSize.enqueue((0L, 0, i)))

      filesSorted.foreach {
        case (file, sz) =>
          addToBucket(heapByFileSize, file, sz)
      }
    }

    partitions.zipWithIndex.map { case (p, idx) => FilePartition(idx, p.toArray) }
  }
}
