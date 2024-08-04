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
package org.apache.spark.sql.execution

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable

class ClickhouseBackendWrite(
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    jobTrackerID: String)
  extends SparkHadoopMapReduceCommitProtocol(description, committer, jobTrackerID) {

  override def doCollectNativeResult(cb: ColumnarBatch): Option[WriteTaskResult] = {
    val numFiles = cb.numRows()
    // Write an empty iterator
    if (numFiles == 0) {
      None
    } else {
      val file_col = cb.column(0)
      val partition_col = cb.column(1)
      val count_col = cb.column(2)

      val outputPath = description.path
      var updatedPartitions = Set.empty[String]
      val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()

      val write_stats = Range(0, cb.numRows()).map {
        i =>
          val targetFileName = file_col.getUTF8String(i).toString
          val partition = partition_col.getUTF8String(i).toString
          if (partition != "__NO_PARTITION_ID__") {
            updatedPartitions += partition
            val tmpOutputPath = outputPath + "/" + partition + "/" + targetFileName
            val customOutputPath =
              description.customPartitionLocations.get(
                PartitioningUtils.parsePathFragment(partition))
            if (customOutputPath.isDefined) {
              addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
            }
          }
          count_col.getLong(i)
      }

      val partitionsInternalRows = updatedPartitions.map {
        part =>
          val parts = new Array[Any](1)
          parts(0) = part
          new GenericInternalRow(parts)
      }.toSeq

      val numWrittenRows = write_stats.sum
      val stats = BasicWriteTaskStats(
        partitions = partitionsInternalRows,
        numFiles = numFiles,
        numBytes = 101,
        numRows = numWrittenRows)
      val summary =
        ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
          summary))
    }
  }
}
