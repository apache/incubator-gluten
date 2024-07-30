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

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable

// Velox write files metrics start
//
// Follows the code in velox `HiveDataSink::close()`
// The json can be as following:
// {
//     "inMemoryDataSizeInBytes":0,
//     "containsNumberedFileNames":true,
//     "onDiskDataSizeInBytes":307,
//     "fileWriteInfos":[
//         {
//             "fileSize":307,
//             "writeFileName":
//                "Gluten_Stage_1_TID_2_0_2_d1db3b31-4f99-41cb-a4e7-3b8607506168.parquet",
//             "targetFileName":
//                "Gluten_Stage_1_TID_2_0_2_d1db3b31-4f99-41cb-a4e7-3b8607506168.parquet"
//         }
//     ],
//     "writePath":"file:/home/gluten/spark-warehouse/inserttable/part1=1/part2=1",
//     "rowCount":1,
//     "targetPath":"file:/home/gluten/spark-warehouse/inserttable/part1=1/part2=1",
//     "updateMode":"NEW",
//     "name":"part1=1/part2=1"
// }
case class VeloxWriteFilesInfo(writeFileName: String, targetFileName: String, fileSize: Long)

case class VeloxWriteFilesMetrics(
    name: String,
    updateMode: String,
    writePath: String,
    targetPath: String,
    fileWriteInfos: Seq[VeloxWriteFilesInfo],
    rowCount: Long,
    inMemoryDataSizeInBytes: Long,
    onDiskDataSizeInBytes: Long,
    containsNumberedFileNames: Boolean)

// Velox write files metrics end

case class VeloxBackendWrite(description: WriteJobDescription) extends BackendWrite with Logging {

  override def collectNativeWriteFilesMetrics(cb: ColumnarBatch): Option[WriteTaskResult] = {
    // Currently, the cb contains three columns: row, fragments, and context.
    // The first row in the row column contains the number of written numRows.
    // The fragments column contains detailed information about the file writes.
    val loadedCb = ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance, cb)
    assert(loadedCb.numCols() == 3)
    val numWrittenRows = loadedCb.column(0).getLong(0)

    var updatedPartitions = Set.empty[String]
    val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()
    var numBytes = 0L
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    for (i <- 0 until loadedCb.numRows() - 1) {
      val fragments = loadedCb.column(1).getUTF8String(i + 1)
      val metrics = objectMapper
        .readValue(fragments.toString.getBytes("UTF-8"), classOf[VeloxWriteFilesMetrics])
      logDebug(s"Velox write files metrics: $metrics")

      val fileWriteInfos = metrics.fileWriteInfos
      assert(fileWriteInfos.length == 1)
      val fileWriteInfo = fileWriteInfos.head
      numBytes += fileWriteInfo.fileSize
      val targetFileName = fileWriteInfo.targetFileName
      val outputPath = description.path

      // part1=1/part2=1
      val partitionFragment = metrics.name
      // Write a partitioned table
      if (partitionFragment != "") {
        updatedPartitions += partitionFragment
        val tmpOutputPath = outputPath + "/" + partitionFragment + "/" + targetFileName
        val customOutputPath = description.customPartitionLocations.get(
          PartitioningUtils.parsePathFragment(partitionFragment))
        if (customOutputPath.isDefined) {
          addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
        }
      }
    }

    val numFiles = loadedCb.numRows() - 1
    val partitionsInternalRows = updatedPartitions.map {
      part =>
        val parts = new Array[Any](1)
        parts(0) = part
        new GenericInternalRow(parts)
    }.toSeq
    val stats = BasicWriteTaskStats(
      partitions = partitionsInternalRows,
      numFiles = numFiles,
      numBytes = numBytes,
      numRows = numWrittenRows)
    val summary =
      ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

    // Write an empty iterator
    if (numFiles == 0) {
      None
    } else {
      Some(
        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
          summary))
    }
  }
}
