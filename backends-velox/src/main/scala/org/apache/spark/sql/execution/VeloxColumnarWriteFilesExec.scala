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

import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.execution.WriteFilesExecTransformer
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, GenericInternalRow}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, ExecutedWriteSummary, FileFormat, PartitioningUtils, WriteFilesExec, WriteFilesSpec, WriteTaskResult}

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

class VeloxColumnarWriteFilesExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends WriteFilesExec(child, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)
  with GlutenPlan {

  override lazy val references: AttributeSet = AttributeSet.empty

  override def supportsColumnar(): Boolean = true

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    assert(child.supportsColumnar)

    // We need to pass the WritePath to the Velox TableWriter in the doTransform
    // method of the WriteTransformer. However, the WritePath is not accessible
    // during the planning phase in the WriteTransformer, and can only be obtained
    // during the actual execution, specifically in the doExecuteWrite method of
    // ColumnarWriteFilesExec, where it is available within the WriteFilesSpec.
    // Therefore, we use this hack method to pass the writePath.
    WriteFilesExecTransformer.withWriteFilePath(writeFilesSpec.description.path) {
      child.executeColumnar().mapPartitionsInternal {
        iter =>
          // Currently, the cb contains three columns: row, fragments, and context.
          // The first row in the row column contains the number of written numRows.
          // The fragments column contains detailed information about the file writes.
          assert(iter.hasNext)
          val cb = iter.next()
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
            val outputPath = writeFilesSpec.description.path

            // part1=1/part2=1
            val partitionFragment = metrics.name
            // write a non-partitioned table
            if (partitionFragment != "") {
              updatedPartitions += partitionFragment
              val tmpOutputPath = outputPath + "/" + partitionFragment + "/" + targetFileName
              val customOutputPath = writeFilesSpec.description.customPartitionLocations.get(
                PartitioningUtils.parsePathFragment(partitionFragment))
              if (customOutputPath.isDefined) {
                addedAbsPathFiles(tmpOutputPath) = customOutputPath.get + "/" + targetFileName
              }
            }
          }

          // Reports bytesWritten and recordsWritten to the Spark output metrics.
          Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach {
            outputMetrics =>
              outputMetrics.setBytesWritten(numBytes)
              outputMetrics.setRecordsWritten(numWrittenRows)
          }

          val partitionsInternalRows = updatedPartitions.map {
            part =>
              val parts = new Array[Any](1)
              parts(0) = part
              new GenericInternalRow(parts)
          }.toSeq
          val stats = BasicWriteTaskStats(
            partitions = partitionsInternalRows,
            numFiles = loadedCb.numRows() - 1,
            numBytes = numBytes,
            numRows = numWrittenRows)
          val summary =
            ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

          val result = WriteTaskResult(
            new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
            summary)
          Iterator.single(result)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteFilesExec =
    new VeloxColumnarWriteFilesExec(
      newChild,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
}
