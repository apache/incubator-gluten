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
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators

import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, ExecutedWriteSummary, FileFormat, PartitioningUtils, WriteFilesExec, WriteFilesSpec, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch

import shaded.parquet.com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable

class VeloxColumnarWriteFilesExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends WriteFilesExec(
    child,
    fileFormat,
    partitionColumns,
    bucketSpec,
    options,
    staticPartitions) {

  override def supportsColumnar(): Boolean = true

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    assert(child.supportsColumnar)
    child.session.sparkContext.setLocalProperty("writePath", writeFilesSpec.description.path)
    child.executeColumnar().map {
      cb =>
        // Currently, the cb contains three columns: row, fragments, and context.
        //  The first row in the row column contains the number of written numRows.
        // The fragments column contains detailed information about the file writes.
        // The detailed fragement is https://github.com/facebookincubator/velox/blob/
        // 6b17ea5100a2713a6ee0252a37ce47cb17f46929/velox/connectors/hive/HiveDataSink.cpp#L508.
        val loadedCb = ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance, cb)

        val numRows = loadedCb.column(0).getLong(0)

        var updatedPartitions = Set.empty[String]
        val addedAbsPathFiles: mutable.Map[String, String] = mutable.Map[String, String]()
        var numBytes = 0L
        for (i <- 0 until loadedCb.numRows() - 1) {
          val fragments = loadedCb.column(1).getUTF8String(i + 1)
          val objectMapper = new ObjectMapper()
          val jsonObject = objectMapper.readTree(fragments.toString)

          val fileWriteInfos = jsonObject.get("fileWriteInfos").elements()
          if (jsonObject.get("fileWriteInfos").elements().hasNext) {
            val writeInfo = fileWriteInfos.next();
            numBytes += writeInfo.get("fileSize").size()
            // Get partition informations.
            if (jsonObject.get("name").textValue().nonEmpty) {
              val targetFileName = writeInfo.get("targetFileName").textValue()
              val partitionDir = jsonObject.get("name").textValue()
              updatedPartitions += partitionDir
              val tmpOutputPath =
                writeFilesSpec.description.path + "/" + partitionDir + "/" + targetFileName
              val absOutputPathObject =
                writeFilesSpec.description.customPartitionLocations.get(
                  PartitioningUtils.parsePathFragment(partitionDir))
              if (absOutputPathObject.nonEmpty) {
                val absOutputPath = absOutputPathObject.get + "/" + targetFileName
                addedAbsPathFiles(tmpOutputPath) = absOutputPath
              }
            }
          }
        }

        // TODO: need to get the partition Internal row?
        val stats = BasicWriteTaskStats(Seq.empty, (numRows - 1).toInt, numBytes, numRows)
        val summary =
          ExecutedWriteSummary(updatedPartitions = updatedPartitions, stats = Seq(stats))

        WriteTaskResult(
          new TaskCommitMessage(addedAbsPathFiles.toMap -> updatedPartitions),
          summary)
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
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
