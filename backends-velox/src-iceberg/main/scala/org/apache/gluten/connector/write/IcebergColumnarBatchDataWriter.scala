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
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.google.common.collect.ImmutableMap
import org.apache.iceberg._
import org.apache.iceberg.spark.source.IcebergWriteUtil

case class IcebergColumnarBatchDataWriter(
    writer: Long,
    jniWrapper: IcebergWriteJniWrapper,
    format: Int,
    partitionSpec: PartitionSpec)
  extends DataWriter[ColumnarBatch]
  with Logging {

  private val mapper = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  override def write(batch: ColumnarBatch): Unit = {
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    jniWrapper.write(writer, batchHandle)
  }

  override def commit: WriterCommitMessage = {
    val dataFiles = jniWrapper.commit(writer).map(d => parseDataFile(d, partitionSpec))
    IcebergWriteUtil.commitDataFiles(dataFiles)
  }

  override def abort(): Unit = {
    logInfo("Abort the ColumnarBatchDataWriter")
  }

  override def close(): Unit = {
    logDebug("Close the ColumnarBatchDataWriter")
  }

  private def parseDataFile(json: String, spec: PartitionSpec): DataFile = {
    val dataFile = mapper.readValue(json, classOf[DataFileJson])
    // TODO: add partition
    val metrics = new Metrics(
      dataFile.metrics.recordCount,
      ImmutableMap.of(),
      ImmutableMap.of(),
      ImmutableMap.of(),
      ImmutableMap.of())

    val builder = DataFiles
      .builder(spec)
      .withPath(dataFile.path)
      .withFormat(getFileFormat)
      .withFileSizeInBytes(dataFile.fileSizeInBytes)
      .withPartition(PartitionDataJson.fromJson(dataFile.partitionDataJson, partitionSpec))
      .withMetrics(metrics)
    builder.build()
  }

  private def getFileFormat: FileFormat = {
    format match {
      case 0 => FileFormat.ORC
      case 1 => FileFormat.PARQUET
      case _ => throw new UnsupportedOperationException()
    }
  }
}
