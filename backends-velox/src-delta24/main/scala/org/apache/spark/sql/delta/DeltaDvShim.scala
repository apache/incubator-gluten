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

package org.apache.spark.sql.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.util.Utils

object DeltaDvShim {
  def canOffloadDvScan: Boolean = {
    true
  }

  def getIfContainedFlags(deltaParquetFileFormat: DeltaParquetFileFormat, partition: FilePartition): Seq[Boolean] = {
    partition.files.map(
      file =>
        deltaParquetFileFormat.broadcastDvMap.get.value(file.pathUri).filterType == RowIndexFilterType.IF_CONTAINED
    ).toSeq
  }

  def readSerializedDvBitmap(deltaParquetFileFormat: DeltaParquetFileFormat, partition: FilePartition): Seq[Array[Byte]] = {
    val hadoopConf = deltaParquetFileFormat.broadcastHadoopConf.get.value.value
    val tablePath = deltaParquetFileFormat.tablePath.map(new Path(_))
    partition.files.map(
      file => {
        val dvDescriptor = deltaParquetFileFormat.broadcastDvMap.get.value(file.pathUri).descriptor
        if (dvDescriptor.isEmpty) {
          new RoaringBitmapArray().serializeAsByteArray(RoaringBitmapArrayFormat.Portable);
        } else {
          require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
          if (dvDescriptor.isInline) {
            dvDescriptor.inlineData
          } else {
            assert(dvDescriptor.isOnDisk)
            val onDiskPath = tablePath.map(dvDescriptor.absolutePath)
            val fs = onDiskPath.get.getFileSystem(hadoopConf)
            val buffer = Utils.tryWithResource(fs.open(onDiskPath.get)) { reader =>
              reader.seek(dvDescriptor.offset.getOrElse[Int](0))
              DeletionVectorStore.readRangeFromStream(reader, dvDescriptor.sizeInBytes)
            }
            buffer
          }
        }
      }
    ).toSeq
  }
}
