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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.NativeDvDescriptor
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.util.Utils

object DeltaDvShim {
  def canOffloadDvScan: Boolean = {
    true
  }

  def toNativeDvDescriptors(deltaParquetFileFormat: DeltaParquetFileFormat, partition: FilePartition): Seq[NativeDvDescriptor] = {
    val dvMap = deltaParquetFileFormat.broadcastDvMap.get.value
    val hadoopConf = deltaParquetFileFormat.broadcastHadoopConf.get.value.value
    val tablePath = deltaParquetFileFormat.tablePath.map(new Path(_))
    partition.files.map {
      file =>
        if (!dvMap.contains(file.pathUri)) {
          newEmptyNativeDvDescriptor()
        } else {
          val dvDescriptor = dvMap(file.pathUri)
          if (dvDescriptor.descriptor.isEmpty) {
            newEmptyNativeDvDescriptor()
          } else {
            val ifContainedFlag = dvDescriptor.filterType match {
              case RowIndexFilterType.IF_CONTAINED =>
                true
              case RowIndexFilterType.IF_NOT_CONTAINED =>
                false
              case otherType =>
                throw new GlutenException(s"Unexpected row-index filter type: $otherType")
            }
            require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
            val dvBitmapData = if (dvDescriptor.descriptor.isInline) {
              dvDescriptor.descriptor.inlineData
            } else {
              assert(dvDescriptor.descriptor.isOnDisk)
              val onDiskPath = tablePath.map(dvDescriptor.descriptor.absolutePath)
              val fs = onDiskPath.get.getFileSystem(hadoopConf)
              Utils.tryWithResource(fs.open(onDiskPath.get)) { reader =>
                reader.seek(dvDescriptor.descriptor.offset.getOrElse[Int](0))
                DeletionVectorStore.readRangeFromStream(reader, dvDescriptor.descriptor.sizeInBytes)
              }
            }
            new NativeDvDescriptor(ifContainedFlag, dvBitmapData)
          }
        }
    }
  }

  private def newEmptyNativeDvDescriptor(): NativeDvDescriptor = {
    new NativeDvDescriptor(true, new RoaringBitmapArray().serializeAsByteArray(RoaringBitmapArrayFormat.Portable))
  }
}
