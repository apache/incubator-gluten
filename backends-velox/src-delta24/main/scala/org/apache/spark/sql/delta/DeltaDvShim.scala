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

import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{DeletionVectorInfo, EmptyDeletionVectorInfo, RegularDeletionVectorInfo}
import org.apache.spark.sql.execution.datasources.FilePartition

object DeltaDvShim {
  def canOffloadDvScan: Boolean = {
    true
  }

  def toDvInfos(
      deltaParquetFileFormat: DeltaParquetFileFormat,
      partition: FilePartition): Seq[DeletionVectorInfo] = {
    val dvMap = deltaParquetFileFormat.broadcastDvMap.get.value
    val hadoopConf = deltaParquetFileFormat.broadcastHadoopConf.get.value
    val tablePath = deltaParquetFileFormat.tablePath
    partition.files.map {
      file =>
        val filePathUri = file.pathUri
        if (!dvMap.contains(filePathUri)) {
          new EmptyDeletionVectorInfo()
        } else {
          val dvDescriptor = dvMap(filePathUri)
          if (dvDescriptor.descriptor.isEmpty) {
            new EmptyDeletionVectorInfo()
          } else {
            require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
            new RegularDeletionVectorInfo(
              dvDescriptor.descriptor,
              dvDescriptor.filterType,
              hadoopConf,
              tablePath.get)
          }
        }
    }.toSeq
  }
}
