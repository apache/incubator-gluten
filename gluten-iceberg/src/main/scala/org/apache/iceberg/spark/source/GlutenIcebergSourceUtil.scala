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
package org.apache.iceberg.spark.source

import org.apache.gluten.substrait.rel.{IcebergLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.types.StructType

import org.apache.iceberg.{CombinedScanTask, DeleteFile, FileFormat, FileScanTask, ScanTask}

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object GlutenIcebergSourceUtil {

  def genSplitInfo(inputPartition: InputPartition, index: Int): SplitInfo = inputPartition match {
    case partition: SparkInputPartition =>
      val paths = new JArrayList[String]()
      val starts = new JArrayList[JLong]()
      val lengths = new JArrayList[JLong]()
      val partitionColumns = new JArrayList[JMap[String, String]]()
      val deleteFilesList = new JArrayList[JList[DeleteFile]]()
      var fileFormat = ReadFileFormat.UnknownFormat

      val tasks = partition.taskGroup[ScanTask]().tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          val filePath = task.file().path().toString
          paths.add(filePath)
          starts.add(task.start())
          lengths.add(task.length())
          partitionColumns.add(getPartitionColumns(task))
          deleteFilesList.add(task.deletes());
          val currentFileFormat = convertFileFormat(task.file().format())
          if (fileFormat == ReadFileFormat.UnknownFormat) {
            fileFormat = currentFileFormat
          } else if (fileFormat != currentFileFormat) {
            throw new UnsupportedOperationException(
              s"Only one file format is supported, " +
                s"find different file format $fileFormat and $currentFileFormat")
          }
      }
      val preferredLoc = SoftAffinity.getFilePartitionLocations(
        paths.asScala.toArray,
        inputPartition.preferredLocations())
      IcebergLocalFilesBuilder.makeIcebergLocalFiles(
        index,
        paths,
        starts,
        lengths,
        partitionColumns,
        fileFormat,
        preferredLoc.toList.asJava,
        deleteFilesList
      )
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkInputPartition.")
  }

  def getFileFormat(sparkScan: Scan): ReadFileFormat = sparkScan match {
    case scan: SparkBatchQueryScan =>
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          task.file().format() match {
            case FileFormat.PARQUET => return ReadFileFormat.ParquetReadFormat
            case FileFormat.ORC => return ReadFileFormat.OrcReadFormat
            case _ =>
          }
      }
      throw new UnsupportedOperationException("Iceberg Only support parquet and orc file format.")
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
  }

  def getPartitionSchema(sparkScan: Scan): StructType = sparkScan match {
    case scan: SparkBatchQueryScan =>
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          val spec = task.spec()
          if (spec.isPartitioned) {
            var partitionSchema = new StructType()
            val partitionFields = spec.partitionType().fields().asScala
            partitionFields.foreach {
              field =>
                TypeUtil.validatePartitionColumnType(field.`type`().typeId())
                partitionSchema = partitionSchema.add(field.name(), field.`type`().toString)
            }
            return partitionSchema
          } else {
            return new StructType()
          }
      }
      throw new UnsupportedOperationException(
        "Failed to get partition schema from iceberg SparkBatchQueryScan.")
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
  }

  private def asFileScanTask(tasks: List[ScanTask]): List[FileScanTask] = {
    if (tasks.forall(_.isFileScanTask)) {
      tasks.map(_.asFileScanTask())
    } else if (tasks.forall(_.isInstanceOf[CombinedScanTask])) {
      tasks.flatMap(_.asCombinedScanTask().tasks().asScala)
    } else {
      throw new UnsupportedOperationException(
        "Only support iceberg CombinedScanTask and FileScanTask.")
    }
  }

  private def getPartitionColumns(task: FileScanTask): JHashMap[String, String] = {
    val partitionColumns = new JHashMap[String, String]()
    val spec = task.spec()
    val partition = task.partition()
    if (spec.isPartitioned) {
      val partitionFields = spec.partitionType().fields().asScala
      partitionFields.zipWithIndex.foreach {
        case (field, index) =>
          val partitionValue = partition.get(index, field.`type`().typeId().javaClass())
          val partitionType = field.`type`()
          if (partitionValue != null) {
            partitionColumns.put(
              field.name(),
              TypeUtil.getPartitionValueString(partitionType, partitionValue))
          } else {
            partitionColumns.put(field.name(), ExternalCatalogUtils.DEFAULT_PARTITION_NAME)
          }
      }
    }
    partitionColumns
  }

  def convertFileFormat(icebergFileFormat: FileFormat): ReadFileFormat =
    icebergFileFormat match {
      case FileFormat.PARQUET => ReadFileFormat.ParquetReadFormat
      case FileFormat.ORC => ReadFileFormat.OrcReadFormat
      case _ =>
        throw new UnsupportedOperationException("Iceberg Only support parquet and orc file format.")
    }
}
