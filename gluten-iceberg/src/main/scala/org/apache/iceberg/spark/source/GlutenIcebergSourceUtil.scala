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

import io.glutenproject.substrait.rel.{IcebergLocalFilesBuilder, SplitInfo}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.softaffinity.SoftAffinityUtil
import org.apache.spark.sql.connector.read.{InputPartition, Scan}

import org.apache.iceberg.{CombinedScanTask, FileFormat, FileScanTask, ScanTask}

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._

object GlutenIcebergSourceUtil {

  def genSplitInfo(inputPartition: InputPartition, index: Int): SplitInfo = inputPartition match {
    case partition: SparkInputPartition =>
      val paths = new JArrayList[String]()
      val starts = new JArrayList[JLong]()
      val lengths = new JArrayList[JLong]()
      val partitionColumns = new JArrayList[JMap[String, String]]()
      var fileFormat = ReadFileFormat.UnknownFormat

      val tasks = partition.taskGroup[ScanTask]().tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          paths.add(task.file().path().toString)
          starts.add(task.start())
          lengths.add(task.length())
          partitionColumns.add(new JHashMap[String, String]())
          val currentFileFormat = task.file().format() match {
            case FileFormat.PARQUET => ReadFileFormat.ParquetReadFormat
            case FileFormat.ORC => ReadFileFormat.OrcReadFormat
            case _ =>
              throw new UnsupportedOperationException(
                "Iceberg Only support parquet and orc file format.")
          }
          if (fileFormat == ReadFileFormat.UnknownFormat) {
            fileFormat = currentFileFormat
          } else if (fileFormat != currentFileFormat) {
            throw new UnsupportedOperationException(
              s"Only one file format is supported, " +
                s"find different file format $fileFormat and $currentFileFormat")
          }
      }
      val preferredLoc = SoftAffinityUtil.getFilePartitionLocations(
        paths.asScala.toArray,
        inputPartition.preferredLocations())
      IcebergLocalFilesBuilder.makeIcebergLocalFiles(
        index,
        paths,
        starts,
        lengths,
        partitionColumns,
        fileFormat,
        preferredLoc.toList.asJava
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
}
