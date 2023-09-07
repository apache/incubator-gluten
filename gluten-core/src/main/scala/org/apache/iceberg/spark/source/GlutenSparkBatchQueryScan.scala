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

import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.connector.read.Scan

import org.apache.iceberg.FileFormat

import scala.collection.JavaConverters._

object GlutenSparkBatchQueryScan {
  def getFileFormat(scan: Scan): ReadFileFormat = {
    scan match {
      case scan: SparkBatchQueryScan =>
        val tasks = scan.tasks().asScala
        tasks.map(_.asCombinedScanTask()).foreach {
          task =>
            val file = task.files().asScala.head.file()
            file.format() match {
              case FileFormat.PARQUET => return ReadFileFormat.ParquetReadFormat
              case FileFormat.ORC => return ReadFileFormat.OrcReadFormat
              case _ =>
            }
        }
        throw new UnsupportedOperationException("Iceberg Only support parquet and orc file format.")
      case _ =>
        throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
    }
  }
}
