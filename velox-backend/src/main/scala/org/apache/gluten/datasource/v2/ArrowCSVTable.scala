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
package org.apache.gluten.datasource.v2

import org.apache.gluten.datasource.ArrowCSVOptionConverter
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.memory.arrow.pool.ArrowNativeMemoryPool
import org.apache.gluten.utils.ArrowUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.task.TaskResources

import org.apache.hadoop.fs.FileStatus

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class ArrowCSVTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val (allocator, pool) = if (!TaskResources.inSparkTask()) {
      TaskResources.runUnsafe(
        (ArrowBufferAllocators.contextInstance(), ArrowNativeMemoryPool.arrowPool("inferSchema"))
      )
    } else {
      (ArrowBufferAllocators.contextInstance(), ArrowNativeMemoryPool.arrowPool("inferSchema"))
    }
    val parsedOptions: CSVOptions = new CSVOptions(
      options.asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord
    )
    val arrowConfig = ArrowCSVOptionConverter.convert(parsedOptions)
    ArrowUtil.readSchema(
      files.head,
      org.apache.arrow.dataset.file.FileFormat.CSV,
      arrowConfig,
      allocator,
      pool
    )
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    ArrowCSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    throw new UnsupportedOperationException
  }

  override def formatName: String = "arrowcsv"
}
