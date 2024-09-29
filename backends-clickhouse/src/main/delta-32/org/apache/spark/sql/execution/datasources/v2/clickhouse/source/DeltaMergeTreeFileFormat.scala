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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.clickhouse.ClickhouseMetaSerializer
import org.apache.spark.sql.execution.datasources.v1.{CHMergeTreeWriterInjects, GlutenMergeTreeWriterInjects}
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class DeltaMergeTreeFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    val snapshotId: String,
    val deltaPath: String,
    @transient val clickhouseTableConfigs: Map[String, String],
    val partitionColumns: Seq[String])
  extends DeltaParquetFileFormat(protocol, metadata) {

  override def shortName(): String = "mergetree"

  override def toString(): String = "MergeTree"

  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaMergeTreeFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
        ff.referenceSchema == referenceSchema &&
        ff.optimizationsEnabled == optimizationsEnabled
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // pass compression to job conf so that the file extension can be aware of it.
    // val conf = ContextUtil.getConfiguration(job)
    val nativeConf =
      GlutenMergeTreeWriterInjects
        .getInstance()
        .nativeConf(options, "")

    val database = clickhouseTableConfigs("storage_db")
    val tableName = clickhouseTableConfigs("storage_table")
    val extensionTableBC = sparkSession.sparkContext.broadcast(
      ClickhouseMetaSerializer
        .forWrite(
          snapshotId,
          deltaPath,
          metadata.schema,
          clickhouseTableConfigs
        )
        .toByteArray)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        ".mergetree"
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        require(path == deltaPath)
        GlutenMergeTreeWriterInjects
          .getInstance()
          .asInstanceOf[CHMergeTreeWriterInjects]
          .createOutputWriter(
            path,
            metadata.schema,
            context,
            nativeConf,
            database,
            tableName,
            extensionTableBC.value
          )
      }
    }
  }
}
