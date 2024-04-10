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
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.v1.GlutenMergeTreeWriterInjects
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

class DeltaMergeTreeFileFormat(metadata: Metadata) extends DeltaParquetFileFormat(metadata) {

  protected var database = ""
  protected var tableName = ""
  protected var snapshotId = ""
  protected var orderByKeyOption: Option[Seq[String]] = None
  protected var lowCardKeyOption: Option[Seq[String]] = None
  protected var minmaxIndexKeyOption: Option[Seq[String]] = None
  protected var bfIndexKeyOption: Option[Seq[String]] = None
  protected var setIndexKeyOption: Option[Seq[String]] = None
  protected var primaryKeyOption: Option[Seq[String]] = None
  protected var partitionColumns: Seq[String] = Seq.empty[String]
  protected var clickhouseTableConfigs: Map[String, String] = Map.empty

  // scalastyle:off argcount
  def this(
      metadata: Metadata,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      clickhouseTableConfigs: Map[String, String],
      partitionColumns: Seq[String]) {
    this(metadata)
    this.database = database
    this.tableName = tableName
    this.snapshotId = snapshotId
    this.orderByKeyOption = orderByKeyOption
    this.lowCardKeyOption = lowCardKeyOption
    this.minmaxIndexKeyOption = minmaxIndexKeyOption
    this.bfIndexKeyOption = bfIndexKeyOption
    this.setIndexKeyOption = setIndexKeyOption
    this.primaryKeyOption = primaryKeyOption
    this.clickhouseTableConfigs = clickhouseTableConfigs
    this.partitionColumns = partitionColumns
  }
  // scalastyle:on argcount

  override def shortName(): String = "mergetree"

  override def toString(): String = "MergeTree"

  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaMergeTreeFileFormat =>
        ff.columnMappingMode == columnMappingMode && ff.referenceSchema == referenceSchema
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

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        ".mergetree"
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        GlutenMergeTreeWriterInjects
          .getInstance()
          .createOutputWriter(
            path,
            database,
            tableName,
            snapshotId,
            orderByKeyOption,
            lowCardKeyOption,
            minmaxIndexKeyOption,
            bfIndexKeyOption,
            setIndexKeyOption,
            primaryKeyOption,
            partitionColumns,
            metadata.schema,
            clickhouseTableConfigs,
            context,
            nativeConf
          )
      }
    }
  }
}
