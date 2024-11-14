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

import org.apache.gluten.execution.datasource.GlutenFormatFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.mergetree.{DeltaMetaReader, StorageMeta}
import org.apache.spark.sql.execution.datasources.v1.Write
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

trait MergeTreeFileFormat extends FileFormat with DataSourceRegister {

  def metadata: Metadata

  override def shortName(): String = "mergetree"
  override def toString: String = "MergeTree"

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    // pass compression to job conf so that the file extension can be aware of it.
    val conf = job.getConfiguration
    val nativeConf = GlutenFormatFactory(shortName()).nativeConf(options, "")

    @transient val deltaMetaReader = DeltaMetaReader(metadata)
    deltaMetaReader.storageConf.foreach { case (k, v) => conf.set(k, v) }

    new OutputWriterFactory {

      /** no extension for MergeTree */
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {

        /** dataSchema doesn't contain partition column */
        GlutenFormatFactory(shortName())
          .createOutputWriter(path, metadata.schema, context, nativeConf)
      }
    }
  }
}

object MergeTreeFileFormat {
  def createWrite(outputPath: Option[String], conf: Map[String, String]): Write.MergeTreeWrite = {
    Write.MergeTreeWrite
      .newBuilder()
      .setDatabase(conf(StorageMeta.DB))
      .setTable(conf(StorageMeta.TABLE))
      .setSnapshotId(conf(StorageMeta.SNAPSHOT_ID))
      .setOrderByKey(conf(StorageMeta.ORDER_BY_KEY))
      .setLowCardKey(conf(StorageMeta.LOW_CARD_KEY))
      .setMinmaxIndexKey(conf(StorageMeta.MINMAX_INDEX_KEY))
      .setBfIndexKey(conf(StorageMeta.BF_INDEX_KEY))
      .setSetIndexKey(conf(StorageMeta.SET_INDEX_KEY))
      .setPrimaryKey(conf(StorageMeta.PRIMARY_KEY))
      .setRelativePath(outputPath.map(StorageMeta.normalizeRelativePath).getOrElse(""))
      .setAbsolutePath("")
      .setStoragePolicy(conf(StorageMeta.POLICY))
      .build()
  }
  def createWrite(metadata: Metadata): Write.MergeTreeWrite = {
    // we can get the output path at the driver side
    val deltaMetaReader = DeltaMetaReader(metadata)
    createWrite(None, deltaMetaReader.storageConf)
  }
}
