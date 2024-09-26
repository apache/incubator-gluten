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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.gluten.execution.datasource.GlutenRowSplitter
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.memory.CHThreadGroup
import org.apache.gluten.vectorized.CHColumnVector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.types.StructType

import io.substrait.proto.{NamedStruct, Type}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

trait CHFormatWriterInjects extends GlutenFormatWriterInjectsBase {

  override def createOutputWriter(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: java.util.Map[String, String]): OutputWriter = {
    val originPath = path
    val datasourceJniWrapper = new CHDatasourceJniWrapper()
    CHThreadGroup.registerNewThreadGroup()

    val namedStructBuilder = NamedStruct.newBuilder
    val structBuilder = Type.Struct.newBuilder
    for (field <- dataSchema.fields) {
      namedStructBuilder.addNames(field.name)
      structBuilder.addTypes(ConverterUtils.getTypeNode(field.dataType, field.nullable).toProtobuf)
    }
    namedStructBuilder.setStruct(structBuilder.build)
    val namedStruct = namedStructBuilder.build

    val instance =
      datasourceJniWrapper.nativeInitFileWriterWrapper(path, namedStruct.toByteArray, formatName)

    new OutputWriter {
      override def write(row: InternalRow): Unit = {
        assert(row.isInstanceOf[FakeRow])
        val nextBatch = row.asInstanceOf[FakeRow].batch

        if (nextBatch.numRows > 0) {
          val col = nextBatch.column(0).asInstanceOf[CHColumnVector]
          datasourceJniWrapper.write(instance, col.getBlockAddress)
        } // else just ignore this empty block
      }

      override def close(): Unit = {
        datasourceJniWrapper.close(instance)
      }

      // Do NOT add override keyword for compatibility on spark 3.1.
      def path(): String = {
        originPath
      }
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    // TODO: parquet and mergetree
    OrcUtils.inferSchema(sparkSession, files, options)
  }

  // scalastyle:off argcount
  /** For CH MergeTree format */
  def createOutputWriter(
      path: String,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      tableSchema: StructType,
      clickhouseTableConfigs: Map[String, String],
      context: TaskAttemptContext,
      nativeConf: java.util.Map[String, String]): OutputWriter = null
  // scalastyle:on argcount
}

class CHRowSplitter extends GlutenRowSplitter {
  override def splitBlockByPartitionAndBucket(
      row: FakeRow,
      partitionColIndice: Array[Int],
      hasBucket: Boolean,
      reserve_partition_columns: Boolean = false): CHBlockStripes = {
    val col = row.batch.column(0).asInstanceOf[CHColumnVector]
    new CHBlockStripes(
      CHDatasourceJniWrapper
        .splitBlockByPartitionAndBucket(
          col.getBlockAddress,
          partitionColIndice,
          hasBucket,
          reserve_partition_columns))
  }
}
