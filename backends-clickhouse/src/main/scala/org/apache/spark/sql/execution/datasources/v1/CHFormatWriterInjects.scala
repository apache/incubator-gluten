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
import org.apache.gluten.utils.SubstraitUtil
import org.apache.gluten.vectorized.CHColumnVector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.types.StructType

import com.google.protobuf.Any
import io.substrait.proto
import io.substrait.proto.{AdvancedExtension, NamedObjectWrite, NamedStruct}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.{util => ju}

import scala.collection.JavaConverters.seqAsJavaListConverter

trait CHFormatWriterInjects extends GlutenFormatWriterInjectsBase {

  // TODO: move to SubstraitUtil
  private def toNameStruct(dataSchema: StructType): NamedStruct = {
    SubstraitUtil
      .createNameStructBuilder(
        ConverterUtils.collectAttributeTypeNodes(dataSchema),
        dataSchema.fieldNames.map(ConverterUtils.normalizeColName).toSeq.asJava,
        ju.Collections.emptyList()
      )
      .build()
  }
  def createWriteRel(
      outputPath: String,
      dataSchema: StructType,
      context: TaskAttemptContext): proto.WriteRel = {
    proto.WriteRel
      .newBuilder()
      .setTableSchema(toNameStruct(dataSchema))
      .setNamedTable(
        NamedObjectWrite.newBuilder
          .setAdvancedExtension(
            AdvancedExtension
              .newBuilder()
              .setOptimization(Any.pack(createNativeWrite(outputPath, context)))
              .build())
          .build())
      .build()
  }

  def createNativeWrite(outputPath: String, context: TaskAttemptContext): Write

  override def createOutputWriter(
      outputPath: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: ju.Map[String, String]): OutputWriter = {
    CHThreadGroup.registerNewThreadGroup()

    val datasourceJniWrapper =
      new CHDatasourceJniWrapper(outputPath, createWriteRel(outputPath, dataSchema, context))

    new OutputWriter {
      override def write(row: InternalRow): Unit = {
        assert(row.isInstanceOf[FakeRow])
        val nextBatch = row.asInstanceOf[FakeRow].batch

        if (nextBatch.numRows > 0) {
          val col = nextBatch.column(0).asInstanceOf[CHColumnVector]
          datasourceJniWrapper.write(col.getBlockAddress)
        } // else just ignore this empty block
      }

      override def close(): Unit = {
        datasourceJniWrapper.close()
      }

      // Do NOT add override keyword for compatibility on spark 3.1.
      def path(): String = {
        outputPath
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
