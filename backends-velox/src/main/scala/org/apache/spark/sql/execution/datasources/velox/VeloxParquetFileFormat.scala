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
package org.apache.spark.sql.execution.datasources.velox

import io.glutenproject.GlutenConfig
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper
import io.glutenproject.utils.{ArrowAbiUtil, DatasourceUtil}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil

import com.google.common.base.Preconditions
import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable

class VeloxParquetFileFormat
  extends GlutenParquetFileFormat
  with DataSourceRegister
  with Serializable {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    DatasourceUtil.readSchema(files)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // pass compression to job conf so that the file extension can be aware of it.
    val conf = ContextUtil.getConfiguration(job)
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)
    val nativeConf =
      VeloxParquetFileFormat.nativeConf(options, parquetOptions.compressionCodecClassName)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val originPath = path

        URI.create(originPath) // validate uri
        val arrowSchema = SparkArrowUtil.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
        val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
        var instanceId = -1L
        val datasourceJniWrapper = new DatasourceJniWrapper()
        val allocator = ArrowBufferAllocators.contextInstance()
        try {
          ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
          instanceId = datasourceJniWrapper.nativeInitDatasource(
            originPath,
            cSchema.memoryAddress(),
            nativeConf)
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        } finally {
          cSchema.close()
        }

        val writeQueue =
          new VeloxWriteQueue(instanceId, arrowSchema, allocator, datasourceJniWrapper, originPath)

        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            val batch = row.asInstanceOf[FakeRow].batch
            Preconditions.checkState(ColumnarBatches.isLightBatch(batch))
            ColumnarBatches.retain(batch)
            writeQueue.enqueue(batch)
          }

          override def close(): Unit = {
            writeQueue.close()
            datasourceJniWrapper.close(instanceId)
          }

          // Do NOT add override keyword for compatibility on spark 3.1.
          def path(): String = {
            originPath
          }
        }
      }
    }
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def shortName(): String = "velox"
}

object VeloxParquetFileFormat {
  def nativeConf(
      options: Map[String, String],
      compressionCodec: String): java.util.Map[String, String] = {
    // pass options to native so that velox can take user-specified conf to write parquet,
    // i.e., compression, block size, block rows.
    val sparkOptions = new mutable.HashMap[String, String]()
    sparkOptions.put(SQLConf.PARQUET_COMPRESSION.key, compressionCodec)
    val blockSize = options.getOrElse(
      GlutenConfig.PARQUET_BLOCK_SIZE,
      GlutenConfig.getConf.columnarParquetWriteBlockSize.toString)
    sparkOptions.put(GlutenConfig.PARQUET_BLOCK_SIZE, blockSize)
    val blockRows = options.getOrElse(
      GlutenConfig.PARQUET_BLOCK_ROWS,
      GlutenConfig.getConf.columnarParquetWriteBlockRows.toString)
    sparkOptions.put(GlutenConfig.PARQUET_BLOCK_ROWS, blockRows)
    sparkOptions.asJava
  }
}
