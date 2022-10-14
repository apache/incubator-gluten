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

import java.io.IOException

import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DwrfDatasourceJniWrapper
import io.glutenproject.utils.{ArrowAbiUtil, VeloxDatasourceUtil}
import io.glutenproject.vectorized.NativeColumnarToRowInfo
import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.VeloxColumnarRules._

class DwrfFileFormat extends FileFormat with DataSourceRegister with Serializable {

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    VeloxDatasourceUtil.readSchema(files)
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".dwrf"
      }

      override def newInstance(path: String, dataSchema: StructType,
                               context: TaskAttemptContext): OutputWriter = {
        val originPath = path
        val arrowSchema = ArrowUtils.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
        val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
        var instanceId = -1L
        val dwrfDatasourceJniWrapper = new DwrfDatasourceJniWrapper()
        try {
          ArrowAbiUtil.exportSchema(ArrowBufferAllocators.contextInstance(), arrowSchema, cSchema)
          instanceId = dwrfDatasourceJniWrapper.nativeInitDwrfDatasource(originPath,
            cSchema.memoryAddress())
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        } finally {
          cSchema.close()
        }

        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            val batch = row.asInstanceOf[FakeRow].batch
            val allocator = ArrowBufferAllocators.contextInstance()
            val cArray = ArrowArray.allocateNew(allocator)
            val cSchema = ArrowSchema.allocateNew(allocator)
            var info: NativeColumnarToRowInfo = null
            try {
              ArrowAbiUtil.exportFromSparkColumnarBatch(
                ArrowBufferAllocators.contextInstance(), batch, cSchema, cArray)

              dwrfDatasourceJniWrapper.write(instanceId, cSchema.memoryAddress(),
                cArray.memoryAddress())
            } finally {
              cArray.close()
              cSchema.close()
            }
          }

          override def close(): Unit = {
            dwrfDatasourceJniWrapper.close(instanceId)
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

  override def shortName(): String = "dwrf"
}
