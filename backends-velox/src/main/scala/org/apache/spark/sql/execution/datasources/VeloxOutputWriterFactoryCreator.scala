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

package org.apache.spark.sql.execution.datasources

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, IndicatorVector}
import io.glutenproject.execution.datasource.GlutenOutputWriterFactoryCreator
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper
import io.glutenproject.utils.ArrowAbiUtil

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil

import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.codec.CodecConfig

import java.io.IOException

class VeloxOutputWriterFactoryCreator extends GlutenOutputWriterFactoryCreator {

  def createFactory(fileFormat: String, options: Map[String, String]): OutputWriterFactory =
    fileFormat match {
      case "parquet" =>
        parquetOutputWriter(options)
      case "hive" =>
        parquetOutputWriter(options)
      case _ => throw new UnsupportedOperationException(s"$fileFormat writer was not supported.")
    }

  private def parquetOutputWriter(options: Map[String, String]): OutputWriterFactory = {
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }

      override def newInstance(
          filePath: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val arrowSchema =
          SparkArrowUtil.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
        val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
        var instanceId = -1L
        val datasourceJniWrapper = new DatasourceJniWrapper()
        val allocator = ArrowBufferAllocators.contextInstance()
        try {
          ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
          import scala.collection.JavaConverters._
          instanceId = datasourceJniWrapper
            .nativeInitDatasource(filePath, cSchema.memoryAddress(), options.asJava)
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        } finally {
          cSchema.close()
        }

        val writeQueue =
          new VeloxWriteQueue(instanceId, arrowSchema, allocator, datasourceJniWrapper, filePath)

        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            val batch = row.asInstanceOf[FakeRow].batch
            if (batch.column(0).isInstanceOf[IndicatorVector]) {
              val giv = batch.column(0).asInstanceOf[IndicatorVector]
              giv.retain()
              writeQueue.enqueue(batch)
            } else {
              val offloaded =
                ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance, batch)
              writeQueue.enqueue(offloaded)
            }
          }

          override def close(): Unit = {
            writeQueue.close()
            datasourceJniWrapper.close(instanceId)
          }

          // Do NOT add override keyword for compatibility on spark 3.1.
          def path(): String = {
            filePath
          }
        }
      }
    }
  }
}
