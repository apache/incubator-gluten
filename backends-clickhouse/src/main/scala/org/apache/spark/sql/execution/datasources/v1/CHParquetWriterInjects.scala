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

import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.vectorized.{CHColumnVector, CHNativeBlock}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{BlockStripe, CHDatasourceJniWrapper, FakeRow, GlutenParquetWriterInjectsBase, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig

import scala.collection.JavaConverters._

class CHParquetWriterInjects extends GlutenParquetWriterInjectsBase {

  def createOutputWriterFactory = {
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {

        val originPath = path
        val datasourceJniWrapper = new CHDatasourceJniWrapper();
        val instance = datasourceJniWrapper.nativeInitFileWriterWrapper(path);

        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            assert(row.isInstanceOf[FakeRow])
            val nextBatch = row.asInstanceOf[FakeRow].batch

            if (nextBatch.numRows > 0) {
              val col = nextBatch.column(0).asInstanceOf[CHColumnVector]
              datasourceJniWrapper.write(instance, col.getBlockAddress)
            } else throw new IllegalStateException
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
    }
  }

  override def inferSchema: Option[StructType] = {
    throw new IllegalStateException("CHParquetWriterInjects does not support inferSchema")
  }

  override def splitBlockByPartitionAndBucket(
      row: FakeRow,
      partitionColIndice: Array[Int],
      hasBucket: Boolean): CHBlockStripes = {
    val nextBatch = row.batch

    if (nextBatch.numRows > 0) {
      val col = nextBatch.column(0).asInstanceOf[CHColumnVector]
      new CHBlockStripes(
        CHDatasourceJniWrapper
          .splitBlockByPartitionAndBucket(col.getBlockAddress, partitionColIndice, hasBucket))
    } else throw new IllegalStateException
  }
}
