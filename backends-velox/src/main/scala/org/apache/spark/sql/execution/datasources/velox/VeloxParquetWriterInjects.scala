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

import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.exception.GlutenException
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper
import io.glutenproject.utils.{ArrowAbiUtil, DatasourceUtil}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil

import com.google.common.base.Preconditions
import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.io.IOException

class VeloxParquetWriterInjects extends VeloxFormatWriterInjects {
  override def nativeConf(
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

  override def getFormatName(): String = {
    "parquet"
  }
}
