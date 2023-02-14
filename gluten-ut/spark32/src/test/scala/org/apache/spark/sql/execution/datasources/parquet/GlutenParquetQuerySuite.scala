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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.{DebugFilesystem, SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{SchemaColumnConvertNotSupportedException, SQLHadoopMapReduceCommitProtocol}
import org.apache.spark.sql.execution.datasources.parquet.TestingUDT.{NestedStruct, NestedStructUDT, SingleElement}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A test suite that tests various Parquet queries.
 */
class GlutenParquetV1QuerySuite extends ParquetV1QuerySuite with GlutenSQLTestsBaseTrait {
  override def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false")(code)
    // Disabled: We don't yet support this case as of now
    // test the vectorized reader
//    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }
}

class GlutenParquetV2QuerySuite extends ParquetV2QuerySuite with GlutenSQLTestsBaseTrait {
  override def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false")(code)
    // Disabled: We don't yet support this case as of now
    // test the vectorized reader
    //    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }
}
