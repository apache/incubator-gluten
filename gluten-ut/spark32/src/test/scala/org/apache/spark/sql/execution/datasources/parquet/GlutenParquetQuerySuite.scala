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

  test("tmp") {
    val data = (1 to 10).map { i =>
      val ts = new java.sql.Timestamp(i)
      ts.setNanos(2000)
      Row(i, ts)
    }
    val schema = StructType(List(StructField("d", IntegerType, false),
      StructField("time", TimestampType, false)).toArray)
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      withTempPath { file =>
        val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
        df.write.parquet(file.getCanonicalPath)
        withAllParquetReaders {
          val df2 = spark.read.parquet(file.getCanonicalPath)
          df2.explain(true)
          checkAnswer(df2, df.collect().toSeq)
        }
      }
    }
  }
}

class GlutenParquetV2QuerySuite extends ParquetV2QuerySuite with GlutenSQLTestsBaseTrait {
}
