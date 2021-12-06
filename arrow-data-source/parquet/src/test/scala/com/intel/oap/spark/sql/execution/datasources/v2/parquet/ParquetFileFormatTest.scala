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
package com.intel.oap.spark.sql.execution.datasources.v2.parquet

import java.io.File

import com.intel.oap.vectorized.ArrowWritableColumnVector

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetSQLConf
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.test.SharedSparkSession

class ParquetFileFormatTest extends QueryTest with SharedSparkSession {

  private val parquetFile1 = "parquet-1.parquet"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(256 * 1024 * 1024))
    conf
  }

  def closeAllocators(): Unit = {
    SparkMemoryUtils.contextAllocator().close()
  }

  test("overwrite write only") {
    import testImplicits._
    withSQLConf(ParquetSQLConf.OVERWRITE_PARQUET_DATASOURCE_READ.key -> "false") {
      ServiceLoaderUtil.ensureParquetFileFormatOverwritten()
      spark.read
        .json(Seq("{\"col\": -1}", "{\"col\": 0}", "{\"col\": 1}", "{\"col\": 2}",
          "{\"col\": null}")
          .toDS())
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(ParquetFileFormatTest.locateResourcePath(parquetFile1))
      val path = ParquetFileFormatTest.locateResourcePath(parquetFile1)
      val frame = spark.read.parquet(path)
      val eplan = frame.queryExecution.executedPlan
      assert(eplan.toString
        .contains("Format: Parquet-Overwritten-By-Arrow"))
      val scan = eplan.find(_.isInstanceOf[FileSourceScanExec]).get
      val typeAssertions = scan.executeColumnar()
          .flatMap(b => (0 until b.numCols()).map(b.column(_)))
          .map(!_.isInstanceOf[ArrowWritableColumnVector])
          .collect()
      assert(typeAssertions.forall(p => p))
    }
  }

  test("overwrite read and write") {
    import testImplicits._
    ServiceLoaderUtil.ensureParquetFileFormatOverwritten()
    spark.read
      .json(Seq("{\"col\": -1}", "{\"col\": 0}", "{\"col\": 1}", "{\"col\": 2}", "{\"col\": null}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ParquetFileFormatTest.locateResourcePath(parquetFile1))
    val path = ParquetFileFormatTest.locateResourcePath(parquetFile1)
    val frame = spark.read.parquet(path)
    val eplan = frame.queryExecution.executedPlan
    assert(eplan.toString
      .contains("Format: Parquet-Overwritten-By-Arrow"))
    val scan = eplan.find(_.isInstanceOf[FileSourceScanExec]).get
    val typeAssertions = scan.executeColumnar()
        .flatMap(b => (0 until b.numCols()).map(b.column(_)))
        .map(_.isInstanceOf[ArrowWritableColumnVector])
        .collect()
    assert(typeAssertions.forall(p => p))
  }
}

object ParquetFileFormatTest {
  private def locateResourcePath(resource: String): String = {
    classOf[ParquetFileFormatTest].getClassLoader.getResource("")
      .getPath.concat(File.separator).concat(resource)
  }
}
