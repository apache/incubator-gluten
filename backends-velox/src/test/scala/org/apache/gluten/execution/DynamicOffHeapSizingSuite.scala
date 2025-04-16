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
package org.apache.gluten.execution

import org.apache.gluten.benchmarks.RandomParquetDataGenerator
import org.apache.gluten.tags.SkipTest

import org.apache.spark.SparkConf

@SkipTest
class DynamicOffHeapSizingSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val dataGenerator = RandomParquetDataGenerator(System.currentTimeMillis())
  private val outputPath = getClass.getResource("/").getPath + "dynamicoffheapsizing_output.parquet"
  private val AGG_SQL =
    """select f_1, count(DISTINCT f_1)
      |from tbl group
      |group by 1""".stripMargin

  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.executor.memory", "6GB")
      .set("spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction", "0.8")
      .set("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "true")
  }

  def getRootCause(e: Throwable): Throwable = {
    if (e.getCause == null) {
      return e
    }
    getRootCause(e.getCause)
  }

  test("Dynamic off-heap sizing") {
    System.gc()
    dataGenerator.generateRandomData(spark, Some(outputPath))
    spark.read.format("parquet").load(outputPath).createOrReplaceTempView("tbl")
    spark.sql(AGG_SQL)
  }
}
