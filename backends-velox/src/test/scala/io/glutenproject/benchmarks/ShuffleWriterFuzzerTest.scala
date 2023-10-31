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
package io.glutenproject.benchmarks

import io.glutenproject.execution.VeloxWholeStageTransformerSuite
import io.glutenproject.tags.FuzzerTest

import org.apache.spark.SparkConf

@FuzzerTest
class ShuffleWriterFuzzerTest extends VeloxWholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  private val dataGenerator = RandomParquetDataGenerator(System.currentTimeMillis())
  private val outputPath = getClass.getResource("/").getPath + "fuzzer_output.parquet"

  private val REPARTITION_SQL = "select /*+ REPARTITION(3) */ * from tbl"
  private val AGG_REPARTITION_SQL = "select count(*) from tbl group by f_1, f_2, f_3, f_4, f_5, f_6"
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "512MB")
      .set("spark.driver.memory", "4g")
  }

  def executeQuery(sql: String): Boolean = {
    try {
      System.gc()
      dataGenerator.generateRandomData(spark, outputPath)
      spark.read.format("parquet").load(outputPath).createOrReplaceTempView("tbl")
      spark.sql(sql).foreach(_ => ())
      true
    } catch {
      case t: Throwable =>
        logError(s"Failed to run test with seed: ${dataGenerator.getSeed}", t)
        false
    }
  }

  def repeatQuery(sql: String, iterations: Int): Unit = {
    val failed = (0 until iterations)
      .filterNot {
        i =>
          logWarning(
            s"==============================> " +
              s"Started iteration $i (seed: ${dataGenerator.getSeed})")
          val success = executeQuery(sql)
          dataGenerator.reFake(System.currentTimeMillis())
          success
      }
      .map(_ => dataGenerator.getSeed)
    if (failed.nonEmpty) {
      logError(s"Failed to run test with seed: ${failed.mkString(", ")}")
    }
  }

  test("repartition") {
    repeatQuery(REPARTITION_SQL, 10)
  }

  test("with aggregation") {
    repeatQuery(AGG_REPARTITION_SQL, 10)
  }

  ignore("reproduce") {
    val sql = REPARTITION_SQL
    Seq(0L).foreach {
      seed =>
        dataGenerator.reFake(seed)
        logWarning(
          s"==============================> " +
            s"Started reproduction (seed: ${dataGenerator.getSeed})")
        executeQuery(sql)
    }
  }
}
