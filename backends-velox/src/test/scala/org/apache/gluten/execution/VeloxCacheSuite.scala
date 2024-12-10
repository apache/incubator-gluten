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

import org.apache.gluten.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class VeloxCacheSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("metrics_t1")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table metrics_t1")

    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_VELOX_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.LOAD_QUANTUM.key, "8MB")
  }

  test("Velox cache metrics") {
    val table = s"metrics_t1"

    // first scan, read from storage.
    val df = spark.sql(s"SELECT * FROM $table")
    val scans = collect(df.queryExecution.executedPlan) {
      case scan: FileSourceScanExecTransformer => scan
    }
    df.collect()
    assert(scans.length === 1)
    val metrics = scans.head.metrics
    assert(metrics("storageReadBytes").value > 0)
    assert(metrics("ramReadBytes").value == 0)

    // second scan, read from cache if cache enabled.
    val df2 = spark.sql(s"SELECT * FROM $table")
    val scans2 = collect(df2.queryExecution.executedPlan) {
      case scan: FileSourceScanExecTransformer => scan
    }
    df2.collect()
    assert(scans2.length === 1)
    val metrics2 = scans2.head.metrics
    assert(metrics2("storageReadBytes").value == 0)
    assert(metrics2("ramReadBytes").value > 0)
  }
}
