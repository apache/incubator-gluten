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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.storage.StorageLevel

class VeloxColumnarCacheSuite extends WholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "3")
  }

  private def checkColumnarTableCache(plan: SparkPlan): Unit = {
    assert(
      find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.isDefined,
      plan)
    assert(
      collect(plan) { case v: VeloxColumnarToRowExec => v }.size <= 1,
      plan
    )
  }

  test("input columnar batch") {
    TPCHTables.foreach {
      case (table, _) =>
        runQueryAndCompare(s"SELECT * FROM $table", cache = true) {
          df => checkColumnarTableCache(df.queryExecution.executedPlan)
        }
    }
  }

  test("input columnar batch and column pruning") {
    val expected = sql("SELECT l_partkey FROM lineitem").collect()
    val cached = sql("SELECT * FROM lineitem").cache()
    try {
      val df = cached.select("l_partkey")
      checkAnswer(df, expected)
      checkColumnarTableCache(df.queryExecution.executedPlan)
    } finally {
      cached.unpersist()
    }
  }

  test("input row") {
    withTable("t") {
      sql("CREATE TABLE t USING json AS SELECT * FROM values(1, 'a', (2, 'b'), (3, 'c'))")
      runQueryAndCompare("SELECT * FROM t", cache = true) {
        df => checkColumnarTableCache(df.queryExecution.executedPlan)
      }
    }
  }

  test("input vanilla Spark columnar batch") {
    withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
      val df = spark.table("lineitem")
      val expected = df.collect()
      val actual = df.cache()
      try {
        checkAnswer(actual, expected)
      } finally {
        actual.unpersist()
      }
    }
  }

  test("CachedColumnarBatch serialize and deserialize") {
    val df = spark.table("lineitem")
    val expected = df.collect()
    val actual = df.persist(StorageLevel.DISK_ONLY)
    try {
      checkAnswer(actual, expected)
    } finally {
      actual.unpersist()
    }
  }

  test("Support transform count(1) with table cache") {
    val cached = spark.table("lineitem").cache()
    try {
      val df = spark.sql("SELECT COUNT(*) FROM lineitem")
      checkAnswer(df, Row(60175))
      assert(
        find(df.queryExecution.executedPlan) {
          case _: RowToVeloxColumnarExec => true
          case _ => false
        }.isEmpty
      )
    } finally {
      cached.unpersist()
    }
  }
}
