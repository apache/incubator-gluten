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
import org.apache.gluten.utils.{BackendTestUtils, SystemParameters}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}

abstract class BaseMergeTwoPhasesHashBaseAggregateSuite extends WholeStageTransformerSuite {
  val fileFormat: String = "parquet"
  override protected val resourcePath: String = "/tpch-data-parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .sql(s"""
              |CREATE TABLE t1 (id int, age int, phone int, date string)
              |USING $fileFormat
              |PARTITIONED BY (date)
              |""".stripMargin)
      .show()

    spark
      .sql(s"""
              |INSERT INTO t1 PARTITION(date = '2020-01-01')
              |SELECT id, id % 10 as age, id % 10 as phone
              |FROM range(100)
              |""".stripMargin)
      .show()
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.gluten.sql.mergeTwoPhasesAggregate.enabled", "true")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
    }
    conf
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS t1")
    } finally {
      super.afterAll()
    }
  }

  test("Merge two phase hash-based aggregate into one aggregate") {
    def checkHashAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      df.collect()
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    withTempView("v1") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("v1")
      // no exchange hash aggregate, merge to one hash aggregate
      checkHashAggregateCount(
        spark.sql("""
                    |SELECT key, count(key)
                    |FROM v1
                    |GROUP BY key
                    |""".stripMargin),
        1
      )

      // with filter hash aggregate
      checkHashAggregateCount(
        spark.sql("""
                    |SELECT key, count(key) FILTER (WHERE key LIKE '%1%') AS pc2
                    |FROM v1
                    |GROUP BY key
                    |""".stripMargin),
        2
      )
    }

    // with exchange hash aggregate
    checkHashAggregateCount(
      spark.sql("""
                  |SELECT count(1) FROM t1
                  |""".stripMargin),
      2)
  }

  test("Merge two phase object-based aggregate into one aggregate") {
    def checkObjectAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      df.collect()
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    withTempView("v1") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("v1")
      // no exchange object aggregate, merge to one hash aggregate
      checkObjectAggregateCount(
        spark.sql("""
                    |SELECT key, collect_list(key)
                    |FROM v1
                    |GROUP BY key
                    |""".stripMargin),
        1
      )

      // with filter object aggregate
      checkObjectAggregateCount(
        spark.sql("""
                    |SELECT key, collect_list(key) FILTER (WHERE key LIKE '%1%') AS pc2
                    |FROM v1
                    |GROUP BY key
                    |""".stripMargin),
        2
      )
    }

    // with exchange object aggregate
    checkObjectAggregateCount(
      spark.sql("""
                  |SELECT collect_list(id) FROM t1
                  |""".stripMargin),
      2)
  }

  test("Merge two phase sort-based aggregate into one aggregate") {
    def checkSortAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      df.collect()
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    withSQLConf("spark.sql.test.forceApplySortAggregate" -> "true") {
      withTempView("v1") {
        spark.range(100).selectExpr("id as key").createOrReplaceTempView("v1")
        // no exchange sort aggregate, merge to one hash aggregate
        checkSortAggregateCount(
          spark.sql("""
                      |SELECT sum(if(key<0,0,key))
                      |FROM v1
                      |GROUP BY key
                      |""".stripMargin),
          1
        )

        // with filter sort aggregate
        checkSortAggregateCount(
          spark.sql("""
                      |SELECT key, sum(if(key<0,0,key)) FILTER (WHERE key LIKE '%1%') AS pc2
                      |FROM v1
                      |GROUP BY key
                      |""".stripMargin),
          2
        )
      }

      // with exchange sort aggregate
      checkSortAggregateCount(
        spark.sql("""
                    |SELECT sum(if(id<0,0,id)) FROM t1
                    |""".stripMargin),
        2)
    }
  }
}

class MergeTwoPhasesAggregateSuiteAEOn
  extends BaseMergeTwoPhasesHashBaseAggregateSuite
  with EnableAdaptiveExecutionSuite

class MergeTwoPhasesAggregateSuiteAEOff
  extends BaseMergeTwoPhasesHashBaseAggregateSuite
  with DisableAdaptiveExecutionSuite
