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
import org.apache.spark.sql.execution.{ApplyResourceProfileExec, ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

class AutoAdjustStageResourceProfileSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.auto.adjustStageResource.enabled", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
    spark
      .range(100)
      .selectExpr("cast(id % 9 as int) as c1")
      .write
      .format("parquet")
      .saveAsTable("tmp2")
    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "cast(id % 9 as int) as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp3")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table tmp1")
    spark.sql("drop table tmp2")
    spark.sql("drop table tmp3")

    super.afterAll()
  }

  private def collectColumnarToRow(plan: SparkPlan): Int = {
    collect(plan) { case v: VeloxColumnarToRowExec => v }.size
  }

  private def collectColumnarShuffleExchange(plan: SparkPlan): Int = {
    collect(plan) { case c: ColumnarShuffleExchangeExec => c }.size
  }

  private def collectShuffleExchange(plan: SparkPlan): Int = {
    collect(plan) { case c: ShuffleExchangeExec => c }.size
  }

  private def collectApplyResourceProfileExec(plan: SparkPlan): Int = {
    collect(plan) { case c: ApplyResourceProfileExec => c }.size
  }

  test("stage contains r2c and apply new resource profile") {
    withSQLConf(
      GlutenConfig.COLUMNAR_SHUFFLE_ENABLED.key -> "false",
      GlutenConfig.AUTO_ADJUST_STAGE_RESOURCES_C2R_OR_R2C_THRESHOLD.key -> "1") {
      runQueryAndCompare("select c1, count(*) from tmp1 group by c1") {
        df =>
          val plan = df.queryExecution.executedPlan

          assert(collectColumnarShuffleExchange(plan) == 0)
          assert(collectShuffleExchange(plan) == 1)

          val wholeQueryColumnarToRow = collectColumnarToRow(plan)
          assert(wholeQueryColumnarToRow == 2)

          val applyResourceProfileExec = collectApplyResourceProfileExec(plan)
          assert(applyResourceProfileExec == 1)
        // here we can't check the applied resource profile since
        // ResourceProfiles are only supported on YARN and Kubernetes
        // with dynamic allocation enabled. In testing mode, we apply
        // default resource profile to make sure ut works.
      }
    }
  }

  test("whole stage fallback") {
    withSQLConf(
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1",
      GlutenConfig.AUTO_ADJUST_STAGE_RESOURCES_C2R_OR_R2C_THRESHOLD.key -> "1"
    ) {
      runQueryAndCompare(
        "select " +
          "java_method('java.lang.Integer', 'signum', tmp1.c1), count(*) " +
          "from tmp1 group by java_method('java.lang.Integer', 'signum', tmp1.c1)") {
        df =>
          assert(
            collect(df.queryExecution.executedPlan) {
              case h: HashAggregateExecTransformer => h
            }.size == 2,
            df.queryExecution.executedPlan)
          assert(collectApplyResourceProfileExec(df.queryExecution.executedPlan) == 2)
      }
    }
  }
}
