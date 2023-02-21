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

package org.apache.spark.sql.execution

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ColumnarAQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, QueryTest, Row, SparkSession}

class GlutenCoalesceShufflePartitionsSuite extends CoalesceShufflePartitionsSuite
  with GlutenSQLTestsBaseTrait {

  object ColumnarCoalescedShuffleRead {
    def unapply(read: ColumnarAQEShuffleReadExec): Boolean = {
      !read.isLocalRead && !read.hasSkewedPartition && read.hasCoalescedPartition
    }
  }

  test(GLUTEN_TEST +
      "SPARK-24705 adaptive query execution works correctly when exchange reuse enabled") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      spark.sql("SET spark.sql.exchange.reuse=true")
      val df = spark.range(0, 6, 1).selectExpr("id AS key", "id AS value")

      // test case 1: a query stage has 3 child stages but they are the same stage.
      // Final Stage 1
      //   ShuffleQueryStage 0
      //   ReusedQueryStage 0
      //   ReusedQueryStage 0
      val resultDf = df.join(df, "key").join(df, "key")
      QueryTest.checkAnswer(resultDf, (0 to 5).map(i => Row(i, i, i, i)))
      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      assert(finalPlan.collect {
        case ShuffleQueryStageExec(_, r: ReusedExchangeExec, _) => r
      }.length == 2)
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleRead() => r
          case c @ ColumnarCoalescedShuffleRead() => c
        }.length == 3)

      // test case 2: a query stage has 2 parent stages.
      // Final Stage 3
      //   ShuffleQueryStage 1
      //     ShuffleQueryStage 0
      //   ShuffleQueryStage 2
      //     ReusedQueryStage 0
      val grouped = df.groupBy("key").agg(max("value").as("value"))
      val resultDf2 = grouped.groupBy(col("key") + 1).max("value")
        .union(grouped.groupBy(col("key") + 2).max("value"))
      QueryTest.checkAnswer(resultDf2, Row(1, 0) :: Row(2, 0) :: Row(2, 1) :: Row(3, 1) ::
        Row(3, 2) :: Row(4, 2) :: Row(4, 3) :: Row(5, 3) :: Row(5, 4) :: Row(6, 4) :: Row(6, 5) ::
        Row(7, 5) :: Nil)

      val finalPlan2 = resultDf2.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan

      // The result stage has 2 children
      val level1Stages = finalPlan2.collect { case q: QueryStageExec => q }
      assert(level1Stages.length == 2)
      level1Stages.foreach(qs =>
        assert(qs.plan.collect {
          case r @ CoalescedShuffleRead() => r
          case c @ ColumnarCoalescedShuffleRead() => c
        }.length == 1,
          "Wrong CoalescedShuffleRead below " + qs.simpleString(3)))

      val leafStages = level1Stages.flatMap { stage =>
        // All of the child stages of result stage have only one child stage.
        val children = stage.plan.collect { case q: QueryStageExec => q }
        assert(children.length == 1)
        children
      }
      assert(leafStages.length == 2)

      val reusedStages = level1Stages.flatMap { stage =>
        stage.plan.collect {
          case ShuffleQueryStageExec(_, r: ReusedExchangeExec, _) => r
        }
      }
      assert(reusedStages.length == 1)
    }
    withSparkSession(test, 400, None)
  }

  test(GLUTEN_TEST + "SPARK-34790: enable IO encryption in AQE partition coalescing") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      val ds = spark.range(0, 100, 1, numInputPartitions)
      val resultDf = ds.repartition(ds.col("id"))
      resultDf.collect()

      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleRead() => r
          case c @ ColumnarCoalescedShuffleRead() => c
        }.isDefinedAt(0))
    }
    Seq(true, false).foreach { enableIOEncryption =>
      // Before SPARK-34790, it will throw an exception when io encryption enabled.
      withSparkSession(test, Int.MaxValue, None, enableIOEncryption)
    }
  }

}
