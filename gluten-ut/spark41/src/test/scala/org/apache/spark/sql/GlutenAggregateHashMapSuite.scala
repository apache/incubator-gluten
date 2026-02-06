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
package org.apache.spark.sql

import org.apache.gluten.execution.{HashAggregateExecTransformer, WholeStageTransformer}

import org.apache.spark.sql.execution.aggregate.{ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, collect_list, expr, monotonically_increasing_id, rand, randn, spark_partition_id, sum}
import org.apache.spark.sql.internal.SQLConf

import scala.util.Random

class GlutenSingleLevelAggregateHashMapSuite
  extends SingleLevelAggregateHashMapSuite
  with GlutenSQLTestsTrait {
  import testImplicits._

  private def assertNoExceptions(wholeStageBoundary: Boolean, c: Column): Unit = {
    for (
      (wholeStage, useObjectHashAgg) <-
        Seq((true, true), (true, false), (false, true), (false, false))
    ) {
      withSQLConf(
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString),
        (SQLConf.USE_OBJECT_HASH_AGG.key, useObjectHashAgg.toString)) {

        val df = Seq(("1", 1), ("1", 2), ("2", 3), ("2", 4)).toDF("x", "y")

        // test case for HashAggregate
        // Converts to HashAggregateExecTransformer
        val hashAggDF = df.groupBy("x").agg(c, sum("y"))
        hashAggDF.collect()
        val hashAggPlan = hashAggDF.queryExecution.executedPlan
        assert(find(hashAggPlan) {
          case WholeStageTransformer(_: HashAggregateExecTransformer, _) if wholeStageBoundary =>
            true
          case _: HashAggregateExecTransformer if !wholeStageBoundary => true
          case _ => false
        }.isDefined)

        // test case for ObjectHashAggregate and SortAggregate
        // Both gets converted to HashAggregateExecTransformer
        val objHashAggOrSortAggDF = df.groupBy("x").agg(c, collect_list("y"))
        objHashAggOrSortAggDF.collect()
        val objHashAggOrSortAggPlan =
          stripAQEPlan(objHashAggOrSortAggDF.queryExecution.executedPlan)
        assert(find(objHashAggOrSortAggPlan) {
          case WholeStageTransformer(_: HashAggregateExecTransformer, _) if wholeStageBoundary =>
            true
          case _: HashAggregateExecTransformer if !wholeStageBoundary => true
          case _ => false
        }.isDefined)
      }
    }
  }

  testGluten(
    "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
      " before using it") {
    Seq(
      (true, monotonically_increasing_id()),
      (false, spark_partition_id()),
      (false, rand(Random.nextLong())),
      (false, randn(Random.nextLong()))
    ).foreach(tup => assertNoExceptions(tup._1, tup._2))
  }

  testGluten("SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      val df = Seq(("1", "2", 1), ("1", "2", 2), ("2", "3", 3), ("2", "3", 4))
        .toDF("a", "b", "c")
        .repartition(col("a"))

      val objHashAggDF = df
        .withColumn("d", expr("(a, b, c)"))
        .groupBy("a", "b")
        .agg(collect_list("d").as("e"))
        .withColumn("f", expr("(b, e)"))
        .groupBy("a")
        .agg(collect_list("f").as("g"))
      val aggPlan = objHashAggDF.queryExecution.executedPlan

      val sortAggPlans = collect(aggPlan) { case sortAgg: SortAggregateExec => sortAgg }
      // SortAggregate will be retained due velox_collect_list
      assert(sortAggPlans.size == 4)

      val objHashAggPlans = collect(aggPlan) {
        case objHashAgg: ObjectHashAggregateExec => objHashAgg
      }
      assert(objHashAggPlans.isEmpty)

      val exchangePlans = collect(aggPlan) { case shuffle: ShuffleExchangeExec => shuffle }
      assert(exchangePlans.length == 1)
    }
  }

  Seq(true, false).foreach {
    value =>
      testGluten(s"SPARK-31620: agg with subquery (whole-stage-codegen = $value)") {
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> value.toString) {
          withTempView("t1", "t2") {
            sql("create temporary view t1 as select * from values (1, 2) as t1(a, b)")
            sql("create temporary view t2 as select * from values (3, 4) as t2(c, d)")

            // test without grouping keys
            checkAnswer(
              sql("select sum(if(c > (select a from t1), d, 0)) as csum from t2"),
              Row(4) :: Nil)

            // test with grouping keys
            checkAnswer(
              sql(
                "select c, sum(if(c > (select a from t1), d, 0)) as csum from " +
                  "t2 group by c"),
              Row(3, 4) :: Nil)

            // test with distinct
            checkAnswer(
              sql(
                "select avg(distinct(d)), sum(distinct(if(c > (select a from t1)," +
                  " d, 0))) as csum from t2 group by c"),
              Row(4, 4) :: Nil)

            // test subquery with agg
            checkAnswer(
              sql(
                "select sum(distinct(if(c > (select sum(distinct(a)) from t1)," +
                  " d, 0))) as csum from t2 group by c"),
              Row(4) :: Nil)

            // test SortAggregateExec
            // Converts to HashAggregateExecTransformer
            var df = sql("select max(if(c > (select a from t1), 'str1', 'str2')) as csum from t2")
            df.collect()
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
            checkAnswer(df, Row("str1") :: Nil)

            // test ObjectHashAggregateExec
            // Converts to HashAggregateExecTransformer
            df =
              sql("select collect_list(d), sum(if(c > (select a from t1), d, 0)) as csum from t2")
            df.collect()
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
            checkAnswer(df, Row(Array(4), 4) :: Nil)
          }
        }
      }
  }
}

class GlutenTwoLevelAggregateHashMapSuite
  extends TwoLevelAggregateHashMapSuite
  with GlutenSQLTestsTrait {}

class GlutenTwoLevelAggregateHashMapWithVectorizedMapSuite
  extends TwoLevelAggregateHashMapWithVectorizedMapSuite
  with GlutenSQLTestsTrait {}
