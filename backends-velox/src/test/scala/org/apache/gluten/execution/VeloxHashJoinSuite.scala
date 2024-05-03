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

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, InputIteratorTransformer}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}

class VeloxHashJoinSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  test("generate hash join plan - v1") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      ("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      // scalastyle:off println
      System.out.println(plan)
      // scalastyle:on println line=68 column=19
      assert(joins.length == 2)

      // Children of Join should be seperated into different `TransformContext`s.
      assert(joins.forall(_.children.forall(_.isInstanceOf[InputIteratorTransformer])))

      // WholeStageTransformer should be inserted for joins and its children separately.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }
      assert(wholeStages.length == 5)

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      assert(countSHJ == 2)
    }
  }

  testWithSpecifiedSparkVersion("generate hash join plan - v2", Some("3.2")) {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      ("spark.gluten.sql.columnar.forceShuffledHashJoin", "true"),
      ("spark.sql.sources.useV1SourceList", "avro")
    ) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      assert(joins.length == 2)

      // The computing is combined into one single whole stage transformer.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }
      if (SparkShimLoader.getSparkVersion.startsWith("3.2.")) {
        assert(wholeStages.length == 1)
      } else if (SparkShimLoader.getSparkVersion.startsWith("3.5.")) {
        assert(wholeStages.length == 5)
      } else {
        assert(wholeStages.length == 3)
      }

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      if (SparkShimLoader.getSparkVersion.startsWith("3.2.")) {
        assert(countSHJ == 1)
      } else {
        assert(countSHJ == 2)
      }
    }
  }

  test("Reuse broadcast exchange for different build keys with same table") {
    withTable("t1", "t2") {
      spark.sql("""
                  |CREATE TABLE t1 USING PARQUET
                  |AS SELECT id as c1, id as c2 FROM range(10)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE TABLE t2 USING PARQUET
                  |AS SELECT id as c1, id as c2 FROM range(3)
                  |""".stripMargin)

      val df = spark.sql("""
                           |SELECT * FROM t1
                           |JOIN t2 as tmp1 ON t1.c1 = tmp1.c1 and tmp1.c1 = tmp1.c2
                           |JOIN t2 as tmp2 on t1.c2 = tmp2.c2 and tmp2.c1 = tmp2.c2
                           |""".stripMargin)

      assert(collect(df.queryExecution.executedPlan) {
        case b: BroadcastExchangeExec => b
      }.size == 2)

      checkAnswer(
        df,
        Row(2, 2, 2, 2, 2, 2) :: Row(1, 1, 1, 1, 1, 1) :: Row(0, 0, 0, 0, 0, 0) :: Nil)

      assert(collect(df.queryExecution.executedPlan) {
        case b: ColumnarBroadcastExchangeExec => b
      }.size == 1)
      assert(collect(df.queryExecution.executedPlan) {
        case r @ ReusedExchangeExec(_, _: ColumnarBroadcastExchangeExec) => r
      }.size == 1)
    }
  }
}
