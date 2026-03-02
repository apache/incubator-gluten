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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarSubqueryBroadcastExec, InputIteratorTransformer}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}

class VeloxHashJoinSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  test("generate hash join plan - v1") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      (GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
    ) {
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

  test("generate hash join plan - v2") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      (GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true"),
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

      if (
        SparkShimLoader.getSparkVersion.startsWith("3.3.") ||
        SparkShimLoader.getSparkVersion.startsWith("3.4.")
      ) {
        assert(wholeStages.length == 3)
      } else {
        assert(wholeStages.length == 5)
      }

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

  test("Reuse broadcast exchange for different build keys with same table") {
    Seq("true", "false").foreach(
      enabledOffheapBroadcast =>
        withSQLConf(
          VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key -> enabledOffheapBroadcast) {
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
        })
  }

  test("ColumnarBuildSideRelation with small columnar to row memory") {
    Seq("true", "false").foreach(
      enabledOffheapBroadcast =>
        withSQLConf(
          GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key -> "16",
          VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key -> enabledOffheapBroadcast) {
          withTable("t1", "t2") {
            spark.sql("""
                        |CREATE TABLE t1 USING PARQUET
                        |AS SELECT id as c1, id as c2 FROM range(10)
                        |""".stripMargin)

            spark.sql("""
                        |CREATE TABLE t2 USING PARQUET PARTITIONED BY (c1)
                        |AS SELECT id as c1, id as c2 FROM range(30)
                        |""".stripMargin)

            val df = spark.sql("""
                                 |SELECT t1.c2
                                 |FROM t1, t2
                                 |WHERE t1.c1 = t2.c1
                                 |AND t1.c2 < 4
                                 |""".stripMargin)

            checkAnswer(df, Row(0) :: Row(1) :: Row(2) :: Row(3) :: Nil)

            val subqueryBroadcastExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
              case subqueryBroadcast: ColumnarSubqueryBroadcastExec => subqueryBroadcast
            }
            assert(subqueryBroadcastExecs.size == 1)
          }
        })
  }

  test("ColumnarBuildSideRelation transform support multiple key columns") {
    Seq("true", "false").foreach(
      enabledOffheapBroadcast =>
        withSQLConf(
          VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key -> enabledOffheapBroadcast) {
          withTable("t1", "t2") {
            val df1 =
              (0 until 50)
                .map(i => (i % 2, i % 3, s"${i % 25}"))
                .toDF("t1_c1", "t1_c2", "date")
                .as("df1")
            val df2 = (0 until 50)
              .map(i => (i % 11, i % 13, s"${i % 10}"))
              .toDF("t2_c1", "t2_c2", "date")
              .as("df2")
            df1.write.partitionBy("date").saveAsTable("t1")
            df2.write.partitionBy("date").saveAsTable("t2")

            val df = sql("""
                           |SELECT t1.date, t1.t1_c1, t2.t2_c2
                           |FROM t1
                           |JOIN t2 ON t1.date = t2.date
                           |WHERE t1.date=if(3 <= t2.t2_c2, if(3 < t2.t2_c1, 3, t2.t2_c1), t2.t2_c2)
                           |ORDER BY t1.date DESC, t1.t1_c1 DESC, t2.t2_c2 DESC
                           |LIMIT 1
                           |""".stripMargin)

            checkAnswer(df, Row("3", 1, 4) :: Nil)
            // collect the DPP plan.
            val subqueryBroadcastExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
              case subqueryBroadcast: ColumnarSubqueryBroadcastExec => subqueryBroadcast
            }
            assert(subqueryBroadcastExecs.size == 2)
            val buildKeysAttrs = subqueryBroadcastExecs
              .flatMap(_.buildKeys)
              .map(e => e.collect { case a: AttributeReference => a })
            // the buildKeys function can accept expressions with multiple columns.
            assert(buildKeysAttrs.exists(_.size > 1))
          }
        })
  }

  test("duplicate projections") {
    withTable("t1", "t2", "t3") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").write.saveAsTable("t1")
      Seq(1, 2, 3).toDF("c1").write.saveAsTable("t2")
      Seq(1, 2, 3).toDF("c1").write.saveAsTable("t3")
      // Test HashProbe.
      val q1 =
        """
          |select tt1.* from
          |(select c1,c2, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |""".stripMargin
      val q2 =
        """
          |select tt1.* from
          |(select c1, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |limit 1
          |""".stripMargin
      val q3 =
        """
          |select tt1.* from
          |(select c1, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |left join t3
          |on tt1.c1 = t3.c1
          |""".stripMargin
      val q4 =
        """
          |select tt1.* from
          |(select c1, c2, c2 from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |""".stripMargin

      // Test FilterProject.
      val q5 =
        """
          |select c1, c2, a, b from
          |(select c1, c2, c2 as a, c2 as b, rand() as c from t1) tt1
          |where c > -1 and b > 1
          |""".stripMargin

      Seq(q1, q2, q3, q4, q5).foreach {
        runQueryAndCompare(_) { _ => }
      }
    }
  }

  test("Hash probe dynamic filter pushdown") {
    withSQLConf(
      VeloxConfig.HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED.key -> "true",
      VeloxConfig.HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE.key -> "1048576"
    ) {
      withTable("probe_table", "build_table") {
        spark.sql("""
        CREATE TABLE probe_table USING PARQUET
        AS SELECT id as a FROM range(110001)
      """)

        spark.sql("""
        CREATE TABLE build_table USING PARQUET
        AS SELECT id * 1000 as b FROM range(220002)
      """)

        runQueryAndCompare(
          "SELECT a FROM probe_table JOIN build_table ON a = b"
        ) {
          df =>
            val join = find(df.queryExecution.executedPlan) {
              case _: BroadcastHashJoinExecTransformer => true
              case _ => false
            }
            assert(join.isDefined)
            val metrics = join.get.metrics
            assert(metrics.contains("bloomFilterBlocksByteSize"))
            assert(metrics("bloomFilterBlocksByteSize").value > 0)

            assert(metrics.contains("hashProbeDynamicFiltersProduced"))
            assert(metrics("hashProbeDynamicFiltersProduced").value == 1)
        }
      }
    }
  }

  test("Hash probe dynamic filter pushdown - string") {
    withSQLConf(
      VeloxConfig.HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED.key -> "true",
      VeloxConfig.HASH_PROBE_STRING_DYNAMIC_FILTER_PUSHDOWN_ENABLED.key -> "true"
    ) {
      withTable("probe_table_str", "build_table_str") {

        spark.sql("""
        CREATE TABLE probe_table_str USING PARQUET AS
        SELECT CAST(id AS STRING) AS a
        FROM range(100000)
      """)

        spark.sql("""
        CREATE TABLE build_table_str USING PARQUET AS
        SELECT CAST(id * 1000 AS STRING) AS b
        FROM range(10)
      """)

        runQueryAndCompare(
          "SELECT a FROM probe_table_str JOIN build_table_str ON a = b"
        ) {
          df =>
            val join = find(df.queryExecution.executedPlan) {
              case _: BroadcastHashJoinExecTransformer => true
              case _ => false
            }
            assert(join.isDefined)

            val metrics = join.get.metrics

            assert(metrics.contains("hashProbeDynamicFiltersProduced"))
            assert(metrics("hashProbeDynamicFiltersProduced").value == 1)
        }
      }
    }
  }
}
