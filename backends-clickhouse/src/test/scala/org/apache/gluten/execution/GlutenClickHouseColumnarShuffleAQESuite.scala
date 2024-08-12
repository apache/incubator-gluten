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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, AQEShuffleReadExec}

class GlutenClickHouseColumnarShuffleAQESuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper
  with Logging {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"
  private val backendConfigPrefix = "spark.gluten.sql.columnar.backend.ch."

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])

        val colCustomShuffleReaderExecs = collect(df.queryExecution.executedPlan) {
          case csr: AQEShuffleReadExec => csr
        }
        assert(colCustomShuffleReaderExecs.size == 2)
        val coalescedPartitionSpec0 = colCustomShuffleReaderExecs(0)
          .partitionSpecs(0)
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec0.startReducerIndex == 0)
        assert(coalescedPartitionSpec0.endReducerIndex == 5)
        val coalescedPartitionSpec1 = colCustomShuffleReaderExecs(1)
          .partitionSpecs(0)
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec1.startReducerIndex == 0)
        assert(coalescedPartitionSpec1.endReducerIndex == 5)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) { df => }
  }

  test("TPCH Q3") {
    runTPCHQuery(3) { df => }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    runTPCHQuery(5) { df => }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    runTPCHQuery(7) { df => }
  }

  test("TPCH Q8") {
    runTPCHQuery(8) { df => }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df => }
  }

  test("TPCH Q14") {
    runTPCHQuery(14) { df => }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false) { df => }
  }

  test("TPCH Q17") {
    runTPCHQuery(17) { df => }
  }

  test("TPCH Q18") {
    runTPCHQuery(18) {
      df =>
        val hashAggregates = collect(df.queryExecution.executedPlan) {
          case hash: HashAggregateExecBaseTransformer => hash
        }
        assert(hashAggregates.size == 3)
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df => }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df => }
  }

  test("TPCH Q21") {
    runTPCHQuery(21, noFallBack = false) { df => }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 3)
        assert(adaptiveSparkPlanExec(1) == adaptiveSparkPlanExec(2))
    }
  }

  test("GLUTEN-6768 rerorder hash join") {
    withSQLConf(
      ("spark.gluten.sql.columnar.backend.ch.enable_reorder_hash_join_tables", "true"),
      ("spark.sql.adaptive.enabled", "true")) {
      spark.sql("create table t1(a int, b int) using parquet")
      spark.sql("create table t2(a int, b int) using parquet")

      spark.sql("insert into t1 select id as a, id as b from range(100000)")
      spark.sql("insert into t1 select id as a, id as b from range(100)")

      def isExpectedJoinNode(plan: SparkPlan, joinType: JoinType, buildSide: BuildSide): Boolean = {
        plan match {
          case join: CHShuffledHashJoinExecTransformer =>
            join.joinType == joinType && join.buildSide == buildSide
          case _ => false
        }
      }

      def collectExpectedJoinNode(
          plan: SparkPlan,
          joinType: JoinType,
          buildSide: BuildSide): Seq[SparkPlan] = {
        if (isExpectedJoinNode(plan, joinType, buildSide)) {
          Seq(plan) ++ plan.children.flatMap(collectExpectedJoinNode(_, joinType, buildSide))
        } else {
          plan.children.flatMap(collectExpectedJoinNode(_, joinType, buildSide))
        }
      }

      var sql = """
                  |select * from t2 left join t1 on t1.a = t2.a
                  |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, RightOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      sql = """
              |select * from t2 right join t1 on t1.a = t2.a
              |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, LeftOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      sql = """
              |select * from t1 right join t2 on t1.a = t2.a
              |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, RightOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      spark.sql("drop table t1")
      spark.sql("drop table t2")
    }
  }

  test("GLUTEN-6768 change mixed join condition into multi join on clauses") {
    withSQLConf(
      (backendConfigPrefix + "runtime_config.prefer_inequal_join_to_multi_join_on_clauses", "true"),
      (
        backendConfigPrefix + "runtime_config.inequal_join_to_multi_join_on_clauses_row_limit",
        "1000000")
    ) {

      spark.sql("create table t1(a int, b int, c int, d int) using parquet")
      spark.sql("create table t2(a int, b int, c int, d int) using parquet")

      spark.sql("""
                  |insert into t1
                  |select id % 2 as a, id as b, id + 1 as c, id + 2 as d from range(1000)
                  |""".stripMargin)
      spark.sql("""
                  |insert into t2
                  |select id % 2 as a, id as b, id + 1 as c, id + 2 as d from range(1000)
                  |""".stripMargin)

      var sql = """
                  |select * from t1 join t2 on
                  |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or t1.d = t2.d)
                  |order by t1.a, t1.b, t1.c, t1.d
                  |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      sql = """
              |select * from t1 join t2 on
              |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or (t1.c = t2.c and t1.d = t2.d))
              |order by t1.a, t1.b, t1.c, t1.d
              |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      sql = """
              |select * from t1 join t2 on
              |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or (t1.d = t2.d and t1.c >= t2.c))
              |order by t1.a, t1.b, t1.c, t1.d
              |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      spark.sql("drop table t1")
      spark.sql("drop table t2")
    }
  }
}
