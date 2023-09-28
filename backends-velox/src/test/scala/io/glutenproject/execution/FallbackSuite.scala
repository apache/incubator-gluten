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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, ColumnarAQEShuffleReadExec}

class FallbackSuite extends WholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
    spark
      .range(100)
      .selectExpr("cast(id % 9 as int) as c1")
      .write
      .format("parquet")
      .saveAsTable("tmp2")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table tmp1")
    spark.sql("drop table tmp2")

    super.afterAll()
  }

  private def collectColumnarToRow(plan: SparkPlan): Int = {
    collect(plan) { case v: VeloxColumnarToRowExec => v }.size
  }

  test("fallback with collect") {
    withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
      runQueryAndCompare("SELECT count(*) FROM tmp1") {
        df =>
          val columnarToRow = collectColumnarToRow(df.queryExecution.executedPlan)
          assert(columnarToRow == 1)
      }
    }
  }

  test("fallback with bhj") {
    withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "2") {
      runQueryAndCompare(
        """
          |SELECT *, java_method('java.lang.Integer', 'sum', tmp1.c1, tmp2.c1) FROM tmp1
          |LEFT JOIN tmp2 on tmp1.c1 = tmp2.c1
          |""".stripMargin
      ) {
        df =>
          val plan = df.queryExecution.executedPlan
          val bhj = find(plan) {
            case _: BroadcastHashJoinExecTransformer => true
            case _ => false
          }
          assert(bhj.isDefined)
          val columnarToRow = collectColumnarToRow(bhj.get)
          assert(columnarToRow == 0)

          val wholeQueryColumnarToRow = collectColumnarToRow(plan)
          assert(wholeQueryColumnarToRow == 1)
      }

      // before the fix, it would fail
      spark
        .sql("""
               |SELECT *, java_method('java.lang.Integer', 'sum', tmp1.c1, tmp2.c1) FROM tmp1
               |LEFT JOIN tmp2 on tmp1.c1 = tmp2.c1
               |""".stripMargin)
        .show()
    }
  }

  test("fallback with AQE read") {
    runQueryAndCompare(
      """
        |select java_method('java.lang.Integer', 'sum', c1, c1), * from (
        |select /*+ repartition */ cast(c1 as int) as c1 from tmp1
        |)
        |""".stripMargin
    ) {
      df =>
        val aqeRead = find(df.queryExecution.executedPlan) {
          case _: ColumnarAQEShuffleReadExec => true
          case _ => false
        }
        assert(aqeRead.isDefined)
    }
  }
}
