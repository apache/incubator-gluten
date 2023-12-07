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
package io.glutenproject.extension

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.{ProjectExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CollapseProjectExecTransformerSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {

  protected val rootPath: String = getClass.getResource("/").getPath
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
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("Support ProjectExecTransformer collapse") {
    val query =
      """
        |SELECT
        |  o_orderpriority
        |FROM
        |  orders
        |WHERE
        |  o_orderdate >= '1993-07-01'
        |  AND EXISTS (
        |    SELECT
        |      *
        |    FROM
        |      lineitem
        |    WHERE
        |      l_orderkey = o_orderkey
        |      AND l_commitdate < l_receiptdate
        |  )
        |ORDER BY
        | o_orderpriority
        |LIMIT
        | 100;
        |""".stripMargin
    Seq(true, false).foreach {
      collapsed =>
        withSQLConf(GlutenConfig.ENABLE_COLUMNAR_PROJECT_COLLAPSE.key -> collapsed.toString) {
          runQueryAndCompare(query) {
            df =>
              {
                assert(
                  getExecutedPlan(df).exists {
                    case _ @ProjectExecTransformer(_, _: ProjectExecTransformer) => true
                    case _ => false
                  } == !collapsed
                )
              }
          }
        }
    }
  }
}
