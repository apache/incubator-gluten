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

class GlutenClickHouseDSV2Suite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 1)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
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
    runTPCHQuery(11) { df => }
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
    runTPCHQuery(15) { df => }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false) { df => }
  }

  test("TPCH Q17") {
    runTPCHQuery(17) { df => }
  }

  test("TPCH Q18") {
    runTPCHQuery(18) { df => }
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
    runTPCHQuery(22) { df => }
  }

  test("test 'select count(1)' with empty columns to read") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select count(*)' with empty columns to read") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select sum(2)' with empty columns to read") {
    val result = runSql("""
                          |select sum(2) from lineitem
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'select 1' with empty columns to read") {
    val result = runSql("""
                          |select 1 from lineitem limit 2
                          |""".stripMargin) { _ => }
    assert(result.size == 2)
    assert(result(0).getInt(0) == 1 && result(1).getInt(0) == 1)
  }
}
