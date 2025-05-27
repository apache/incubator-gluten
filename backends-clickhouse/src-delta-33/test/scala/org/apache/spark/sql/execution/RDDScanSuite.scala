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

import org.apache.gluten.execution._
import org.apache.gluten.extension.CHRemoveTopmostColumnarToRow

import org.apache.spark.sql.Dataset

class RDDScanSuite extends ParquetSuite {

  test("test RDDScanTransform") {
    val test_sql =
      """
        |SELECT
        |    l_returnflag,
        |    l_linestatus,
        |    sum(l_quantity) AS sum_qty
        |FROM
        |    lineitem
        |WHERE
        |    l_shipdate <= date'1998-09-02' - interval 1 day
        |GROUP BY
        |    l_returnflag,
        |    l_linestatus
        |""".stripMargin

    val expectedAnswer = sql(test_sql).collect()

    spark.sparkContext.setLocalProperty(
      CHRemoveTopmostColumnarToRow.REMOVE_TOPMOST_COLUMNAR_TO_ROW,
      "true")
    val data = sql(test_sql)
    val node = LogicalRDD.fromDataset(
      rdd = data.queryExecution.toRdd,
      originDataset = data,
      isStreaming = false)

    spark.sparkContext.setLocalProperty(
      CHRemoveTopmostColumnarToRow.REMOVE_TOPMOST_COLUMNAR_TO_ROW,
      "false")
    val df = Dataset.ofRows(data.sparkSession, node).toDF()
    checkAnswer(df, expectedAnswer)

    var cnt = df.queryExecution.executedPlan.collect { case _: CHRDDScanTransformer => true }
    assertResult(1)(cnt.size)

    val data2 = sql(test_sql)
    val node2 = LogicalRDD.fromDataset(
      rdd = data2.queryExecution.toRdd,
      originDataset = data2,
      isStreaming = false)

    val df2 = Dataset.ofRows(data2.sparkSession, node2).toDF()
    checkAnswer(df2, expectedAnswer)
    cnt = df2.queryExecution.executedPlan.collect { case _: CHRDDScanTransformer => true }
    assertResult(1)(cnt.size)
  }
}
