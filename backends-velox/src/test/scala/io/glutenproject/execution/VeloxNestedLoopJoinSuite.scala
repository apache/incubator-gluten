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

import org.apache.spark.SparkConf

class VeloxNestedLoopJoinSuite extends VeloxWholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.crossJoin.enabled", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")

  test(
    "cross join with equi join condition should not get " +
      "converted to cartesian product") {
    createTPCHNotNullTables()
    val query = "select * from nation cross join region on n_regionkey = r_regionkey"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.isEmpty)
  }

  test(
    "cross join with non equi join conditions should get converted to" +
      "CartesianProductExecTransformer") {
    createTPCHNotNullTables()
    val query = "select * from nation cross join region on n_regionkey != r_regionkey"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.size == 1)
  }

  test(
    "cross join with no join condition should get converted to  " +
      "CartesianProductExecTransformer") {
    createTPCHNotNullTables()
    val query = "select * from nation cross join region"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.size == 1)
  }
}
