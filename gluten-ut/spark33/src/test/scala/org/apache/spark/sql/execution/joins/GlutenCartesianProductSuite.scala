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
package org.apache.spark.sql.execution.joins

import io.glutenproject.execution.CartesianProductExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, TPCHBase}
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

class GlutenCartesianProductSuite extends TPCHBase with GlutenSQLTestsBaseTrait {

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    conf
  }

  test(
    GLUTEN_TEST + "cross join with equi join condition should not get " +
      "converted to cartesian product") {

    val query = "select * from lineitem cross join orders on l_orderkey = o_orderkey"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.isEmpty)
  }

  test(GLUTEN_TEST + "cross join with non equi join conditions are not yet supported") {
    val query = "select * from lineitem cross join orders on l_orderkey < o_orderkey"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.isEmpty)
    assert(plan.isInstanceOf[CartesianProductExec])
  }

  test(
    GLUTEN_TEST + "cross join with no join condition should get converted to  " +
      "CartesianProductExecTransformer") {
    val query = "select * from lineitem cross join orders"
    val df = sql(query)
    df.collect()

    val plan = df.queryExecution.executedPlan
    assert(plan.collectWithSubqueries { case c: CartesianProductExecTransformer => c }.size == 1)
  }
}
