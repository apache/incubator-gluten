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
package io.glutenproject.execution.extension

import io.glutenproject.execution.{GlutenClickHouseTPCHAbstractSuite, HashAggregateExecBaseTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.aggregate.CustomSum
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class GlutenCustomAggExpressionSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.gluten.sql.columnar.extended.expressions.transformer",
        "io.glutenproject.execution.extension.CustomAggExpressionTransformer")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createTPCHParquetTables(tablesPath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (expressionInfo, builder) =
      FunctionRegistryBase.build[CustomSum]("custom_sum", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("custom_sum"),
      expressionInfo,
      builder
    )
  }

  test("test custom aggregate function") {
    val sql =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    custom_sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price
         |FROM
         |    lineitem
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val hashAggExec = collect(df.queryExecution.executedPlan) {
            case hash: HashAggregateExecBaseTransformer => hash
          }
          assert(hashAggExec.size == 2)

          assert(hashAggExec(0).aggregateExpressions(0).aggregateFunction.isInstanceOf[CustomSum])
          assert(hashAggExec(1).aggregateExpressions(0).aggregateFunction.isInstanceOf[CustomSum])
      }
    )
  }
}
