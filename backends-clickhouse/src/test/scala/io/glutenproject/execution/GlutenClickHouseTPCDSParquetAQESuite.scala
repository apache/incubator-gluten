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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

class GlutenClickHouseTPCDSParquetAQESuite
    extends GlutenClickHouseTPCDSAbstractSuite
    with AdaptiveSparkPlanHelper {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../jvm/src/test/resources/tpcds-queries"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  /**
   * Run Gluten + ClickHouse Backend with SortShuffleManager
   */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      // Currently, it can not support to read multiple partitioned file in one task.
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.sql.files.openCostInBytes", "134217728")
      .set("spark.sql.adaptive.enabled", "true")
  }

  test("test 'select count(*)'") {
    val df = spark.sql("""
        |select count(c_customer_sk) from customer
        |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 100000L)
  }

  test("test reading from partitioned table") {
    val df = spark.sql("""
        |select count(*)
        |  from store_sales
        |  where ss_quantity between 1 and 20
        |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 550458L)
  }

  test("test reading from partitioned table with partition column filter") {
    val df = spark.sql("""
        |select avg(ss_net_paid_inc_tax)
        |  from store_sales
        |  where ss_quantity between 1 and 20
        |  and ss_sold_date_sk = 2452635
        |""".stripMargin)
    val result = df.collect()
    assert(result(0).getDouble(0) == 379.21313271604936)
  }

  test("test select avg(int), avg(long)") {
    val testSql =
      """
        |select avg(cs_item_sk), avg(cs_order_number)
        |  from catalog_sales
        |""".stripMargin
    val result = spark.sql(testSql).collect()
    assert(result(0).getDouble(0) == 8998.463336886734)
    assert(result(0).getDouble(1) == 80037.12727449503)
  }

  test("TPCDS Q9") {
    withSQLConf(("spark.gluten.sql.columnar.columnartorow", "true")) {
      runTPCDSQuery(9) { df =>
        val subqueryAdaptiveSparkPlan = collectWithSubqueries(df.queryExecution.executedPlan) {
          case a: AdaptiveSparkPlanExec if a.isSubquery => a
        }
        assert(subqueryAdaptiveSparkPlan.size == 15)
      }
    }
  }

  test("TPCDS Q21") {
    withSQLConf(("spark.gluten.sql.columnar.columnartorow", "true")) {
      runTPCDSQuery(21) { df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer if f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              } =>
            f
        }
        assert(foundDynamicPruningExpr.nonEmpty == true)

        val reusedExchangeExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }
        assert(reusedExchangeExec.nonEmpty == true)
      }
    }
  }

  test("TPCDS Q21 with DPP + SHJ") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery(21) { df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer if f.partitionFilters.exists {
            case _: DynamicPruningExpression => true
            case _ => false
          } => f
        }
        assert(foundDynamicPruningExpr.nonEmpty == true)

        val reusedExchangeExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }
        assert(reusedExchangeExec.isEmpty)
      }
    }
  }

  test("TPCDS Q21 with non-separated scan rdd") {
    withSQLConf(("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")) {
      runTPCDSQuery(21) { df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer if f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              } =>
            f
        }
        assert(foundDynamicPruningExpr.nonEmpty == true)

        val reusedExchangeExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }
        assert(reusedExchangeExec.nonEmpty == true)
      }
    }
  }
}
