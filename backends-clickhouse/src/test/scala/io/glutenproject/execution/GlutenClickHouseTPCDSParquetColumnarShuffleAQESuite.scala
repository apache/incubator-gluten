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
import org.apache.spark.sql.execution.ReusedSubqueryExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

class GlutenClickHouseTPCDSParquetColumnarShuffleAQESuite
  extends GlutenClickHouseTPCDSAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.original"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      // Currently, it can not support to read multiple partitioned file in one task.
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.sql.files.openCostInBytes", "134217728")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.memory.offHeap.size", "3g")
  }

  tpcdsAllQueries.foreach(
    sql =>
      if (!independentTestTpcdsQueries.contains(sql)) {
        if (excludedTpcdsQueries.contains(sql)) {
          ignore(s"TPCDS ${sql.toUpperCase()}") {
            runTPCDSQuery(sql) { df => }
          }
        } else {
          ignore(s"TPCDS ${sql.toUpperCase()}") {
            runTPCDSQuery(sql) { df => }
          }
        }
      })

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

  test("Gluten-1235: Fix missing reading from the broadcasted value when executing DPP") {
    val testSql =
      """
        |select dt.d_year
        |       ,sum(ss_ext_sales_price) sum_agg
        | from  date_dim dt
        |      ,store_sales
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |   and dt.d_moy=12
        | group by dt.d_year
        | order by dt.d_year
        |         ,sum_agg desc
        |  LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      testSql,
      true,
      df => {
        val foundDynamicPruningExpr = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(foundDynamicPruningExpr.size == 2)
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .partitionFilters
            .exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .selectedPartitions
            .size == 1823)
      }
    )
  }

  test("TPCDS Q9") {
    withSQLConf(("spark.gluten.sql.columnar.columnartorow", "true")) {
      runTPCDSQuery("q9") {
        df =>
          val subqueryAdaptiveSparkPlan = collectWithSubqueries(df.queryExecution.executedPlan) {
            case a: AdaptiveSparkPlanExec if a.isSubquery => true
            case r: ReusedSubqueryExec => true
            case _ => false
          }
          // On Spark 3.2, there are 15 AdaptiveSparkPlanExec,
          // and on Spark 3.3, there are 5 AdaptiveSparkPlanExec and 10 ReusedSubqueryExec
          assert(subqueryAdaptiveSparkPlan.filter(_ == true).size == 15)
      }
    }
  }

  test("TPCDS Q21") {
    withSQLConf(("spark.gluten.sql.columnar.columnartorow", "true")) {
      runTPCDSQuery("q21") {
        df =>
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
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery("q21") {
        df =>
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
          assert(reusedExchangeExec.isEmpty)
      }
    }
  }

  test("TPCDS Q21 with non-separated scan rdd") {
    withSQLConf(("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")) {
      runTPCDSQuery("q21") {
        df =>
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

  test("Gluten-1234: Fix error when executing hash agg after union all") {
    val testSql =
      """
        |select channel, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        |  SELECT 'store' as channel, ss_ext_sales_price ext_sales_price
        |  FROM store_sales, item, date_dim
        |  WHERE ss_addr_sk IS NULL
        |    AND ss_sold_date_sk=d_date_sk
        |    AND ss_item_sk=i_item_sk
        |  UNION ALL
        |  SELECT 'web' as channel, ws_ext_sales_price ext_sales_price
        |  FROM web_sales, item, date_dim
        |  WHERE ws_web_page_sk IS NULL
        |    AND ws_sold_date_sk=d_date_sk
        |    AND ws_item_sk=i_item_sk
        |  ) foo
        |GROUP BY channel
        |ORDER BY channel
        | LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(testSql, true, df => {})
  }
}
