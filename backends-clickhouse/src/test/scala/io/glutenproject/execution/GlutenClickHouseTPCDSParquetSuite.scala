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
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Not}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

class GlutenClickHouseTPCDSParquetSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.original"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      // Currently, it can not support to read multiple partitioned file in one task.
      //      .set("spark.sql.files.maxPartitionBytes", "134217728")
      //      .set("spark.sql.files.openCostInBytes", "134217728")
      .set("spark.memory.offHeap.size", "4g")
  }

  tpcdsAllQueries.foreach(
    sql =>
      if (!independentTestTpcdsQueries.contains(sql)) {
        if (excludedTpcdsQueries.contains(sql)) {
          ignore(s"TPCDS ${sql.toUpperCase()}") {
            runTPCDSQuery(sql) { df => }
          }
        } else {
          test(s"TPCDS ${sql.toUpperCase()}") {
            runTPCDSQuery(sql) { df => }
          }
        }
      })

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

  test("test union all operator with two tables") {
    val testSql =
      """
        |select count(date_sk) from (
        |  select d_date_sk as date_sk from date_dim
        |  union all
        |  select ws_sold_date_sk as date_sk from web_sales
        |)
        |""".stripMargin
    val result = spark.sql(testSql).collect()
    assert(result(0).getLong(0) == 791809)
  }

  test("test union all operator with three tables") {
    val testSql =
      """
        |select count(date_sk) from (
        |  select d_date_sk as date_sk from date_dim
        |  union all
        |  select ws_sold_date_sk as date_sk from web_sales
        |  union all (
        |   select ws_sold_date_sk as date_sk from web_sales limit 100
        |  )
        |)
        |""".stripMargin
    val result = spark.sql(testSql).collect()
    assert(result(0).getLong(0) == 791909)
  }

  test("test union operator with two tables") {
    val testSql =
      """
        |select count(date_sk) from (
        |  select d_date_sk as date_sk from date_dim
        |  union
        |  select ws_sold_date_sk as date_sk from web_sales
        |)
        |""".stripMargin
    val result = spark.sql(testSql).collect()
    assert(result(0).getLong(0) == 73049)
  }

  test(
    "test fallback operations not supported by ch backend " +
      "in CHHashJoinExecTransformer && CHBroadcastHashJoinExecTransformer") {
    val testSql =
      """
        |SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact,
        | 	sum(ss_ext_sales_price) AS ext_price
        | FROM date_dim
        | LEFT JOIN store_sales ON d_date_sk = ss_sold_date_sk
        | LEFT JOIN item ON ss_item_sk = i_item_sk AND i_manager_id = 7
        | LEFT JOIN customer ON ss_customer_sk = c_customer_sk
        | LEFT JOIN customer_address ON c_current_addr_sk = ca_address_sk
        | LEFT JOIN store ON ss_store_sk = s_store_sk AND substr(ca_zip,1,5) <> substr(s_zip,1,5)
        | WHERE d_moy = 11
        |   AND d_year = 1999
        | GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        | ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    val operateWithCondition = df.queryExecution.executedPlan.collect {
      case f: BroadcastHashJoinExec if f.condition.get.isInstanceOf[Not] => f
    }
    assert(
      operateWithCondition(0).left
        .asInstanceOf[InputAdapter]
        .child
        .isInstanceOf[CHColumnarToRowExec])
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
        val foundDynamicPruningExpr = df.queryExecution.executedPlan.collect {
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
    withSQLConf(("spark.gluten.sql.columnar.columnarToRow", "true")) {
      runTPCDSQuery("q9") {
        df =>
          var countSubqueryExec = 0
          df.queryExecution.executedPlan.transformAllExpressions {
            case s @ ScalarSubquery(_: SubqueryExec, _) =>
              countSubqueryExec = countSubqueryExec + 1
              s
            case s @ ScalarSubquery(_: ReusedSubqueryExec, _) =>
              countSubqueryExec = countSubqueryExec + 1
              s
          }
          assert(countSubqueryExec == 15)
      }
    }
  }

  test("TPCDS Q21") {
    withSQLConf(("spark.gluten.sql.columnar.columnarToRow", "true")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty == true)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.nonEmpty == true)
      }
    }
  }

  test("TPCDS Q21 with DPP + SHJ") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty == true)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.isEmpty)
      }
    }
  }

  test("TPCDS Q21 with non-separated scan rdd") {
    withSQLConf(("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty == true)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.nonEmpty == true)
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

  test("Bug-382 collec_list failure") {
    val sql =
      """
        |select cc_call_center_id, collect_list(cc_call_center_sk) from call_center group by cc_call_center_id
        |order by cc_call_center_id
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, df => {})
  }
}
