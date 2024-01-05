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
package io.substrait.spark

import org.apache.spark.sql.TPCDSBase
import org.apache.spark.sql.internal.SQLConf

class TPCDSPlan extends TPCDSBase with SubstraitPlanTestBase {

  private val runAllQueriesIncludeFailed = false
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")

    conf.setConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED, false)
    // introduced in spark 3.4
    spark.conf.set("spark.sql.readSideCharPadding", "false")
  }

  // "q9" failed in spark 3.3
  val successfulSQL: Set[String] = Set("q41", "q62", "q93", "q96", "q99")

  tpcdsQueries.foreach {
    q =>
      if (runAllQueriesIncludeFailed || successfulSQL.contains(q)) {
        test(s"check simplified (tpcds-v1.4/$q)") {
          testQuery("tpcds", q)
        }
      } else {
        ignore(s"check simplified (tpcds-v1.4/$q)") {
          testQuery("tpcds", q)
        }
      }
  }

  ignore("window") {
    val qry = s"""(SELECT
                 |    item_sk,
                 |    rank()
                 |    OVER (
                 |      ORDER BY rank_col DESC) rnk
                 |  FROM (SELECT
                 |    ss_item_sk item_sk,
                 |    avg(ss_net_profit) rank_col
                 |  FROM store_sales ss1
                 |  WHERE ss_store_sk = 4
                 |  GROUP BY ss_item_sk
                 |  HAVING avg(ss_net_profit) > 0.9 * (SELECT avg(ss_net_profit) rank_col
                 |  FROM store_sales
                 |  WHERE ss_store_sk = 4
                 |    AND ss_addr_sk IS NULL
                 |  GROUP BY ss_store_sk)) V2) """.stripMargin
    assertSqlSubstraitRelRoundTrip(qry)
  }
}
