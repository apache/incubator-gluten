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

package org.apache.spark.sql

import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

class GlutenCachedTableSuite extends CachedTableSuite with GlutenSQLTestsTrait {
  import testImplicits._

  private def verifyNumExchanges(df: DataFrame, expected: Int): Unit = {
    assert(
      collect(df.queryExecution.executedPlan) {
        case e: ShuffleExchangeExec =>
          print("### exchange: " + e)
          e
        case p =>
          print("###: " + p)
      }.size == expected)
  }

  test("Gluten: A cached table preserves the partitioning and ordering of its cached SparkPlan") {
    val table3x = testData.union(testData).union(testData)
    table3x.createOrReplaceTempView("testData3x")

    sql("SELECT key, value FROM testData3x ORDER BY key").createOrReplaceTempView("orderedTable")
    spark.catalog.cacheTable("orderedTable")
    assertCached(spark.table("orderedTable"))
    // Should not have an exchange as the query is already sorted on the group by key.
    val df = sql("SELECT key, count(*) FROM orderedTable GROUP BY key")
    print(df.queryExecution.executedPlan)
    verifyNumExchanges(sql("SELECT key, count(*) FROM orderedTable GROUP BY key"), 0)
    checkAnswer(
      sql("SELECT key, count(*) FROM orderedTable GROUP BY key ORDER BY key"),
      sql("SELECT key, count(*) FROM testData3x GROUP BY key ORDER BY key").collect())
    uncacheTable("orderedTable")
    spark.catalog.dropTempView("orderedTable")

    // Set up two tables distributed in the same way. Try this with the data distributed into
    // different number of partitions.
    for (numPartitions <- 1 until 10 by 4) {
      withTempView("t1", "t2") {
        testData.repartition(numPartitions, $"key").createOrReplaceTempView("t1")
        testData2.repartition(numPartitions, $"a").createOrReplaceTempView("t2")
        spark.catalog.cacheTable("t1")
        spark.catalog.cacheTable("t2")

        // Joining them should result in no exchanges.
        verifyNumExchanges(sql("SELECT * FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a"), 0)
        checkAnswer(sql("SELECT * FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a"),
          sql("SELECT * FROM testData t1 JOIN testData2 t2 ON t1.key = t2.a"))

        // Grouping on the partition key should result in no exchanges
        verifyNumExchanges(sql("SELECT count(*) FROM t1 GROUP BY key"), 0)
        checkAnswer(sql("SELECT count(*) FROM t1 GROUP BY key"),
          sql("SELECT count(*) FROM testData GROUP BY key"))

        uncacheTable("t1")
        uncacheTable("t2")
      }
    }
  }
}
