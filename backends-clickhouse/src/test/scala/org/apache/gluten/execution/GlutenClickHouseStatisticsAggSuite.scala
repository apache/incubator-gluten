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

class GlutenClickHouseStatisticsAggSuite extends GlutenClickHouseWholeStageTransformerSuite {

  // make sure precision after change to stable version
  test("var_samp precision") {
    withTable("example_table") {
      sql("""CREATE TABLE example_table(id int, value double) USING parquet""".stripMargin)
      sql("""INSERT INTO example_table VALUES (1, 10.5),
            |(2, 12.3), (3, 9.8), (4, 11.2), (5, 10.7)
            |""".stripMargin).show()
      val q =
        """SELECT
          |var_samp(value),
          |var_pop(value),
          |covar_samp(id, value),
          |covar_pop(id, value),
          |stddev_samp(value),
          |stddev_pop(value),
          |corr(id, value)
          |FROM example_table""".stripMargin
      compareResultsAgainstVanillaSpark(q, true, { _ => })
    }
  }

  test("covar_samp non-nullable input") {
    val q =
      """SELECT covar_samp(c1, c2), covar_pop(c1, c2)
        |FROM VALUES (1,1), (2,2), (3,3)
        |AS tab(c1, c2)""".stripMargin
    compareResultsAgainstVanillaSpark(q, true, { _ => })
  }

  test("corr non-nullable input") {
    val q =
      """SELECT corr(c1, c2)
        |FROM VALUES (1,1), (2,2), (3,3)
        |AS tab(c1, c2)""".stripMargin
    compareResultsAgainstVanillaSpark(q, true, { _ => })
  }

  test("var_samp Infinity") {
    withTable("example_table") {
      sql("""CREATE TABLE example_table(id int, value double) USING parquet""".stripMargin)
      sql("""INSERT INTO example_table VALUES (1, 10.5)
            |""".stripMargin)
      val q =
        """SELECT var_samp(value) FROM example_table""".stripMargin
      compareResultsAgainstVanillaSpark(q, true, { _ => })
    }
  }

  test("var_samp Infinity in window function") {
    val q =
      """SELECT VAR_SAMP(n) OVER
        |(ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        |FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n)""".stripMargin
    compareResultsAgainstVanillaSpark(q, true, { _ => })
  }

}
