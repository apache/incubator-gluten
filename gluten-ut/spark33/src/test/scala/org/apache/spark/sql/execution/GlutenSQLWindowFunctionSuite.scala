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

import io.glutenproject.execution.WindowExecTransformer

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.Row

class GlutenSQLWindowFunctionSuite extends SQLWindowFunctionSuite with GlutenSQLTestsTrait {

  import testImplicits._

  val customerData: Seq[(Int, Int)] = Seq[(Int, Int)](
    (4553, 11),
    (4953, 10),
    (35403, 5),
    (35803, 12),
    (60865, 5),
    (61065, 13),
    (127412, 13),
    (148303, 10),
    (9954, 5),
    (95337, 12)
  )

  test("Literal in window partition by and sort") {
    withTable("customer") {
      customerData
        .toDF("c_custkey", "c_nationkey")
        .write
        .format("parquet")
        .saveAsTable("customer")
      val query =
        """
          |SELECT
          |  c_custkey,
          |  row_number() OVER (
          |    PARTITION BY c_nationkey,
          |    "a"
          |    ORDER BY
          |      c_custkey,
          |      "a"
          |  ) AS row_num
          |FROM
          |   customer
          |ORDER BY 1, 2;
          |""".stripMargin
      val df = sql(query)
      checkAnswer(
        df,
        Seq(
          Row(4553, 1),
          Row(4953, 1),
          Row(9954, 1),
          Row(35403, 2),
          Row(35803, 1),
          Row(60865, 3),
          Row(61065, 1),
          Row(95337, 2),
          Row(127412, 2),
          Row(148303, 2))
      )
      assert(
        getExecutedPlan(df).exists {
          case _: WindowExecTransformer => true
          case _ => false
        }
      )
    }
  }
}
