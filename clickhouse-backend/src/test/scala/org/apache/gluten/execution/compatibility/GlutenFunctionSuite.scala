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
package org.apache.gluten.execution.compatibility

import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite
import org.apache.gluten.test.GlutenSQLTestUtils
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.internal.Logging

class GlutenFunctionSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with GlutenSQLTestUtils
  with Logging {

  override protected val fileFormat: String = "parquet"
  private val testPath: String = s"${UTSystemParameters.testDataPath}/$fileFormat/function"

  case class TestCase(
      name: String,
      sql: String,
      ignore: Boolean = false
  )

  private val testCase = Seq(
    TestCase(
      "left",
      s"""|select
          |    left(`99`, 2)
          |  , left(`100`, 3)
          |  , left(`101`, 4)
          |  , left(`101`, 0)
          |  , left(`101`, -1)  -- error
          | from parquet.`$testPath/left`
          |""".stripMargin
    ),
    TestCase(
      "trim",
      s"""|select
          |    trim(both ' ' from `99`)
          |  , trim(LEADING `100` from `99`) -- error
          |  , trim(TRAILING `100` from `99`)  -- error
          | from parquet.`$testPath/left`
          |""".stripMargin
    ),
    TestCase(
      "date_format 1",
      s"""|select
          |   `0`
          |  , date_format(`0`, 'y')
          |  , date_format(`0`, 'M')
          |  , date_format(`0`, 'D') -- error timezone related issue
          |  , date_format(`0`, 'd')
          |  , date_format(`0`, 'H')
          |  , date_format(`0`, 'h')
          |  , date_format(`0`, 'm')
          |  , date_format(`0`, 's')
          | from parquet.`$testPath/date_format/date`
          |""".stripMargin
    ),
    TestCase(
      "date_format 2",
      s"""|select
          |   `4`
          |  , date_format(`4`, 'y')
          |  , date_format(`4`, 'M')
          |  , date_format(`4`, 'D') -- error timezone related issue
          |  , date_format(`4`, 'd')
          |  , date_format(`4`, 'H')
          |  , date_format(`4`, 'h')
          |  , date_format(`4`, 'm')
          |  , date_format(`4`, 's')
          | from parquet.`$testPath/date_format/timestamp`
          |""".stripMargin
    )
  )

  testCase.foreach {
    data =>
      if (data.ignore) {
        ignore(s"${data.name}") {}
      } else {
        test(s"${data.name}") {
          compareResultsAgainstVanillaSpark(
            data.sql,
            compareResult = true,
            { _ => }
          )
        }
      }
  }

}
