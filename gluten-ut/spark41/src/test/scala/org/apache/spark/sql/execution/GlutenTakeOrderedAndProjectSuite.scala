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

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, Rand}
import org.apache.spark.sql.types.{IntegerType, StructType}

class GlutenTakeOrderedAndProjectSuite
  extends TakeOrderedAndProjectSuite
  with GlutenSQLTestsBaseTrait {

  private def noOpFilter(plan: SparkPlan): SparkPlan = FilterExec(Literal(true), plan)

  testGluten("SPARK-47104: Non-deterministic expressions in projection") {
    val expected = (input: SparkPlan) => {
      GlobalLimitExec(limit, LocalLimitExec(limit, SortExec(sortOrder, true, input)))
    }
    val schema = StructType.fromDDL("a int, b int, c double")
    val rdd = sparkContext.parallelize(
      Seq(
        Row(1, 2, 0.6027633705776989d),
        Row(2, 3, 0.7151893651681639d),
        Row(3, 4, 0.5488135024422883d)),
      1)
    val df = spark.createDataFrame(rdd, schema)
    val projection = df.queryExecution.sparkPlan.output.take(2) :+
      Alias(Rand(Literal(0, IntegerType)), "_uuid")()

    // test executeCollect
    checkThatPlansAgree(
      df,
      input =>
        TakeOrderedAndProjectExec(limit, sortOrder, projection, SortExec(sortOrder, false, input)),
      input => expected(input),
      sortAnswers = false)

    // test doExecute
    checkThatPlansAgree(
      df,
      input =>
        noOpFilter(
          TakeOrderedAndProjectExec(
            limit,
            sortOrder,
            projection,
            SortExec(sortOrder, false, input))),
      input => expected(input),
      sortAnswers = false)
  }
}
