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

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}

class GlutenDataFrameJoinSuite extends DataFrameJoinSuite with GlutenSQLTestsTrait {

  override def blackTestNameList: Seq[String] = Seq(
    "join - sorted columns not in join's outputSet",
    "join - join using multiple columns and specifying join type",
    "broadcast join hint using broadcast function",
    "broadcast join hint using Dataset.hint",
    "process outer join results using the non-nullable columns in the join input",
    "SPARK-16991: Full outer join followed by inner join produces wrong results",
    // there is issue when executing this test case with velox backend
    "SPARK-24690 enables star schema detection even if CBO disabled",
    "Supports multi-part names for broadcast hint resolution",
    "SPARK-32693: Compare two dataframes with same schema except nullable property",
    "SPARK-34527: Resolve common columns from USING JOIN",
    "SPARK-39376: Hide duplicated columns in star expansion of subquery alias from USING JOIN",
    "SPARK-17685: WholeStageCodegenExec throws IndexOutOfBoundsException"
  )

  /**
   * re-write the original unit test.
   */
  test(GlutenTestConstants.GLUTEN_TEST + "broadcast join hint using Dataset.hint") {
    // make sure a giant join is not broadcastable
    val plan1 =
      spark.range(10e10.toLong)
        .join(spark.range(10e10.toLong), "id")
        .queryExecution.executedPlan
    assert(plan1.collect { case p: BroadcastHashJoinExec => p }.size == 0)

    // now with a hint it should be broadcasted
    val plan2 =
      spark.range(10e10.toLong)
        .join(spark.range(10e10.toLong).hint("broadcast"), "id")
        .queryExecution.executedPlan
    // Currently, Gluten can not support join hint
    assert(collect(plan2) { case p: ShuffledHashJoinExec => p }.size == 1)
  }
}
