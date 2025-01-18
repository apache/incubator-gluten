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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.RangeExecBaseTransformer
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.functions.sum

class GlutenSQLRangeExecSuite extends GlutenSQLTestsTrait {

  private def assertGlutenOperatorMatch[T: reflect.ClassTag](df: DataFrame): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(
      executedPlan.exists(plan => implicitly[reflect.ClassTag[T]].runtimeClass.isInstance(plan)),
      s"Expected operator ${implicitly[reflect.ClassTag[T]].runtimeClass.getSimpleName} not found " +
        s"in executed plan:\n $executedPlan"
    )
  }

  testGluten("ColumnarRangeExec produces correct results") {
    val df = spark.range(0, 10, 1).toDF("id")
    val expectedData = (0L until 10L).map(Row(_)).toSeq

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[RangeExecBaseTransformer](df)
  }

  testGluten("ColumnarRangeExec with step") {
    val df = spark.range(5, 15, 2).toDF("id")
    val expectedData = Seq(5L, 7L, 9L, 11L, 13L).map(Row(_))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[RangeExecBaseTransformer](df)
  }

  testGluten("ColumnarRangeExec with filter") {
    val df = spark.range(0, 20, 1).toDF("id").filter("id % 3 == 0")
    val expectedData = Seq(0L, 3L, 6L, 9L, 12L, 15L, 18L).map(Row(_))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[RangeExecBaseTransformer](df)
  }

  testGluten("ColumnarRangeExec with aggregation") {
    val df = spark.range(1, 6, 1).toDF("id")
    val sumDf = df.agg(sum("id"))
    val expectedData = Seq(Row(15L))

    checkAnswer(sumDf, expectedData)

    assertGlutenOperatorMatch[RangeExecBaseTransformer](df)
  }

  testGluten("ColumnarRangeExec with join") {
    val df1 = spark.range(0, 5, 1).toDF("id1")
    val df2 = spark.range(3, 8, 1).toDF("id2")
    val joinDf = df1.join(df2, df1("id1") === df2("id2"))
    val expectedData = Seq(Row(3L, 3L), Row(4L, 4L))

    checkAnswer(joinDf, expectedData)

    assertGlutenOperatorMatch[RangeExecBaseTransformer](joinDf)
  }
}
