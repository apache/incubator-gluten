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

import org.apache.gluten.execution.ColumnarCollectLimitExecBaseTransformer

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row}

class GlutenSQLCollectLimitExecSuite extends GlutenSQLTestsTrait {

  private def assertGlutenOperatorMatch[T: reflect.ClassTag](df: DataFrame): Unit = {
    val executedPlan = getExecutedPlan(df)

    val operatorFound = executedPlan.exists {
      plan =>
        try {
          implicitly[reflect.ClassTag[T]].runtimeClass.isInstance(plan)
        } catch {
          case _: Throwable => false
        }
    }

    assert(
      operatorFound,
      s"Operator ${implicitly[reflect.ClassTag[T]].runtimeClass.getSimpleName} not found " +
        s"in executed plan:\n $executedPlan"
    )
  }

  testGluten("ColumnarCollectLimitExec - basic limit test") {
    val df = spark.range(0, 1000, 1).toDF("id").limit(5)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L), Row(3L), Row(4L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](df)
  }

  testGluten("ColumnarCollectLimitExec - with filter") {
    val df = spark
      .range(0, 20, 1)
      .toDF("id")
      .filter("id % 2 == 0")
      .limit(5)
    val expectedData = Seq(Row(0L), Row(2L), Row(4L), Row(6L), Row(8L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](df)
  }

  testGluten("ColumnarCollectLimitExec - range with repartition") {
    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .repartition(3)
      .limit(3)
    val expectedData = Seq(Row(0L), Row(4L), Row(6L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](df)
  }

  testGluten("ColumnarCollectLimitExec - with distinct values") {
    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .select("id")
      .distinct()
      .limit(5)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L), Row(3L), Row(4L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](df)
  }

  testGluten("ColumnarCollectLimitExec - chained limit") {
    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .limit(8)
      .limit(3)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](df)
  }

  testGluten("ColumnarCollectLimitExec - limit after union") {
    val df1 = spark.range(0, 5).toDF("id")
    val df2 = spark.range(5, 10).toDF("id")
    val unionDf = df1.union(df2).limit(3)

    val expectedData = Seq(Row(0L), Row(1L), Row(2L))

    checkAnswer(unionDf, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitExecBaseTransformer](unionDf)
  }
}