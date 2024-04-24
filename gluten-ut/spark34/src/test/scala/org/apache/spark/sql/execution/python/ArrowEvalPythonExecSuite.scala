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
package org.apache.spark.sql.execution.python

import org.apache.spark.SparkConf
import org.apache.spark.api.python.ColumnarArrowEvalPythonExec
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.execution.SparkPlan

import scala.reflect.ClassTag

class ArrowEvalPythonExecSuite extends QueryTest with GlutenSQLTestsTrait {

  import IntegratedUDFTestUtils._
  import testImplicits.localSeqToDatasetHolder
  import testImplicits.newProductEncoder

  val pyarrowTestUDF = TestScalarPandasUDF(name = "pyarrowUDF")

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
      .set("spark.executor.cores", "1")
  }

  def checkSparkOperatorMatch[T <: SparkPlan](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => tag.runtimeClass.isInstance(plan)))
  }

  test("arrow_udf test") {
    lazy val base =
      Seq(("1", 1), ("1", 2), ("2", 1), ("2", 2), ("3", 1), ("3", 2), ("0", 1), ("3", 0))
        .toDF("a", "b")
    lazy val expected = Seq(
      ("1", "1"),
      ("1", "1"),
      ("2", "2"),
      ("2", "2"),
      ("3", "3"),
      ("3", "3"),
      ("0", "0"),
      ("3", "3")
    ).toDF("a", "p_a")

    val df2 = base.select("a").withColumn("p_a", pyarrowTestUDF(base("a")))
    checkSparkOperatorMatch[ColumnarArrowEvalPythonExec](df2)
    checkAnswer(df2, expected)
  }
}
