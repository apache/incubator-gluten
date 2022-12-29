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

import io.glutenproject.execution.WholeStageTransformerExec
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression}
import org.apache.spark.sql.execution.ColumnarShuffleExchangeAdaptor
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{broadcast, lit, when}
import org.apache.spark.sql.internal.SQLConf


class GlutenDataFrameSuite extends DataFrameSuite with GlutenSQLTestsTrait {

  test(GLUTEN_TEST + "reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        collect(join.queryExecution.executedPlan) {
          // replace ShuffleExchangeExec
          case e: ColumnarShuffleExchangeAdaptor => true }.size === 1)
      assert(
        collect(join.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        collect(join2.queryExecution.executedPlan) {
          // replace ShuffleExchangeExec
          case e: ColumnarShuffleExchangeAdaptor => true }.size == 1)
      assert(
        collect(join2.queryExecution.executedPlan) {
          // Gluten reuses operators more than Vanilla Spark
          case e: ReusedExchangeExec => true }.size == 5)
    }
  }

  /**
   * Failed to check WholeStageCodegenExec, so we rewrite the UT.
   */
  test(GLUTEN_TEST + "SPARK-22520: support code generation for large CaseWhen") {
    val N = 30
    var expr1 = when(equalizer($"id", lit(0)), 0)
    var expr2 = when(equalizer($"id", lit(0)), 10)
    (1 to N).foreach { i =>
      expr1 = expr1.when(equalizer($"id", lit(i)), -i)
      expr2 = expr2.when(equalizer($"id", lit(i + 10)), i)
    }
    val df = spark.range(1).select(expr1, expr2.otherwise(0))
    checkAnswer(df, Row(0, 10) :: Nil)
    // We check WholeStageTransformerExec instead of WholeStageCodegenExec
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[WholeStageTransformerExec]).isDefined)
  }

  private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  def equalizer(expr: Expression, other: Any): Column = withExpr {
    val right = lit(other).expr
    if (expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${expr} = $right'. " +
          "Perhaps you need to use aliases.")
    }
    EqualTo(expr, right)
  }
}
