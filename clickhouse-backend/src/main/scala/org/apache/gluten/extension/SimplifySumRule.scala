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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

// Rule 1: sum(expr / literal) -> sum(expr) / literal
// Rule 2: sum(expr * literal) -> literal * sum(expr)
// Rule 3: sum(literal * expr) -> literal * sum(expr)
case class SimplifySumRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || !CHBackendSettings.enableSimplifySum) {
      plan
    } else {
      plan.transform {
        case agg @ Aggregate(_, aggregateExpressions, _) =>
          val newAggregateExpressions = aggregateExpressions.map(transformExpression)
          agg.copy(aggregateExpressions =
            newAggregateExpressions.map(_.asInstanceOf[NamedExpression]))
      }
    }
  }

  private def transformExpression(expr: Expression): Expression = expr match {
    case aggrExpr @ AggregateExpression(Sum(child, _), mode, isDistinct, filter, resultId)
        if !isDistinct =>
      child match {
        // Rule 1: sum(expr / literal) -> sum(expr) / literal
        case Divide(numerator, denominator @ Literal(_, _), _) =>
          val newSum = aggrExpr.copy(aggregateFunction = Sum(numerator))
          ImplicitTypeCasts.transform(Divide(newSum, denominator))

        // Rule 2: sum(expr * literal) -> literal * sum(expr)
        case Multiply(expr, literal @ Literal(_, _), _) =>
          val newSum = aggrExpr.copy(aggregateFunction = Sum(expr))
          ImplicitTypeCasts.transform(Multiply(literal, newSum))

        // Rule 3: sum(literal * expr) -> literal * sum(expr)
        case Multiply(literal @ Literal(_, _), expr, _) =>
          val newSum = aggrExpr.copy(aggregateFunction = Sum(expr))
          ImplicitTypeCasts.transform(Multiply(literal, newSum))

        case _ => aggrExpr
      }
    case aggrExpr @ AggregateExpression(_, _, _, _, _) => aggrExpr
    case e: Expression => e.mapChildren(transformExpression)
  }
}
