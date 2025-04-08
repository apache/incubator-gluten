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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

// Rule1: sum(if(cond, expr, 0)) -> sum(expr) FILTER (WHERE cond)
// Rule2: func(if(cond, expr, null)) -> func(expr) FILTER (WHERE cond)
case class AggregateIfToFilterRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!CHBackendSettings.enableAggregateIfToFilter || !plan.resolved) {
      plan
    } else {
      plan.transform {
        case agg @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
          val newAggregateExpressions = aggregateExpressions.map(transformExpression)
          agg.copy(aggregateExpressions =
            newAggregateExpressions.map(_.asInstanceOf[NamedExpression]))
      }
    }
  }

  private def transformExpression(expr: Expression): Expression = expr match {
    case aggrExpr @ AggregateExpression(aggrFunc, _, isDistinct, filter, _)
        if !isDistinct && filter.isEmpty =>
      aggrFunc match {
        // sum(if(cond, expr, 0)) -> sum(expr) FILTER (WHERE cond)
        case Sum(If(cond, expr, Literal(0, _)), _) =>
          aggrExpr.copy(
            aggregateFunction = Sum(expr),
            filter = Some(cond)
          )
        // func(if(cond, expr, null)) -> func(expr) FILTER (WHERE cond)
        case _ if aggrFunc.children.size == 1 =>
          val expr = aggrFunc.children.head
          expr match {
            case ifExpr @ If(cond, childExpr, Literal(null, _)) =>
              aggrExpr.copy(
                aggregateFunction =
                  aggrFunc.withNewChildren(Seq(childExpr)).asInstanceOf[AggregateFunction],
                filter = Some(cond)
              )
            case _ => aggrExpr
          }
        case _ => aggrExpr
      }
    case aggrExpr @ AggregateExpression(_, _, _, _, _) => aggrExpr
    case e: Expression => e.mapChildren(transformExpression)
  }
}
