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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

// To simplify `count(distinct a, b, ...) group by x,b,...` to
// `count(distinct a,...) group by x,b,...`. `b` in the aggregate function `count(distinct)` is
// useless.
// We do this for
// 1. There is no need to include grouping keys in arguments of count(distinct)
// 2. It introduces duplicate columns in CH, and CH doesn't support duplicate columns in a block.
// 3. When `reusedExchange` is enabled, it will cause schema mismatch.
class RemoveUselessAttributesInDstinct(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.resolved) {
      visitPlan(plan)
    } else {
      visitPlan(plan)
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val newAggregateExpressions =
          aggregateExpressions.map(
            visitExpression(groupingExpressions, _).asInstanceOf[NamedExpression])
        Aggregate(groupingExpressions, newAggregateExpressions, visitPlan(child))
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
  }

  def visitExpression(groupingExpressions: Seq[Expression], expr: Expression): Expression = {
    expr match {
      case agg: AggregateExpression =>
        if (agg.isDistinct && agg.aggregateFunction.isInstanceOf[Count]) {
          val newChildren = agg.aggregateFunction.children.filterNot {
            child => groupingExpressions.contains(child)
          }
          // Cannot remove all children in count(distinct).
          if (newChildren.isEmpty) {
            agg
          } else {
            val newCount = Count(newChildren)
            agg.copy(aggregateFunction = newCount)
          }
        } else {
          agg
        }
      case fun: UnresolvedFunction =>
        if (fun.nameParts.mkString(".") == "count" && fun.isDistinct) {
          val newArguemtns = fun.arguments.filterNot(arg => groupingExpressions.contains(arg))
          if (newArguemtns.isEmpty) {
            fun
          } else {
            fun.copy(arguments = newArguemtns)
          }
        } else {
          fun
        }
      case other =>
        other.withNewChildren(other.children.map(visitExpression(groupingExpressions, _)))
    }
  }
}
