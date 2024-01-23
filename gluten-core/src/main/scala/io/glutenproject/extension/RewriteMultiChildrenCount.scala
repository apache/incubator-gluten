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
package io.glutenproject.extension

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.HashAggregateExecTransformerUtil._
import io.glutenproject.extension.columnar.TransformHints

import org.apache.spark.sql.catalyst.expressions.{If, IsNull, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.types.IntegerType

/**
 * This Rule is used to rewrite the count for aggregates if:
 *   - the count is partial
 *   - the count has more than one child
 *   - the aggregate expressions do not need row construct
 *
 * A rewrite count multi-children example:
 * {{{
 *   Count(c1, c2)
 *   =>
 *   Count(
 *     If(
 *       Or(IsNull(c1), IsNull(c2)),
 *       Literal(null),
 *       Literal(1)
 *     )
 *   )
 * }}}
 *
 * TODO: Remove this rule when Velox support multi-children Count
 */
object RewriteMultiChildrenCount extends Rule[SparkPlan] {
  private def extractCountForRewrite(aggExpr: AggregateExpression): Option[Count] = {
    val isPartialCountWithMoreThanOneChild = aggExpr.mode == Partial && {
      aggExpr.aggregateFunction match {
        case c: Count => c.children.size > 1
        case _ => false
      }
    }
    if (isPartialCountWithMoreThanOneChild) {
      Option(aggExpr.aggregateFunction.asInstanceOf[Count])
    } else {
      None
    }
  }

  private def shouldRewrite(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    // There is a conflict if we would also add a pre-project for row construct
    !rowConstructNeeded(aggregateExpressions) &&
    aggregateExpressions.exists(extractCountForRewrite(_).isDefined)
  }

  private def rewriteCount(
      aggregateExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggregateExpressions.map {
      aggExpr =>
        val countOpt = extractCountForRewrite(aggExpr)
        countOpt
          .map {
            count =>
              // Follow vanillas Spark Count function
              val nullableChildren = count.children.filter(_.nullable)
              val newChild = if (nullableChildren.isEmpty) {
                Literal.create(1, IntegerType)
              } else {
                If(
                  nullableChildren.map(IsNull).reduce(Or),
                  Literal.create(null, IntegerType),
                  Literal.create(1, IntegerType))
              }
              val newCount = count.copy(children = newChild :: Nil)
              aggExpr.copy(aggregateFunction = newCount)
          }
          .getOrElse(aggExpr)
    }
  }

  private def applyInternal[T <: BaseAggregateExec](agg: T): T = {
    val newAggExprs = rewriteCount(agg.aggregateExpressions)
    val newAgg = agg match {
      case a: HashAggregateExec =>
        a.copy(aggregateExpressions = newAggExprs)
      case a: SortAggregateExec =>
        a.copy(aggregateExpressions = newAggExprs)
      case a: ObjectHashAggregateExec =>
        a.copy(aggregateExpressions = newAggExprs)
      case _ => throw new IllegalStateException(s"Unknown aggregate: $agg")
    }
    newAgg.copyTagsFrom(agg)
    newAgg.asInstanceOf[T]
  }

  def applyForValidation[T <: BaseAggregateExec](plan: T): T = {
    if (!BackendsApiManager.getSettings.shouldRewriteCount()) {
      return plan
    }

    plan match {
      case agg: BaseAggregateExec if shouldRewrite(agg.aggregateExpressions) =>
        applyInternal(agg.asInstanceOf[T])
      case _ => plan
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BackendsApiManager.getSettings.shouldRewriteCount()) {
      return plan
    }

    def shouldRewriteAgg(agg: BaseAggregateExec): Boolean = {
      TransformHints.isTransformable(agg) &&
      shouldRewrite(agg.aggregateExpressions)
    }

    plan.transform {
      case agg: BaseAggregateExec if shouldRewriteAgg(agg) =>
        applyInternal(agg)
    }
  }
}
