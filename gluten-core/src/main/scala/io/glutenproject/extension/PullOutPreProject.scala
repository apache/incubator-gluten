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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

// spotless:off
/**
 * This rule pulls out the pre-project if necessary for operators: Aggregate, Sort.
 * Note that, we do not need to handle Expand. This rule is applied before
 * `RewriteDistinctAggregates`, so when Spark rewrites Aggregate to Expand, the
 * projections should be Attribute or Literal.
 *
 * 1. Example for Aggregate: SELECT SUM(c1 + c2) FROM t GROUP BY c3 + 1
 *
 * Before this rule:
 * {{{
 *   Aggregate([c3 + 1], [sum(c1 + c2) as c])
 *     SCAN t [c1, c2, c3]
 * }}}
 *
 * After this rule:
 * {{{
 *   Aggregate([_pre_0], [sum(_pre_1) as c])
 *     Project([(c3 + 1) as _pre_0, (c1 + c2) as _pre_1])
 *       SCAN t [c1, c2, c3]
 * }}}
 *
 * 2. Example for Sort: SELECT * FROM t ORDER BY c1 + 1
 *
 * Before this rule:
 * {{{
 *   Sort([SortOrder(c1 + 1)])
 *     SCAN t [c1, c2, c3]
 * }}}
 *
 * After this rule:
 * {{{
 *   Project([c1, c2, c3])
 *     Sort([SortOrder(_pre_0)])
 *       Project([(c1 + c2) as _pre_0, c1, c2, c3])
 *         SCAN t [c1, c2, c3]
 * }}}
 */
// spotless:on
case class PullOutPreProject(spark: SparkSession) extends Rule[LogicalPlan] {

  private def isNotAttributeAndLiteral(e: Expression): Boolean = {
    e match {
      case _: Literal => false
      case _: Attribute => false
      case _ => true
    }
  }

  private def shouldAddPreProjectForAgg(agg: Aggregate): Boolean = {
    agg.aggregateExpressions.exists(_.find {
      case ae: AggregateExpression =>
        ae.aggregateFunction.children.exists(isNotAttributeAndLiteral) || ae.filter.exists(
          isNotAttributeAndLiteral)
      case _ => false
    }.isDefined) || agg.groupingExpressions.exists(e => isNotAttributeAndLiteral(e))
  }

  private def shouldAddPreProjectForSort(sort: Sort): Boolean = {
    sort.order.exists(e => isNotAttributeAndLiteral(e.child))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    var generatedNameIndex = 0
    val originToProjectExprs = new mutable.HashMap[Expression, NamedExpression]
    def putAndGetProjectExpr(e: Expression): Expression = {
      e match {
        case l: Literal => l
        case alias: Alias =>
          originToProjectExprs.getOrElseUpdate(alias.child.canonicalized, alias)
        case attr: Attribute =>
          originToProjectExprs.getOrElseUpdate(attr.canonicalized, attr)
        case _ =>
          originToProjectExprs
            .getOrElseUpdate(
              e.canonicalized, {
                val alias = Alias(e, s"_pre_$generatedNameIndex")()
                generatedNameIndex += 1
                alias
              })
            .toAttribute
      }
    }

    def getAndCleanProjectExprs(): Seq[NamedExpression] = {
      val exprs = originToProjectExprs.toMap.values
      originToProjectExprs.clear()
      exprs.toSeq
    }

    plan.resolveOperators {
      case agg: Aggregate if shouldAddPreProjectForAgg(agg) =>
        def replaceAggExpr(expr: Expression): Expression = {
          expr match {
            case ae: AggregateExpression =>
              val newAggFunc = ae.aggregateFunction.withNewChildren(
                ae.aggregateFunction.children.map(putAndGetProjectExpr))
              val newFilter = ae.filter.map(putAndGetProjectExpr)
              ae.withNewChildren(Seq(newAggFunc) ++ newFilter)
            case e if originToProjectExprs.contains(e.canonicalized) =>
              // handle the case the aggregate expr is same with the grouping expr
              originToProjectExprs(e.canonicalized).toAttribute
            case other =>
              other.mapChildren(replaceAggExpr)
          }
        }
        val newGroupingExpressions = agg.groupingExpressions.toIndexedSeq.map(putAndGetProjectExpr)
        val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map(replaceAggExpr)
        val preProjectList = getAndCleanProjectExprs() ++ agg.child.output
        val preProject = Project(preProjectList, agg.child)
        Aggregate(
          newGroupingExpressions,
          newAggregateExpressions.asInstanceOf[Seq[NamedExpression]],
          preProject
        )

      case sort: Sort if shouldAddPreProjectForSort(sort) =>
        val newOrder = sort.order.toIndexedSeq.map(e => e.mapChildren(putAndGetProjectExpr))
        val preProjectList = getAndCleanProjectExprs() ++ sort.child.output
        val preProject = Project(preProjectList, sort.child)
        val newSort = Sort(newOrder.asInstanceOf[Seq[SortOrder]], sort.global, preProject)
        // add back the original output
        Project(sort.child.output, newSort)
    }
  }
}
