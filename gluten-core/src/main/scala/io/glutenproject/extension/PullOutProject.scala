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

import io.glutenproject.GlutenConfig
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.utils.{LogicalPlanSelector, PullOutProjectHelper}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * This rule will insert a pre-project in the child of operators such as Aggregate, Sort, Join,
 * etc., when they involve expressions that need to be evaluated in advance.
 */
case class PullOutProject(session: SparkSession)
  extends Rule[LogicalPlan]
  with PullOutProjectHelper {

  /**
   * Check if the input logical plan needs to add a pre-project. Different operators have different
   * checking logic.
   */
  private def needsPreProject(plan: LogicalPlan): Boolean = plan match {
    case Aggregate(groupingExpressions, aggregateExpressions, _) =>
      aggNeedPreProject(groupingExpressions, aggregateExpressions)
    case Sort(order, _, _) =>
      order.exists(o => isNotAttribute(o.child))
    case _ => false
  }

  private def transformAgg(agg: Aggregate): LogicalPlan = {
    val projectExprsMap = getProjectExpressionMap

    // Handle groupingExpressions.
    val newGroupingExpressions =
      agg.groupingExpressions.toIndexedSeq.map(getAndReplaceProjectAttribute(_, projectExprsMap))

    // Handle aggregateExpressions
    val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map {
      expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            val newAggFuncChildren = ae.aggregateFunction.children.map {
              case literal: Literal => literal
              case other => getAndReplaceProjectAttribute(other, projectExprsMap)
            }
            val newAggFunc = ae.aggregateFunction
              .withNewChildren(newAggFuncChildren)
              .asInstanceOf[AggregateFunction]
            val newFilter =
              ae.filter.map(getAndReplaceProjectAttribute(_, projectExprsMap))
            ae.copy(aggregateFunction = newAggFunc, filter = newFilter)

          case a: Alias if projectExprsMap.contains(ExpressionEquals(a.child)) =>
            // Keep the name of the original Alias.
            projectExprsMap(ExpressionEquals(a.child)) match {
              case Alias(child, _) =>
                a.withNewChildren(Seq(child))
              case attr: Attribute =>
                a.withNewChildren(Seq(attr))
            }

          case e if projectExprsMap.contains(ExpressionEquals(e)) =>
            projectExprsMap(ExpressionEquals(e)).toAttribute
        }
    }

    agg.copy(
      groupingExpressions = newGroupingExpressions,
      aggregateExpressions = newAggregateExpressions.asInstanceOf[Seq[NamedExpression]],
      child = Project(agg.child.output ++ projectExprsMap.values.toSeq, agg.child)
    )
  }

  override def apply(plan: LogicalPlan): LogicalPlan = LogicalPlanSelector.maybe(session, plan) {
    if (GlutenConfig.getConf.enableAnsiMode) {
      // Gluten not support Ansi Mode, not pull out pre-project
      return plan
    }
    plan.transformUpWithPruning(_.containsAnyPattern(AGGREGATE, FILTER)) {
      case filter: Filter
          if SparkShimLoader.getSparkShims.needsPreProjectForBloomFilterAgg(filter)(
            needsPreProject) =>
        SparkShimLoader.getSparkShims.addPreProjectForBloomFilter(filter)(transformAgg)

      case agg: Aggregate if needsPreProject(agg) =>
        transformAgg(agg)
    }
  }
}
