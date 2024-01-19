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

import io.glutenproject.execution._
import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

object ColumnarPullOutPostProject extends Rule[SparkPlan] with PullOutProjectHelper {

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case agg: HashAggregateExecBaseTransformer if agg.needsPostProjection =>
      val projectList = agg.resultExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, s"_post_${generatedNameIndex.getAndIncrement()}")()
      }

      val newAgg = agg.copySelf(resultExpressions = agg.allAggregateResultAttributes)

      ProjectExecTransformer(projectList, newAgg, projectType = ProjectType.POST).fallbackIfInvalid
  }
}

object ColumnarPullOutPreProject extends Rule[SparkPlan] with PullOutProjectHelper {

  private def needsPreProject(plan: GlutenPlan): Boolean = plan match {
    case agg: HashAggregateExecBaseTransformer =>
      agg.groupingExpressions.exists(isNotAttribute) ||
      agg.aggregateExpressions.exists {
        expr =>
          expr.filter.exists(isNotAttribute) ||
          (expr.mode match {
            case Partial =>
              expr.aggregateFunction.children.exists(isNotAttributeAndLiteral)
            case _ => false
          })
      }

    case sort: SortExecTransformer =>
      sort.sortOrder.exists(o => isNotAttribute(o.child))

    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case agg: HashAggregateExecBaseTransformer if needsPreProject(agg) =>
      val projectExprsMap = getProjectExpressionMap

      // Handle groupingExpressions.
      val newGroupingExpressions =
        agg.groupingExpressions.toIndexedSeq.map(
          getAndReplaceProjectAttribute(_, projectExprsMap).asInstanceOf[NamedExpression])

      // Handle aggregateExpressions
      val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map {
        ae =>
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
      }

      agg.copySelf(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions,
        child = ProjectExecTransformer(
          agg.child.output ++ projectExprsMap.values.toSeq,
          agg.child,
          ProjectType.PRE).fallbackIfInvalid
      )

    case sort: SortExecTransformer if needsPreProject(sort) =>
      val projectExprsMap = getProjectExpressionMap
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val newSortOrder =
        sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))

      val newSort = sort.copy(
        sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]],
        child = ProjectExecTransformer(
          sort.child.output ++ projectExprsMap.values.toSeq,
          sort.child,
          projectType = ProjectType.PRE).fallbackIfInvalid
      )

      ProjectExecTransformer(
        sort.child.output,
        newSort,
        projectType = ProjectType.POST).fallbackIfInvalid
  }
}

object ColumnarPullOutProject {

  /** This function is used to generate the transformed plan during validation. */
  def getTransformedPlan(transformer: SparkPlan): SparkPlan = {
    val transformedPlan = ColumnarOverrides.applyExtendedColumnarPreRules(transformer)
    transformedPlan match {
      case p: ProjectExecTransformer if p.isPostProject =>
        p.child
      case p: ProjectExec =>
        // Post-project fallback.
        p.child
      case other =>
        other
    }
  }
}
