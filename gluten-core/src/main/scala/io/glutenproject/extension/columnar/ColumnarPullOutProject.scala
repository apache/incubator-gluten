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
package io.glutenproject.extension.columnar

import io.glutenproject.execution._
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

trait ColumnarPullOutProject {
  val applyLocally: PartialFunction[SparkPlan, SparkPlan]

  def getPulledOutPlan[T <: SparkPlan](plan: SparkPlan): T = {
    // Use applyLocally to only transform current plan, not transform its children.
    val pulledOutPlan = applyLocally.lift(plan).getOrElse(plan)
    pulledOutPlan match {
      case p: ProjectExec =>
        p.child.asInstanceOf[T]
      case other =>
        other.asInstanceOf[T]
    }
  }
}

object ColumnarPullOutProject extends Rule[SparkPlan] {

  private lazy val validationRules: List[Rule[SparkPlan] with ColumnarPullOutProject] = List(
    ColumnarPullOutPreProject(validation = true),
    ColumnarPullOutPostProject(validation = true)
  )

  private lazy val rules: List[Rule[SparkPlan]] = List(
    ColumnarPullOutPreProject(),
    ColumnarPullOutPostProject()
  )

  def applyPullOutColumnarPreRules(plan: SparkPlan): SparkPlan = {
    rules.foldLeft(plan) { case (latestPlan, rule) => rule.apply(latestPlan) }
  }

  def getPulledOutPlanLocally[T <: SparkPlan](plan: SparkPlan): T = {
    validationRules
      .foldLeft(plan) {
        case (latestPlan, rule) =>
          rule.getPulledOutPlan[T](latestPlan)
      }
      .asInstanceOf[T]
  }

  def applyPullOutGlutenPlanRules(plan: GlutenPlan): SparkPlan =
    GlutenPlanPullOutProject.apply(plan)

  override def apply(plan: SparkPlan): SparkPlan = applyPullOutColumnarPreRules(plan)
}

case class ColumnarPullOutPostProject(validation: Boolean = false)
  extends Rule[SparkPlan]
  with ColumnarPullOutProject
  with PullOutProjectHelper
  with AliasHelper {

  private def needsPostProjection(plan: SparkPlan): Boolean = {
    // For validation, no need to check the TransformHint in plan.
    if (!validation && !supportTransform(plan)) {
      return false
    }
    plan match {
      case agg: BaseAggregateExec =>
        val pullOutHelper = HashAggregateExecPullOutHelper(agg)
        val allAggregateResultAttributes = pullOutHelper.allAggregateResultAttributes
        // If the result expressions has different size with output attribute,
        // post-projection is needed.
        agg.resultExpressions.size != allAggregateResultAttributes.size ||
        // Compare each item in result expressions and output attributes. Attribute in Alias
        // should be trimmed before checking.
        agg.resultExpressions.map(trimAliases).zip(allAggregateResultAttributes).exists {
          case (exprAttr: Attribute, resAttr) =>
            // If the result attribute and result expression has different name or type,
            // post-projection is needed.
            exprAttr.name != resAttr.name || exprAttr.dataType != resAttr.dataType
          case _ =>
            // If result expression is not instance of Attribute,
            // post-projection is needed.
            true
        }
      case _ => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform(applyLocally)

  override val applyLocally: PartialFunction[SparkPlan, SparkPlan] = {
    case agg: BaseAggregateExec if needsPostProjection(agg) =>
      val projectList = agg.resultExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, s"_post_${generatedNameIndex.getAndIncrement()}")()
      }

      val pullOutHelper = HashAggregateExecPullOutHelper(agg)
      val newResultExpressions = pullOutHelper.allAggregateResultAttributes

      val newAgg = copyBaseAggregateExec(agg, newResultExpressions)
      newAgg.copyTagsFrom(agg)

      val postProject = ProjectExec(projectList, newAgg)
      validatePullOutProject(postProject)
      postProject
  }
}

case class ColumnarPullOutPreProject(validation: Boolean = false)
  extends Rule[SparkPlan]
  with ColumnarPullOutProject
  with PullOutProjectHelper {

  private def needsPreProject(plan: SparkPlan): Boolean = {
    // For validation, no need to check the TransformHint in plan.
    if (!validation && !supportTransform(plan)) {
      return false
    }
    plan match {
      case agg: BaseAggregateExec =>
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

      case sort: SortExec =>
        sort.sortOrder.exists(o => isNotAttribute(o.child))

      case _ => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform(applyLocally)

  override val applyLocally: PartialFunction[SparkPlan, SparkPlan] = {
    case agg: BaseAggregateExec if needsPreProject(agg) =>
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

      val newAgg = copyBaseAggregateExec(agg, newGroupingExpressions, newAggregateExpressions)
      newAgg.copyTagsFrom(agg)

      val preProject = ProjectExec(agg.child.output ++ projectExprsMap.values.toSeq, agg.child)
      validatePullOutProject(preProject)
      newAgg.withNewChildren(Seq(preProject))

    case sort: SortExec if needsPreProject(sort) =>
      val projectExprsMap = getProjectExpressionMap
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val newSortOrder =
        sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))

      val preProject = ProjectExec(sort.child.output ++ projectExprsMap.values.toSeq, sort.child)
      validatePullOutProject(preProject)

      val newSort =
        sort.copy(sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]], child = preProject)
      newSort.copyTagsFrom(sort)

      val postProject = ProjectExec(sort.child.output, newSort)
      validatePullOutProject(postProject)
      postProject
  }
}

/** This rule only used for situation that directly create GlutenPlan. */
object GlutenPlanPullOutProject extends Rule[SparkPlan] with PullOutProjectHelper {
  private def needsPreProject(plan: GlutenPlan): Boolean = plan match {
    case sort: SortExecTransformer =>
      sort.sortOrder.exists(o => isNotAttribute(o.child))
    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case sort: SortExecTransformer if needsPreProject(sort) =>
      val projectExprsMap = getProjectExpressionMap
      val newSortOrder =
        sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))
      val newSort = sort.copy(
        sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]],
        child = ProjectExecTransformer(
          sort.child.output ++ projectExprsMap.values.toSeq,
          sort.child).fallbackIfInvalid
      )
      newSort.copyTagsFrom(sort)
      ProjectExecTransformer(sort.child.output, newSort).fallbackIfInvalid
  }
}
