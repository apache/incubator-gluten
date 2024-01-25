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

import io.glutenproject.utils.{PhysicalPlanSelector, PullOutProjectHelper}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}

import scala.collection.mutable

/**
 * After adding a pre-project to the sort, the expressions in sortOrder are replaced with
 * attributes. To ensure that the downstream post-project can obtain the correct outputOrdering, it
 * is necessary to restore the original sortOrder with this tag hint Otherwise, when verifying
 * whether the output attributes of the downstream are a subset of the references of
 * sortOrder.child, it will not pass. Please check AliasAwareQueryOutputOrdering.outputOrdering in
 * Spark-3.4.
 */
object SortOrderHint {
  val TAG: TreeNodeTag[Seq[SortOrder]] =
    TreeNodeTag[Seq[SortOrder]]("io.glutenproject.sortorderhint")

  def getSortOrder(plan: SparkPlan): Option[Seq[SortOrder]] = {
    plan.getTagValue(TAG)
  }

  def tag(plan: SparkPlan, sortOrder: Seq[SortOrder]): Unit = {
    plan.setTagValue(TAG, sortOrder)
  }
}

/**
 * The native engine only supports executing Expressions within the project operator. When there are
 * Expressions within physical operators such as SortExec and HashAggregateExec in SparkPlan, we
 * need to pull out the Expressions to be executed earlier in the pre-project operator. This rule is
 * to transform the SparkPlan at the physical plan level, constructing a SparkPlan that supports
 * execution by the native engine.
 */
case class ColumnarPullOutPreProject(session: SparkSession)
  extends Rule[SparkPlan]
  with PullOutProjectHelper {

  private def needsPreProject(plan: SparkPlan): Boolean = {
    if (notSupportTransform(plan)) {
      // TagBeforeTransformHits may tag the plan as not transformable for some reason,
      // for example, when ansi mode is enabled. In such cases, there is no need to
      // pull out the project.
      return false
    }
    plan match {
      case sort: SortExec =>
        sort.sortOrder.exists(o => isNotAttribute(o.child))
      case take: TakeOrderedAndProjectExec =>
        take.sortOrder.exists(o => isNotAttribute(o.child))
      case _ => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    plan.transform {
      case sort: SortExec if needsPreProject(sort) =>
        val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
        // The output of the sort operator is the same as the output of the child, therefore it
        // is necessary to retain the output columns of the child in the pre-projection, and
        // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
        // post-projection, the additional columns need to be removed, leaving only the original
        // output of the child.
        val newSortOrder =
          sort.sortOrder.map(_.mapChildren(replaceExpressionWithAttribute(_, expressionMap)))

        val preProject = ProjectExec(
          eliminateProjectList(sort.child.output, expressionMap.values.toSeq),
          sort.child)

        val newSort =
          sort.copy(sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]], child = preProject)
        newSort.copyTagsFrom(sort)
        SortOrderHint.tag(newSort, sort.sortOrder)

        // The pre-project and post-project of SortExec always appear together, so it's more
        // convenient to handle them together. Therefore, SortExec's post-project will no longer
        // be pulled out separately.
        ProjectExec(sort.child.output, newSort)

      case topK: TakeOrderedAndProjectExec if needsPreProject(topK) =>
        val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
        val newSortOrder =
          topK.sortOrder.map(_.mapChildren(replaceExpressionWithAttribute(_, expressionMap)))
        val preProject = ProjectExec(
          eliminateProjectList(topK.child.output, expressionMap.values.toSeq),
          topK.child)
        val newTopK =
          topK.copy(sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]], child = preProject)
        newTopK.copyTagsFrom(topK)
        SortOrderHint.tag(newTopK, topK.sortOrder)
        newTopK
    }
  }
}
