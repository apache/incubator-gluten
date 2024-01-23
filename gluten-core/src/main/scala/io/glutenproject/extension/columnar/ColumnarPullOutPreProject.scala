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
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}

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
        val projectExprsMap = getProjectExpressionMap
        // The output of the sort operator is the same as the output of the child, therefore it
        // is necessary to retain the output columns of the child in the pre-projection, and
        // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
        // post-projection, the additional columns need to be removed, leaving only the original
        // output of the child.
        val newSortOrder =
          sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))

        val preProject = ProjectExec(sort.child.output ++ projectExprsMap.values.toSeq, sort.child)
        ProjectTypeHint.tagPreProject(preProject)

        val newSort =
          sort.copy(sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]], child = preProject)
        newSort.copyTagsFrom(sort)

        // The pre-project and post-project of SortExec always appear together, so it's more
        // convenient to handle them together. Therefore, SortExec's post-project will no longer
        // be pulled out separately.
        ProjectExec(sort.child.output, newSort)

      case take: TakeOrderedAndProjectExec if needsPreProject(take) =>
        val projectExprsMap = getProjectExpressionMap
        val newSortOrder =
          take.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))
        val preProject = ProjectExec(take.child.output ++ projectExprsMap.values.toSeq, take.child)
        ProjectTypeHint.tagPreProject(preProject)
        val newTake =
          take.copy(sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]], child = preProject)
        newTake.copyTagsFrom(take)
        newTake
    }
  }
}
