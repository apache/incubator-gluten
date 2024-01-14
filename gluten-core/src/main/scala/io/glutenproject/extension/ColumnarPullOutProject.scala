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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer

trait ProjectProcessedHint
case class PROJECT_PROCESSED() extends ProjectProcessedHint

object ProjectProcessedHint {
  val TAG: TreeNodeTag[ProjectProcessedHint] =
    TreeNodeTag[ProjectProcessedHint]("io.glutenproject.projectprocessedhint")

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isPostProjectProcessed(plan: SparkPlan): Boolean = {
    plan
      .getTagValue(TAG)
      .isDefined && plan.getTagValue(TAG).get.isInstanceOf[PROJECT_PROCESSED]
  }

  def postProjectProcessDone(plan: SparkPlan): Unit = {
    tag(plan, PROJECT_PROCESSED())
  }

  private def tag(plan: SparkPlan, hint: ProjectProcessedHint): Unit = {
    plan.setTagValue(TAG, hint)
  }
}

object ColumnarPullOutProject extends Rule[SparkPlan] with PullOutProjectHelper {

  protected def needsPreProject(plan: GlutenPlan): Boolean = plan match {
    case sort: SortExecTransformer =>
      sort.sortOrder.exists(o => isNotAttribute(o.child))
    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case agg: HashAggregateExecBaseTransformer
        if !ProjectProcessedHint.isPostProjectProcessed(agg) &&
          agg.needsPostProjection =>
      val aggResultAttributeSet = ExpressionSet(agg.allAggregateResultAttributes)
      // Need to remove columns that are not part of the aggregation result attributes.
      // Although this won't affect the result, it may cause some issues with subquery
      // reuse. For example, if a subquery is referenced by both resultExpressions and
      // post-project, it may result in some unit tests failing that verify the number
      // of GlutenPlan.
      val rewrittenResultExpressions =
        agg.resultExpressions.filter(_.find(aggResultAttributeSet.contains).isDefined)
      val projectList = agg.resultExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, other.toString())()
      }

      ProjectProcessedHint.postProjectProcessDone(agg)
      val newAgg = agg.copySelf(resultExpressions = rewrittenResultExpressions)
      newAgg.copyTagsFrom(agg)

      ProjectExecTransformer(projectList, newAgg, projectType = ProjectType.POST)

    case sort: SortExecTransformer
        if !ProjectProcessedHint.isPostProjectProcessed(sort) && needsPreProject(sort) =>
      val projectExprsMap = getProjectExpressionMap
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val newSortOrder =
        sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))
      ProjectProcessedHint.postProjectProcessDone(sort)
      val newSort = sort.copy(
        sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]],
        child = ProjectExecTransformer(
          sort.child.output ++ projectExprsMap.values.toSeq,
          sort.child,
          projectType = ProjectType.PRE)
      )
      newSort.copyTagsFrom(sort)
      ProjectExecTransformer(sort.child.output, newSort, projectType = ProjectType.POST)
  }

  /**
   * This function is used to generate the transformed plan during validation, and it can return the
   * inserted pre-projection, post-projection, as well as the operator transformer. There are four
   * different scenarios in total.
   *   - post-projection -> transformer -> pre-projection
   *   - post-projection -> transformer
   *   - transformer -> pre-projection
   *   - transformer
   */
  def getTransformedPlan(transformer: SparkPlan): Seq[SparkPlan] = {
    val transformedPlan = apply(transformer)
    val allPlan = ListBuffer[SparkPlan](transformedPlan)

    def addPlanIfPreProject(plan: SparkPlan): Unit = plan match {
      case pre: ProjectExecTransformer if pre.isPreProject =>
        allPlan += pre
      case _ =>
    }

    transformedPlan match {
      case p: ProjectExecTransformer if p.isPostProject =>
        val originPlan = p.child
        allPlan += originPlan
        originPlan.children.foreach(addPlanIfPreProject)
      case p: SparkPlan =>
        p.children.foreach(addPlanIfPreProject)
      case _ =>
    }
    allPlan
  }
}
