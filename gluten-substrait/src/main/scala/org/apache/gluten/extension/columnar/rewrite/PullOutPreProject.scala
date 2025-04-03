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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Partial}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, TypedAggregateExpression}
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}

import scala.collection.mutable

/**
 * The native engine only supports executing Expressions within the project operator. When there are
 * Expressions within physical operators such as SortExec and HashAggregateExec in SparkPlan, we
 * need to pull out the Expressions to be executed earlier in the pre-project operator. This rule is
 * to transform the SparkPlan at the physical plan level, constructing a SparkPlan that supports
 * execution by the native engine.
 */
object PullOutPreProject extends RewriteSingleNode with PullOutProjectHelper {
  override def isRewritable(plan: SparkPlan): Boolean = {
    RewriteEligibility.isRewritable(plan)
  }

  private def needsPreProject(plan: SparkPlan): Boolean = {
    plan match {
      case sort: SortExec =>
        sort.sortOrder.exists(o => isNotAttribute(o.child))
      case take: TakeOrderedAndProjectExec =>
        take.sortOrder.exists(o => isNotAttribute(o.child))
      case agg: BaseAggregateExec =>
        agg.groupingExpressions.exists(isNotAttribute) ||
        agg.aggregateExpressions.exists {
          expr =>
            if (expr.aggregateFunction.isInstanceOf[TypedAggregateExpression]) {
              // We cannot pull out the children of TypedAggregateExpression to pre-project,
              // and Gluten cannot support TypedAggregateExpression.
              false
            } else {
              expr.filter.exists(isNotAttribute) ||
              (expr.mode match {
                case Partial | Complete =>
                  expr.aggregateFunction.children.exists(isNotAttributeAndLiteral)
                case _ => false
              })
            }
        }
      case window: WindowExec =>
        window.orderSpec.exists(o => isNotAttribute(o.child)) ||
        window.partitionSpec.exists(isNotAttribute) ||
        window.windowExpression.exists(_.find {
          case we: WindowExpression =>
            we.windowFunction match {
              case windowFunc: WindowFunction =>
                windowFunc.children.exists(isNotAttributeAndLiteral)
              case ae: AggregateExpression =>
                ae.filter.exists(isNotAttribute) ||
                ae.aggregateFunction.children.exists(isNotAttributeAndLiteral)
              case _ => false
            }
          case _ => false
        }.isDefined) ||
        windowNeedPreComputeRangeFrame(window)
      case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
        val window = SparkShimLoader.getSparkShims
          .getWindowGroupLimitExecShim(plan)
          .asInstanceOf[WindowGroupLimitExecShim]
        window.orderSpec.exists(o => isNotAttribute(o.child)) ||
        window.partitionSpec.exists(isNotAttribute)
      case expand: ExpandExec => expand.projections.flatten.exists(isNotAttributeAndLiteral)
      case _ => false
    }
  }

  /**
   * Pull out Expressions in SortOrder's children, and return the new SortOrder that contains only
   * Attributes.
   */
  private def getNewSortOrder(
      sortOrders: Seq[SortOrder],
      expressionMap: mutable.HashMap[Expression, NamedExpression]): Seq[SortOrder] = {
    sortOrders.map {
      originalOrder =>
        val originalOrderExpressions = mutable.ArrayBuffer[Expression]()
        val newOrder = originalOrder
          .mapChildren {
            child =>
              val newChild = replaceExpressionWithAttribute(child, expressionMap)
              if (!newChild.semanticEquals(child)) {
                // When it is found that a child needs to be pulled out, the original child will
                // be added to the sameOrderExpressions. This ensures that the correct sort orders
                // can be obtained when retrieving the post-project outputOrdering. Spark will
                // verifying whether the output attributes of the post-project are a subset of
                // the references of sortOrder's children. Please check
                // AliasAwareQueryOutputOrdering.outputOrdering in Spark-3.4.
                originalOrderExpressions += child
              }
              newChild
          }
          .asInstanceOf[SortOrder]
        newOrder.copy(sameOrderExpressions =
          newOrder.sameOrderExpressions ++ originalOrderExpressions)
    }
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case sort: SortExec if needsPreProject(sort) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      val newSortOrder = getNewSortOrder(sort.sortOrder, expressionMap)
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val preProject = ProjectExec(
        eliminateProjectList(sort.child.outputSet, expressionMap.values.toSeq),
        sort.child)
      val newSort = sort.copy(sortOrder = newSortOrder, child = preProject)
      // The pre-project and post-project of SortExec always appear together, so it's more
      // convenient to handle them together. Therefore, SortExec's post-project will no longer
      // be pulled out separately.
      ProjectExec(sort.child.output, newSort)

    case topK: TakeOrderedAndProjectExec if needsPreProject(topK) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      val newSortOrder = getNewSortOrder(topK.sortOrder, expressionMap)
      val preProject = ProjectExec(
        eliminateProjectList(topK.child.outputSet, expressionMap.values.toSeq),
        topK.child)
      topK.copy(sortOrder = newSortOrder, child = preProject)

    case agg: BaseAggregateExec if supportedAggregate(agg) && needsPreProject(agg) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      // Handle groupingExpressions.
      val newGroupingExpressions =
        agg.groupingExpressions.toIndexedSeq.map(
          replaceExpressionWithAttribute(_, expressionMap).asInstanceOf[NamedExpression])

      // Handle aggregateExpressions.
      val newAggregateExpressions =
        agg.aggregateExpressions.toIndexedSeq.map(rewriteAggregateExpression(_, expressionMap))

      val newAgg = copyBaseAggregateExec(agg)(
        newGroupingExpressions = newGroupingExpressions,
        newAggregateExpressions = newAggregateExpressions)
      val preProject = ProjectExec(
        eliminateProjectList(agg.child.outputSet, expressionMap.values.toSeq),
        agg.child)
      newAgg.withNewChildren(Seq(preProject))

    case window: WindowExec if needsPreProject(window) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      // Handle foldable orderSpec and foldable partitionSpec. Spark analyzer rule
      // ExtractWindowExpressions will extract expressions from non-foldable orderSpec and
      // partitionSpec.
      var newOrderSpec = getNewSortOrder(window.orderSpec, expressionMap)
      val newPartitionSpec =
        window.partitionSpec.map(replaceExpressionWithAttribute(_, expressionMap))

      // Handle windowExpressions.
      newOrderSpec = rewriteOrderSpecs(window, newOrderSpec, expressionMap)

      val newWindowExpressions = window.windowExpression.toIndexedSeq.map {
        _.transform {
          case we: WindowExpression => rewriteWindowExpression(we, newOrderSpec, expressionMap)
        }
      }

      val newWindow = window.copy(
        orderSpec = newOrderSpec,
        partitionSpec = newPartitionSpec,
        windowExpression = newWindowExpressions.asInstanceOf[Seq[NamedExpression]],
        child = ProjectExec(
          eliminateProjectList(window.child.outputSet, expressionMap.values.toSeq),
          window.child)
      )

      ProjectExec(window.output, newWindow)

    case plan
        if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) && needsPreProject(plan) =>
      val windowLimit = SparkShimLoader.getSparkShims
        .getWindowGroupLimitExecShim(plan)
        .asInstanceOf[WindowGroupLimitExecShim]
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      // Handle orderSpec.
      val newOrderSpec = getNewSortOrder(windowLimit.orderSpec, expressionMap)

      // Handle partitionSpec.
      val newPartitionSpec =
        windowLimit.partitionSpec.map(replaceExpressionWithAttribute(_, expressionMap))

      val newWindowLimitShim = windowLimit.copy(
        orderSpec = newOrderSpec,
        partitionSpec = newPartitionSpec,
        child = ProjectExec(
          eliminateProjectList(windowLimit.child.outputSet, expressionMap.values.toSeq),
          windowLimit.child)
      )

      val newWindowLimit = SparkShimLoader.getSparkShims
        .getWindowGroupLimitExec(newWindowLimitShim)

      ProjectExec(plan.output, newWindowLimit)

    case expand: ExpandExec if needsPreProject(expand) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      val newProjections =
        expand.projections.map(
          _.map(
            replaceExpressionWithAttribute(
              _,
              expressionMap,
              replaceBoundReference = false,
              replaceLiteral = false)))
      expand.copy(
        projections = newProjections,
        child = ProjectExec(
          eliminateProjectList(expand.child.outputSet, expressionMap.values.toSeq),
          expand.child))

    case generate: GenerateExec =>
      BackendsApiManager.getSparkPlanExecApiInstance.genPreProjectForGenerate(generate)

    case arrowEvalPythonExec: ArrowEvalPythonExec =>
      BackendsApiManager.getSparkPlanExecApiInstance.genPreProjectForArrowEvalPythonExec(
        arrowEvalPythonExec)

    case _ => plan
  }
}
