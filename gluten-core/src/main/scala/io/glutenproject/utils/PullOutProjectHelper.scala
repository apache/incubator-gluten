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
package io.glutenproject.utils

import io.glutenproject.execution.ProjectExecTransformer
import io.glutenproject.extension.columnar.{AddTransformHintRule, TransformHints}
import io.glutenproject.extension.columnar.AddTransformHintRule.EncodeTransformableTagImplicits

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait PullOutProjectHelper {

  val generatedNameIndex = new AtomicInteger(0)

  /**
   * Some Expressions support Attribute and Literal when converting them into native plans, such as
   * the child of AggregateFunction.
   */
  protected def isNotAttributeAndLiteral(expression: Expression): Boolean = expression match {
    case _: Attribute | _: Literal => false
    case _ => true
  }

  /** The majority of Expressions only support Attribute when converting them into native plans. */
  protected def isNotAttribute(expression: Expression): Boolean = expression match {
    case _: Attribute => false
    case _ => true
  }

  protected def getProjectExpressionMap = new mutable.HashMap[ExpressionEquals, NamedExpression]()

  protected def getAndReplaceProjectAttribute(
      expr: Expression,
      projectExprsMap: mutable.HashMap[ExpressionEquals, NamedExpression]): Expression =
    expr match {
      case alias: Alias =>
        projectExprsMap.getOrElseUpdate(ExpressionEquals(alias.child), alias).toAttribute
      case attr: Attribute =>
        attr
      case other =>
        projectExprsMap
          .getOrElseUpdate(
            ExpressionEquals(other),
            Alias(other, s"_pre_${generatedNameIndex.getAndIncrement()}")())
          .toAttribute
    }

  protected def supportTransform(plan: SparkPlan): Boolean =
    TransformHints.isAlreadyTagged(plan) && TransformHints.isTransformable(plan)

  protected def supportedAggregate(agg: BaseAggregateExec): Boolean = agg match {
    case _: HashAggregateExec | _: SortAggregateExec | _: ObjectHashAggregateExec => true
    case _ => false
  }

  protected def validatePullOutProject(project: ProjectExec): Unit = {
    if (!AddTransformHintRule().enableColumnarProject) {
      TransformHints.tagNotTransformable(project, "columnar project is disabled")
    } else {
      val transformer = ProjectExecTransformer(project.projectList, project.child)
      TransformHints.tag(project, transformer.doValidate().toTransformHint)
    }
  }

  protected def copyBaseAggregateExec(
      agg: BaseAggregateExec,
      newGroupingExpressions: Seq[NamedExpression],
      newAggregateExpressions: Seq[AggregateExpression]): BaseAggregateExec = {
    copyBaseAggregateExec(
      agg,
      newGroupingExpressions,
      newAggregateExpressions,
      agg.resultExpressions)
  }

  protected def copyBaseAggregateExec(
      agg: BaseAggregateExec,
      newResultExpressions: Seq[NamedExpression]): BaseAggregateExec = {
    copyBaseAggregateExec(
      agg,
      agg.groupingExpressions,
      agg.aggregateExpressions,
      newResultExpressions)
  }

  protected def copyBaseAggregateExec(
      agg: BaseAggregateExec,
      newGroupingExpressions: Seq[NamedExpression],
      newAggregateExpressions: Seq[AggregateExpression],
      newResultExpressions: Seq[NamedExpression]): BaseAggregateExec = agg match {
    case hash: HashAggregateExec =>
      hash.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions,
        resultExpressions = newResultExpressions
      )
    case sort: SortAggregateExec =>
      sort.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions,
        resultExpressions = newResultExpressions
      )
    case objectHash: ObjectHashAggregateExec =>
      objectHash.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions,
        resultExpressions = newResultExpressions
      )
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported agg $agg")
  }
}
