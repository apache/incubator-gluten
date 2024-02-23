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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.aggregate._

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait PullOutProjectHelper {

  private val generatedNameIndex = new AtomicInteger(0)

  protected def generatePreAliasName = s"_pre_${generatedNameIndex.getAndIncrement()}"

  /**
   * The majority of Expressions only support Attribute and BoundReference when converting them into
   * native plans.
   */
  protected def isNotAttribute(expression: Expression): Boolean = expression match {
    case _: Attribute | _: BoundReference => false
    case _ => true
  }

  /**
   * Some Expressions support Attribute, BoundReference and Literal when converting them into native
   * plans, such as the child of AggregateFunction.
   */
  protected def isNotAttributeAndLiteral(expression: Expression): Boolean = expression match {
    case _: Attribute | _: BoundReference | _: Literal => false
    case _ => true
  }

  protected def replaceExpressionWithAttribute(
      expr: Expression,
      projectExprsMap: mutable.HashMap[Expression, NamedExpression]): Expression =
    expr match {
      case alias: Alias =>
        projectExprsMap.getOrElseUpdate(alias.child.canonicalized, alias).toAttribute
      case attr: Attribute => attr
      case e: BoundReference => e
      case other =>
        projectExprsMap
          .getOrElseUpdate(other.canonicalized, Alias(other, generatePreAliasName)())
          .toAttribute
    }

  protected def eliminateProjectList(
      childOutput: AttributeSet,
      appendAttributes: Seq[NamedExpression]): Seq[NamedExpression] = {
    childOutput.toIndexedSeq ++ appendAttributes
      .filter(attr => !childOutput.contains(attr))
      .sortWith(_.exprId.id < _.exprId.id)
  }

  protected def supportedAggregate(agg: BaseAggregateExec): Boolean = agg match {
    case _: HashAggregateExec | _: SortAggregateExec | _: ObjectHashAggregateExec => true
    case _ => false
  }

  protected def copyBaseAggregateExec(agg: BaseAggregateExec)(
      newGroupingExpressions: Seq[NamedExpression] = agg.groupingExpressions,
      newAggregateExpressions: Seq[AggregateExpression] = agg.aggregateExpressions,
      newResultExpressions: Seq[NamedExpression] = agg.resultExpressions
  ): BaseAggregateExec = agg match {
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
