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
package org.apache.gluten.extension

import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.CHColumnarToRowExec
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange._

/*
 * CH doesn't support will for duplicate columns in the a block.
 * Most of the cases that introduce duplicate columns are from group by.
 */
case class RemoveDuplicatedColumns(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    visitPlan(plan)
  }

  private def visitPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case c2r @ CHColumnarToRowExec(hashAgg: CHHashAggregateExecTransformer) =>
        // This is a special case. Use the result from aggregation as the input of sink.
        // We need to make the schema same as the input of sink.
        val newChildren = hashAgg.children.map(visitPlan)
        val newHashAgg = uniqueHashAggregateColumns(hashAgg)
          .withNewChildren(newChildren)
          .asInstanceOf[CHHashAggregateExecTransformer]
        if (newHashAgg.resultExpressions.length != hashAgg.resultExpressions.length) {
          val project = ProjectExecTransformer(hashAgg.resultExpressions, newHashAgg)
          c2r.copy(child = project)
        } else {
          c2r.copy(child = newHashAgg)
        }
      case hashAgg: CHHashAggregateExecTransformer =>
        val newChildren = hashAgg.children.map(visitPlan)
        var newHashAgg = uniqueHashAggregateColumns(hashAgg)
        newHashAgg =
          newHashAgg.withNewChildren(newChildren).asInstanceOf[CHHashAggregateExecTransformer]
        newHashAgg.child match {
          case aqeShuffleRead @ AQEShuffleReadExec(
                child @ ShuffleQueryStageExec(
                  id,
                  reusedShuffle @ ReusedExchangeExec(output, shuffle: ColumnarShuffleExchangeExec),
                  canonicalized),
                partitionSpecs) =>
            if (output.length != shuffle.output.length) {
              // reused exchange may remain duplicate columns in the output, even its child has
              // removed the duplicate columns. In design, reused exchange's output could be
              // different from its child, so we cannot use the child's output as the output of the
              // reused exchange directly.
              // TODO: we cannot build a UT for this case.
              val uniqueOutput = uniqueExpressions(output.map(_.asInstanceOf[NamedExpression]))
                .map(_.asInstanceOf[Attribute])
              val newReusedShuffle = ReusedExchangeExec(uniqueOutput, shuffle)
              val newChild = AQEShuffleReadExec(
                ShuffleQueryStageExec(id, newReusedShuffle, canonicalized),
                partitionSpecs)
              newHashAgg.copy(child = newChild)
            } else {
              newHashAgg
            }
          case _ => newHashAgg
        }
      case _ =>
        plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  private def unwrapAliasNamedExpression(e: NamedExpression): NamedExpression = {
    e match {
      case a: Alias =>
        if (a.child.isInstanceOf[NamedExpression]) {
          a.child.asInstanceOf[NamedExpression]
        } else {
          a
        }
      case _ => e
    }
  }
  private def unwrapAliasExpression(e: Expression): Expression = {
    e match {
      case a: Alias =>
        if (a.child.isInstanceOf[Expression]) {
          a.child.asInstanceOf[Expression]
        } else {
          a
        }
      case _ => e
    }
  }

  private def uniqueNamedExpressions(
      groupingExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    var addedExpression = Seq[NamedExpression]()
    groupingExpressions.foreach {
      e =>
        val unwrapped = unwrapAliasNamedExpression(e)
        if (
          !addedExpression.exists(_.semanticEquals(unwrapped)) && !unwrapped.isInstanceOf[Literal]
        ) {
          addedExpression = addedExpression :+ unwrapped
        }
    }
    addedExpression
  }

  private def uniqueExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    var addedExpression = Seq[Expression]()
    expressions.foreach {
      e =>
        val unwrapped = unwrapAliasExpression(e)
        if (
          !addedExpression.exists(_.semanticEquals(unwrapped)) && !unwrapped.isInstanceOf[Literal]
        ) {
          addedExpression = addedExpression :+ unwrapped
        }
    }
    addedExpression
  }

  private def uniqueHashAggregateColumns(
      hashAgg: CHHashAggregateExecTransformer): CHHashAggregateExecTransformer = {
    val newGroupingExpressions = uniqueNamedExpressions(hashAgg.groupingExpressions)
    val newResultExpressions = uniqueNamedExpressions(hashAgg.resultExpressions)
    if (newResultExpressions.length != hashAgg.resultExpressions.length) {
      hashAgg
        .copy(
          groupingExpressions = newGroupingExpressions,
          resultExpressions = newResultExpressions)
    } else {
      hashAgg
    }
  }
}
