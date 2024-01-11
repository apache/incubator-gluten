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

import io.glutenproject.extension.columnar.TransformHints

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/**
 * [[KnownFloatingPointNormalized]] in Gluten is useless. When converting plans, it directly takes
 * its child. We can reverse the KnownFloatingPointNormalized added by
 * NormalizeFloatingNumbers.normalize in Gluten without affecting the results. Otherwise, pulling
 * out pre-project in the logical plan is not sufficient. The KnownFloatingPointNormalized will be
 * added to the groupingExpressions of the aggregate in the physical plan.
 */
object RemoveKnownFloatingPointNormalized extends Rule[SparkPlan] {

  private def normalizeExpressionExists(expressions: Seq[Expression]): Boolean = {
    expressions.exists(_.find(_.isInstanceOf[KnownFloatingPointNormalized]).isDefined)
  }

  /**
   * If the groupingExpressions of the aggregate are expressions, an Alias will be added on the
   * outside. After removing the KnownFloatingPointNormalized, it is necessary to remove this
   * redundant Alias.
   */
  private def removeRedundantAlias(expression: Expression): Expression = expression match {
    case _ @Alias(child: Attribute, _) => child
    case other => other
  }

  private def supportTransform(plan: SparkPlan): Boolean =
    TransformHints.isAlreadyTagged(plan) && TransformHints.isTransformable(plan)

  override def apply(plan: SparkPlan): SparkPlan = applyWithValidation(plan)

  def applyWithValidation(plan: SparkPlan, validation: Boolean = false): SparkPlan =
    plan.transform {
      // Only remove KnownFloatingPointNormalized when agg is transformable.
      case agg: BaseAggregateExec
          if normalizeExpressionExists(agg.groupingExpressions) &&
            (validation || supportTransform(plan)) =>
        val newGroupingExprs = agg.groupingExpressions.toIndexedSeq.map {
          expr =>
            val transformedExpr = expr.transform {
              case _ @KnownFloatingPointNormalized(_ @NormalizeNaNAndZero(child)) => child
              case _ @KnownFloatingPointNormalized(_ @If(_ @IsNull(expr), _, _)) => expr
              case _ @KnownFloatingPointNormalized(_ @ArrayTransform(expr, _)) => expr
              case other => other
            }
            removeRedundantAlias(transformedExpr)
        }
        agg match {
          case hash: HashAggregateExec =>
            hash.copy(groupingExpressions = newGroupingExprs.asInstanceOf[Seq[NamedExpression]])
          case objectHash: ObjectHashAggregateExec =>
            objectHash.copy(groupingExpressions =
              newGroupingExprs.asInstanceOf[Seq[NamedExpression]])
          case sort: SortAggregateExec =>
            sort.copy(groupingExpressions = newGroupingExprs.asInstanceOf[Seq[NamedExpression]])
          case _ =>
            throw new UnsupportedOperationException(s"Unknown agg $agg")
        }
    }
}
