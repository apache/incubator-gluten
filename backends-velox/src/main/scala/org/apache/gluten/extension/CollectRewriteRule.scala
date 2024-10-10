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

import org.apache.gluten.expression.ExpressionMappings
import org.apache.gluten.expression.aggregate.{VeloxCollectList, VeloxCollectSet}
import org.apache.gluten.utils.LogicalPlanSelector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Coalesce, Expression, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE_EXPRESSION, AVERAGE, WINDOW, WINDOW_EXPRESSION}
import org.apache.spark.sql.types.ArrayType

import scala.reflect.{classTag, ClassTag}

/**
 * Velox's collect_list / collect_set use array as intermediate data type so aren't compatible with
 * vanilla Spark. We here replace the two functions with velox_collect_list / velox_collect_set to
 * distinguish.
 */
case class CollectRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  import CollectRewriteRule._
  override def apply(plan: LogicalPlan): LogicalPlan = LogicalPlanSelector.maybe(spark, plan) {
    if (!has[VeloxCollectSet] && has[VeloxCollectList]) {
      return plan
    }
    plan.transformWithPruning(_.containsAnyPattern(AVERAGE, WINDOW)) {
      case agg: Aggregate =>
        agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
          case ToVeloxCollect(newAggFunc) =>
            newAggFunc
        }
      case w: Window =>
        w.transformExpressionsWithPruning(_.containsPattern(WINDOW_EXPRESSION)) {
          case ToVeloxCollect(newAggFunc) =>
            newAggFunc
        }

    }
  }
}

object CollectRewriteRule {
  private def ensureNonNull(expr: Expression): Expression = {
    val out =
      Coalesce(List(expr, Literal.create(Seq.empty, expr.dataType)))
    assert(!out.nullable)
    assert(!out.dataType.asInstanceOf[ArrayType].containsNull)
    out
  }

  private object ToVeloxCollect {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case ae @ AggregateExpression(s: CollectList, _, _, filter, _) if has[VeloxCollectList] =>
        val newFilter = (filter ++ Some(IsNotNull(s.child))).reduceOption(And)
        val newAggExpr =
          ae.copy(aggregateFunction = VeloxCollectList(s.child), filter = newFilter)
        Some(newAggExpr)
      case ae @ AggregateExpression(s: CollectSet, _, _, filter, _) if has[VeloxCollectSet] =>
        val newFilter = (filter ++ Some(IsNotNull(s.child))).reduceOption(And)
        val newAggExpr =
          ae.copy(aggregateFunction = VeloxCollectSet(s.child), filter = newFilter)
        Some(ensureNonNull(newAggExpr))
      case _ => None
    }
  }

  private def has[T <: Expression: ClassTag]: Boolean = {
    val out = ExpressionMappings.expressionsMap.contains(classTag[T].runtimeClass)
    out
  }
}
