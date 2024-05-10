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
import org.apache.spark.sql.catalyst.expressions.{And, Coalesce, Expression, IsNotNull, Literal, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
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
    val out = plan.transformUp {
      case node =>
        val out = replaceCollectSet(replaceCollectList(node))
        out
    }
    if (out.fastEquals(plan)) {
      return plan
    }
    spark.sessionState.analyzer.checkAnalysis(out)
    out
  }

  private def replaceCollectList(node: LogicalPlan): LogicalPlan = {
    node.transformExpressions {
      case func @ AggregateExpression(l: CollectList, _, _, _, _) if has[VeloxCollectList] =>
        func.copy(VeloxCollectList(l.child))
    }
  }

  private def replaceCollectSet(node: LogicalPlan): LogicalPlan = {
    // 1. Replace null result from VeloxCollectSet with empty array to align with
    //    vanilla Spark.
    // 2. Filter out null inputs from VeloxCollectSet to align with vanilla Spark.
    //
    // Since https://github.com/apache/incubator-gluten/pull/4805
    node match {
      case agg: Aggregate =>
        agg.transformExpressions {
          case ToVeloxCollectSet(newAggFunc) =>
            val out = ensureNonNull(newAggFunc)
            out
        }
      case w: Window =>
        w.transformExpressions {
          case func @ WindowExpression(ToVeloxCollectSet(newAggFunc), _) =>
            val out = ensureNonNull(func.copy(newAggFunc))
            out
        }
      case other => other
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

  private object ToVeloxCollectSet {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case aggFunc @ AggregateExpression(s: CollectSet, _, _, filter, _) if has[VeloxCollectSet] =>
        val newFilter = (filter ++ Some(IsNotNull(s.child))).reduceOption(And)
        val newAggFunc =
          aggFunc.copy(aggregateFunction = VeloxCollectSet(s.child), filter = newFilter)
        Some(newAggFunc)
      case _ => None
    }
  }

  private def has[T <: Expression: ClassTag]: Boolean = {
    if (forceEnabled) {
      return true
    }
    val out = ExpressionMappings.expressionsMap.contains(classTag[T].runtimeClass)
    out
  }

  private var forceEnabled = false

  // Visible for testing.
  def forceEnableAndRun[T](func: => T): T = synchronized {
    assert(!forceEnabled)
    forceEnabled = true
    try {
      func
    } finally {
      forceEnabled = false
    }
  }
}
