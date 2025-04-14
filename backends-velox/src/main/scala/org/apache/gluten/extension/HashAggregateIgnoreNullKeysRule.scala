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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.BaseJoinExec

/**
 * To identify aggregates that the groupby key is used as inner join keys. In this case, we can set
 * ignoreNullKeys to true when convert to velox's AggregateNode.
 */
case class HashAggregateIgnoreNullKeysRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enablePropagateIgnoreNullKeys) {
      return plan
    }
    plan.transformUp {
      case join: BaseJoinExec if join.joinType == Inner =>
        val newLeftChild = setIgnoreKeysIfAggregateOnJoinKeys(join.left, join.leftKeys)
        val newRightChild = setIgnoreKeysIfAggregateOnJoinKeys(join.right, join.rightKeys)
        if (newLeftChild.fastEquals(join.left) && newRightChild.fastEquals(join.right)) {
          join
        } else {
          join.withNewChildren(Seq(newLeftChild, newRightChild))
        }
      case p => p
    }
  }

  private def setIgnoreKeysIfAggregateOnJoinKeys(
      plan: SparkPlan,
      joinKeys: Seq[Expression]): SparkPlan = plan match {
    case agg: HashAggregateExecTransformer =>
      val newChild = setIgnoreKeysIfAggregateOnJoinKeys(agg.child, joinKeys)
      val canIgnoreNullKeysRule = semanticEquals(agg.groupingExpressions, joinKeys)
      agg match {
        case f: FlushableHashAggregateExecTransformer =>
          f.copy(ignoreNullKeys = canIgnoreNullKeysRule, child = newChild)
        case r: RegularHashAggregateExecTransformer =>
          r.copy(ignoreNullKeys = canIgnoreNullKeysRule, child = newChild)
        case _ => agg
      }
    case s: ShuffleQueryStageExec =>
      s.copy(plan = setIgnoreKeysIfAggregateOnJoinKeys(s.plan, joinKeys))
    case p if !canPropagate(p) => p
    case other =>
      other.withNewChildren(
        other.children.map(c => setIgnoreKeysIfAggregateOnJoinKeys(c, joinKeys)))
  }

  private def canPropagate(plan: SparkPlan): Boolean = plan match {
    case _: ProjectExecTransformer => true
    case _: WholeStageTransformer => true
    case _: VeloxResizeBatchesExec => true
    case _: ShuffleExchangeLike => true
    case _: VeloxColumnarToRowExec => true
    case _: SortExecTransformer => true
    case _ => false
  }

  private def semanticEquals(aggExpression: Seq[Expression], joinKeys: Seq[Expression]): Boolean = {
    aggExpression.size == joinKeys.size && aggExpression.zip(joinKeys).forall {
      case (e1: Expression, e2: Expression) => e1.semanticEquals(e2)
    }
  }
}
