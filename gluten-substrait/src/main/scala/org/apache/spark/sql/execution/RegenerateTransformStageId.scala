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
package org.apache.spark.sql.execution

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.WholeStageTransformer
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.atomic.AtomicInteger

case class RegenerateTransformStageId() extends Rule[SparkPlan] {
  val transformStageCounter: AtomicInteger = new AtomicInteger(0)

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)) {
      updateStageId(plan)
    }
    plan
  }

  private def updateStageId(plan: SparkPlan): Unit = {
    plan match {
      case b: BroadcastQueryStageExec =>
        b.plan match {
          case b: BroadcastExchangeLike => updateStageId(b)
          case _: ReusedExchangeExec =>
          case _ =>
            throw new GlutenException(s"wrong plan for broadcast stage:\n ${plan.treeString}")
        }
      case s: ShuffleQueryStageExec =>
        s.plan match {
          case s: ShuffleExchangeLike => updateStageId(s)
          case _: ReusedExchangeExec =>
          case _ =>
            throw new GlutenException(s"wrong plan for shuffle stage:\n ${plan.treeString}")
        }
      case aqe: AdaptiveSparkPlanExec
          if SparkShimLoader.getSparkShims.isFinalAdaptivePlan(aqe) && aqe.isSubquery =>
        // Only handle aqe when it's subquery. The final aqe plan should not go through this rule.
        updateStageId(aqe.executedPlan)
      case wst: WholeStageTransformer =>
        updateStageId(wst.child)
        wst.transformStageId = transformStageCounter.incrementAndGet()
      case plan =>
        plan.subqueries.foreach(updateStageId)
        plan.children.foreach(updateStageId)
    }
  }
}
