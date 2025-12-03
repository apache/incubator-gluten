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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, Exchange, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.atomic.AtomicInteger

case class RegenerateTransformStageId() extends Rule[SparkPlan] {
  val transformStageCounter: AtomicInteger = new AtomicInteger(0)

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)) {
      regenerateTransformStageId(plan)
    } else {
      plan
    }
  }

  private def regenerateTransformStageId(plan: SparkPlan): SparkPlan = {
    plan match {
      case b: BroadcastQueryStageExec =>
        val newPlan = b.plan match {
          case b: BroadcastExchangeLike => regenerateTransformStageId(b)
          case r @ ReusedExchangeExec(_, b: BroadcastExchangeLike) =>
            r.copy(child = regenerateTransformStageId(b).asInstanceOf[Exchange])
          case _ =>
            throw new GlutenException(s"wrong plan for broadcast stage:\n ${plan.treeString}")
        }
        b.copy(plan = newPlan)
      case s: ShuffleQueryStageExec =>
        val newPlan = s.plan match {
          case s: ShuffleExchangeLike => regenerateTransformStageId(s)
          case r @ ReusedExchangeExec(_, s: ShuffleExchangeLike) =>
            r.copy(child = regenerateTransformStageId(s).asInstanceOf[Exchange])
          case _ =>
            throw new GlutenException(s"wrong plan for broadcast stage:\n ${plan.treeString}")
        }
        s.copy(plan = newPlan)
      case wst: WholeStageTransformer =>
        regenerateTransformStageId(wst.child)
        wst.copy()(transformStageId = transformStageCounter.incrementAndGet())
      case other => other.mapChildren(regenerateTransformStageId)
    }
  }
}
