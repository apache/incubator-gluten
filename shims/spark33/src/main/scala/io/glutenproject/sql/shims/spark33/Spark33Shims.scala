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
package io.glutenproject.sql.shims.spark33

import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningSubquery, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

class Spark33Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }

  override def supportAdaptiveWithExchangeConsidered(plan: SparkPlan): Boolean = {
    // TODO migrate dynamic-partition-pruning onto adaptive execution.
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case _: Exchange => true
      case _ => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled &&
      (sanityCheck(plan) &&
        !plan.logicalLink.exists(_.isStreaming) &&
        !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
        plan.children.forall(supportAdaptiveWithExchangeConsidered)))
  }
}
