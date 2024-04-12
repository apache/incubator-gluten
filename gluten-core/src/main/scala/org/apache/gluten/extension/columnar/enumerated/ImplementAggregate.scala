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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.TransformHints
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object ImplementAggregate extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
    case plan if TransformHints.isNotTransformable(plan) => List.empty
    case agg: HashAggregateExec => shiftAgg(agg)
    case _ => List.empty
  }

  private def shiftAgg(agg: HashAggregateExec): Iterable[SparkPlan] = {
    List(implement(agg))
  }

  private def implement(agg: HashAggregateExec): SparkPlan = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genHashAggregateExecTransformer(
        agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        agg.initialInputBufferOffset,
        agg.resultExpressions,
        agg.child
      )
  }

  override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}
