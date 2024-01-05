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
package org.apache.spark.sql.catalyst

import io.glutenproject.execution.{FlushableHashAggregateExecTransformer, RegularHashAggregateExecTransformer}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

/**
 * To transform regular aggregation to intermediate aggregation that internally enables
 * optimizations such as flushing and abandoning.
 *
 * Currently not in use. Will be enabled via a configuration after necessary verification is done.
 */
case class FlushableHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case shuffle: ShuffleExchangeLike =>
      // If an exchange follows a hash aggregate in which all functions are in partial mode,
      // then it's safe to convert the hash aggregate to intermediate hash aggregate.
      shuffle.child match {
        case h: RegularHashAggregateExecTransformer =>
          if (h.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge)) {
            shuffle.withNewChildren(
              Seq(FlushableHashAggregateExecTransformer(
                h.requiredChildDistributionExpressions,
                h.groupingExpressions,
                h.aggregateExpressions,
                h.aggregateAttributes,
                h.initialInputBufferOffset,
                h.resultExpressions,
                h.child
              )))
          } else {
            shuffle
          }
      }
  }
}
