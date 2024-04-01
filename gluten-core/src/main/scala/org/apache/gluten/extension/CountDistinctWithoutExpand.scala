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

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, CountDistinct}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE_EXPRESSION

/**
 * By converting Count(with isDistinct=true) to a UDAF called CountDistinct, we can avoid the Expand
 * operator in the physical plan.
 *
 * This rule takes no effect unless spark.gluten.enabled and
 * spark.gluten.sql.countDistinctWithoutExpand are both true
 */
object CountDistinctWithoutExpand extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      GlutenConfig.getConf.enableGluten && GlutenConfig.getConf.enableCountDistinctWithoutExpand
    ) {
      plan.transformAllExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
        case ae: AggregateExpression if ae.isDistinct && ae.aggregateFunction.isInstanceOf[Count] =>
          ae.copy(
            aggregateFunction =
              CountDistinct.apply(ae.aggregateFunction.asInstanceOf[Count].children),
            isDistinct = false)
      }
    } else {
      plan
    }
  }
}
