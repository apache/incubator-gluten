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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.aggregate.VeloxApproximatePercentile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, AGGREGATE_EXPRESSION}

/**
 * Rewrite Spark native ApproximatePercentile to VeloxApproximatePercentile:
 *   - Accuracy argument must be DOUBLE type (Velox requirement)
 *   - Only rewrite when all expressions are resolved
 */
case class ApproxPercentileRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (GlutenConfig.get.glutenPercentileFunctionFallback) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case a: Aggregate =>
        a.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
          case aggExpr @ AggregateExpression(sparkPercentile: ApproximatePercentile, _, _, _, _) =>
            val veloxPercentile = VeloxApproximatePercentile(
              child = sparkPercentile.child,
              percentageExpression = sparkPercentile.percentageExpression,
              accuracyExpression = sparkPercentile.accuracyExpression
            )

            aggExpr.copy(aggregateFunction = veloxPercentile)
        }
    }
  }
}
