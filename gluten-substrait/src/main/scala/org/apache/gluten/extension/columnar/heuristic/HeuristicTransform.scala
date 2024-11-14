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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.columnar.rewrite.RewriteSparkPlanRulesManager
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Rule to offload Spark query plan to Gluten query plan using programed heuristics.
 *
 * The procedure consists of 3 stages:
 *
 *   1. Rewrite 2. Validate 3. Offload
 *
 * In the rewrite stage, planner will try converting the Spark query plan to various forms of
 * possible alternative Spark query plans, then choose the optimal one to send to next stage. During
 * which, the same validation code that is about to be used in stage 2 might be invoked early to
 * predict on the estimate "cost" of an alternative Spark query plan.
 *
 * Once the plan is rewritten, query planner will call native validation code in stage 2 to
 * determine which part of the plan is offload-able or not, then add fallback tags to the
 * non-offload-able parts.
 *
 * In stage 3, query planner will convert the offload-able Spark plan nodes into Gluten plan nodes.
 */
case class HeuristicTransform() extends Rule[SparkPlan] with LogLevelUtil {

  private val validate = AddFallbackTags()
  private val rewrite = RewriteSparkPlanRulesManager(validate)
  private val offload = LegacyOffload()

  override def apply(plan: SparkPlan): SparkPlan = {
    Seq(rewrite, validate, offload).foldLeft(plan) {
      case (plan, stage) =>
        stage(plan)
    }
  }
}
