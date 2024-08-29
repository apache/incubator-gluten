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

import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.{ColumnarRuleBuilder, ColumnarRuleCall}
import org.apache.gluten.extension.columnar.util.AdaptiveContext
import org.apache.gluten.utils.{LogLevelUtil, PhysicalPlanSelector}

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by enumerating on
 * all the possibilities of executable Gluten plans, then choose the best plan among them.
 *
 * NOTE: This is still working in progress. We still have a bunch of heuristic rules in this
 * implementation's rule list. Future work will include removing them from the list then
 * implementing them in EnumeratedTransform.
 */
@Experimental
class EnumeratedApplier(session: SparkSession, ruleBuilders: Seq[ColumnarRuleBuilder])
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {
  // An empirical value.
  private val aqeStackTraceIndex =
    if (scala.util.Properties.releaseVersion.exists(_.startsWith("2.12"))) {
      16
    } else {
      14
    }
  private val adaptiveContext = AdaptiveContext(session, aqeStackTraceIndex)

  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, adaptiveContext, outputsColumnar)
    PhysicalPlanSelector.maybe(session, plan) {
      val finalPlan = maybeAqe {
        apply0(ruleBuilders.map(b => b(call)), plan)
      }
      finalPlan
    }
  }

  private def apply0(rules: Seq[Rule[SparkPlan]], plan: SparkPlan): SparkPlan = {
    val executor = new ColumnarRuleApplier.Executor("ras", rules)
    executor.execute(plan)
  }

  private def maybeAqe[T](f: => T): T = {
    adaptiveContext.setAdaptiveContext()
    try {
      f
    } finally {
      adaptiveContext.resetAdaptiveContext()
    }
  }
}
