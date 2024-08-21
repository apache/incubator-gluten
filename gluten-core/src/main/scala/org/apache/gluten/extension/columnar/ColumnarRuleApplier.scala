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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.util.AdaptiveContext
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.execution.SparkPlan

trait ColumnarRuleApplier {
  def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan
}

object ColumnarRuleApplier {
  type ColumnarRuleBuilder = ColumnarRuleCall => Rule[SparkPlan]

  class ColumnarRuleCall(
      val session: SparkSession,
      val ac: AdaptiveContext,
      val outputsColumnar: Boolean) {
    val conf: GlutenConfig = {
      new GlutenConfig(session.sessionState.conf)
    }
  }

  class Executor(phase: String, rules: Seq[Rule[SparkPlan]]) extends RuleExecutor[SparkPlan] {
    private val batch: Batch =
      Batch(s"Columnar (Phase [$phase])", Once, rules.map(r => new LoggedRule(r)): _*)

    // TODO Remove this exclusion then pass Spark's idempotence check.
    override protected val excludedOnceBatches: Set[String] = Set(batch.name)

    override protected def batches: Seq[Batch] = List(batch)
  }

  private class LoggedRule(delegate: Rule[SparkPlan])
    extends Rule[SparkPlan]
    with Logging
    with LogLevelUtil {
    // Columnar plan change logging added since https://github.com/apache/incubator-gluten/pull/456.
    private val transformPlanLogLevel = GlutenConfig.getConf.transformPlanLogLevel
    override val ruleName: String = delegate.ruleName

    private def message(oldPlan: SparkPlan, newPlan: SparkPlan, millisTime: Long): String =
      if (!oldPlan.fastEquals(newPlan)) {
        s"""
           |=== Applying Rule $ruleName took $millisTime ms ===
           |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
      } else { s"Rule $ruleName has no effect, took $millisTime ms." }

    override def apply(plan: SparkPlan): SparkPlan = {
      val (out, millisTime) = GlutenTimeMetric.recordMillisTime(delegate.apply(plan))
      logOnLevel(transformPlanLogLevel, message(plan, out, millisTime))
      out
    }

  }
}
