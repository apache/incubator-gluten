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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SQLExecution

case class CheckMultiJoinsRule(session: SparkSession) extends Rule[LogicalPlan] with Logging {
  private val CHECKED_TAG = TreeNodeTag[Boolean]("org.apache.gluten.CheckMultiJoinsRule.checked")

  @transient private lazy val glutenConf: GlutenConfig = GlutenConfig.get
  @transient private lazy val physicalJoinOptimize = glutenConf.enablePhysicalJoinOptimize
  @transient private lazy val optimizeLevel: Int = glutenConf.physicalJoinOptimizationThrottle
  @transient private lazy val outputSize: Int = glutenConf.physicalJoinOptimizationOutputSize

  def existsMultiJoins(plan: LogicalPlan, count: Int = 0): Boolean = {
    plan match {
      case _: Join =>
        val newCount = count + 1
        if (newCount >= optimizeLevel) {
          if (plan.output.map(_.dataType.defaultSize).sum == outputSize) {
            return true
          }
        }
        plan.children.exists(existsMultiJoins(_, newCount))
      case _: Project =>
        plan.children.exists(existsMultiJoins(_, count + 1))
      case _: Aggregate | _: Window | _: Repartition | _: RepartitionByExpression |
          _: ScriptTransformation =>
        // These nodes usually introduce a shuffle or break the stage, so we stop counting.
        // We start a new check from 0 for the children stages.
        plan.children.exists(existsMultiJoins(_, 0))
      case other =>
        other.children.exists(existsMultiJoins(_, count))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!physicalJoinOptimize || plan.getTagValue(CHECKED_TAG).getOrElse(false)) {
      return plan
    }

    val jobDescPattern = glutenConf.physicalJoinOptimizationJobDescPattern
    if (jobDescPattern != null && !jobDescPattern.isEmpty) {
      val jobDesc = session.sparkContext.getLocalProperty("spark.job.description")
      if (jobDesc == null || !jobDesc.contains(jobDescPattern)) {
        plan.setTagValue(CHECKED_TAG, true)
        return plan
      }
    }

    val execId = session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (execId != null) {
      // Optimization: If already enabled for this execution, no need to check again
      if (session.sparkContext.getLocalProperty("gluten.rewriteLong.executionId") == execId) {
        plan.setTagValue(CHECKED_TAG, true)
        return plan
      }

      if (existsMultiJoins(plan)) {
        // Set the execution id to enable rewrite long optimization for this query
        session.sparkContext.setLocalProperty("gluten.rewriteLong.executionId", execId)
      }
    }

    plan.setTagValue(CHECKED_TAG, true)
    plan
  }
}
