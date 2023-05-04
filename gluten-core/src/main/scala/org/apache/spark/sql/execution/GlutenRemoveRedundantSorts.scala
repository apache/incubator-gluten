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
package org.apache.spark.sql

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.SortExecTransformer
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

/**
 * This rule is used to to remove redundant SortExec or SortExecTransformer, similar to vanilla
 * spark's RemoveRedundantSorts. As the use of hash agg to replace sort agg will remove pre-node
 * SortExec, to avoid the caused potential order issue for latter node, we disable vanilla spark's
 * RemoveRedundantSorts which is applied before columnar rule.
 * See the setting for "spark.sql.execution.removeRedundantSorts" in GlutenPlugin.scala.
 * And instead, we have this similar rule applied after columnar rule's TransformPreOverrides.
 */
object GlutenRemoveRedundantSorts extends Rule[SparkPlan] {
  lazy val forceToUseHashAgg =
    GlutenConfig.getConf.forceToUseHashAgg
  def apply(plan: SparkPlan): SparkPlan = {
    if (!forceToUseHashAgg) {
      plan
    } else {
      removeSorts(plan)
    }
  }

  private def removeSorts(plan: SparkPlan): SparkPlan = plan transform {
    case s@SortExec(orders, _, child, _)
      if SortOrder.orderingSatisfies(child.outputOrdering, orders) &&
          child.outputPartitioning.satisfies(s.requiredChildDistribution.head) =>
      child
    case st@SortExecTransformer(orders, _, child, _)
      if SortOrder.orderingSatisfies(child.outputOrdering, orders) &&
          child.outputPartitioning.satisfies(st.requiredChildDistribution.head) =>
      child
  }
}
