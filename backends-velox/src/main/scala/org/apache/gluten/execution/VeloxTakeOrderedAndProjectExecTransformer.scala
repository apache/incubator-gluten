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
package org.apache.gluten.execution

import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, SinglePartition}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * For offloading [[TakeOrderedAndProjectExec]] in Velox backend.
 *
 * Since:
 *   - https://github.com/apache/incubator-gluten/pull/630
 *   - https://github.com/apache/incubator-gluten/pull/2153
 *   - https://github.com/apache/incubator-gluten/pull/4607
 */
object VeloxTakeOrderedAndProjectExecTransformer extends TakeOrderedAndProjectExecTransformer {
  // Collapse sort + limit into top-n.
  private def collapse(plan: ProjectExecTransformer): SparkPlan = {
    // This to-top-n optimization assumes exchange operators were already placed in input plan.
    val out = plan.transformUp {
      case p @ LimitTransformer(SortExecTransformer(sortOrder, _, child, _), 0, count) =>
        val global = child.outputPartitioning.satisfies(AllTuples)
        val topN = TopNTransformer(count, sortOrder, global, child)
        if (topN.doValidate().isValid) {
          topN
        } else {
          p
        }
      case other => other
    }
    out
  }

  def offload(from: TakeOrderedAndProjectExec): SparkPlan = {
    val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(from)
    val child = from.child
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, from.sortOrder)

    val shuffleNeeded = !child.outputPartitioning.satisfies(AllTuples)

    if (!shuffleNeeded) {
      // Child has only a single partition. Thus we don't have to
      // build plan with local + global pattern.
      if (orderingSatisfies) {
        val globalLimit = LimitTransformer(child, offset, limit)
        val project = ProjectExecTransformer(from.projectList, globalLimit)
        return collapse(project)
      }
      // Sort needed.
      val globalSort = SortExecTransformer(from.sortOrder, true, child)
      val globalLimit = LimitTransformer(globalSort, offset, limit)
      val project = ProjectExecTransformer(from.projectList, globalLimit)
      return collapse(project)
    }

    // Local top N + global top N.
    val localPlan = if (orderingSatisfies) {
      // We don't need local sort.
      // And do not set offset on local limit.
      val localLimit = LimitTransformer(child, 0, limit)
      localLimit
    } else {
      // Local sort needed.
      val localSort = SortExecTransformer(from.sortOrder, false, child)
      // And do not set offset on local limit.
      val localLimit = LimitTransformer(localSort, 0, limit)
      localLimit
    }

    // Exchange data from local to global.
    val exchange = ColumnarShuffleExchangeExec(
      ShuffleExchangeExec(SinglePartition, localPlan),
      localPlan,
      localPlan.output)

    // Global sort + limit.
    val globalSort = SortExecTransformer(from.sortOrder, false, exchange)
    val globalLimit = LimitTransformer(globalSort, offset, limit)
    val project = ProjectExecTransformer(from.projectList, globalLimit)

    collapse(project)
  }

  def validate(from: TakeOrderedAndProjectExec): ValidationResult = {
    val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(from)
    val child = from.child
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, from.sortOrder)

    val sorted = if (orderingSatisfies) {
      child
    } else {
      // Sort needed.
      val sort = SortExecTransformer(from.sortOrder, false, child)
      val sortValidation = sort.doValidate()
      if (!sortValidation.isValid) {
        return sortValidation
      }
      sort
    }

    val limitPlan = LimitTransformer(sorted, offset, limit)
    val limitValidation = limitPlan.doValidate()

    if (!limitValidation.isValid) {
      return limitValidation
    }

    val project = ProjectExecTransformer(from.projectList, limitPlan)
    val projectValidation = project.doValidate()
    projectValidation
  }
}
