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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.SortUtils

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/** Convert [[SortAggregateExec]] and [[ObjectHashAggregateExec]] to [[HashAggregateExec]]. */
object RewriteAggregate extends RewriteSingleNode {

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case sortAgg: SortAggregateExec if BackendsApiManager.getSettings.replaceSortAggWithHashAgg =>
      HashAggregateExec(
        sortAgg.requiredChildDistributionExpressions,
        sortAgg.isStreaming,
        sortAgg.numShufflePartitions,
        sortAgg.groupingExpressions,
        sortAgg.aggregateExpressions,
        sortAgg.aggregateAttributes,
        sortAgg.initialInputBufferOffset,
        sortAgg.resultExpressions,
        SortUtils.dropPartialSort(sortAgg.child)
      )
    case objectHashAgg: ObjectHashAggregateExec =>
      HashAggregateExec(
        objectHashAgg.requiredChildDistributionExpressions,
        objectHashAgg.isStreaming,
        objectHashAgg.numShufflePartitions,
        objectHashAgg.groupingExpressions,
        objectHashAgg.aggregateExpressions,
        objectHashAgg.aggregateAttributes,
        objectHashAgg.initialInputBufferOffset,
        objectHashAgg.resultExpressions,
        objectHashAgg.child
      )
    case _ => plan
  }
}
