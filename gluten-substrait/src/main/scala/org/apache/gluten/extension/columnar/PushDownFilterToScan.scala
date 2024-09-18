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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{BatchScanExecTransformerBase, FileSourceScanExecTransformer, FilterExecTransformerBase}

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

/**
 * Vanilla spark just push down part of filter condition into scan, however gluten can push down all
 * filters.
 */
object PushDownFilterToScan extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case filter: FilterExecTransformerBase =>
      filter.child match {
        case fileScan: FileSourceScanExecTransformer =>
          val pushDownFilters =
            BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
              splitConjunctivePredicates(filter.cond),
              fileScan)
          val newScan = fileScan.copy(dataFilters = pushDownFilters)
          if (newScan.doValidate().ok()) {
            filter.withNewChildren(Seq(newScan))
          } else {
            filter
          }
        case batchScan: BatchScanExecTransformerBase =>
          val pushDownFilters =
            BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
              splitConjunctivePredicates(filter.cond),
              batchScan)
          // If BatchScanExecTransformerBase's parent is filter, pushdownFilters can't be None.
          batchScan.setPushDownFilters(Seq.empty)
          val newScan = batchScan
          if (pushDownFilters.size > 0) {
            newScan.setPushDownFilters(pushDownFilters)
            if (newScan.doValidate().ok()) {
              filter.withNewChildren(Seq(newScan))
            } else {
              filter
            }
          } else {
            filter
          }
        case _ => filter
      }
  }
}
