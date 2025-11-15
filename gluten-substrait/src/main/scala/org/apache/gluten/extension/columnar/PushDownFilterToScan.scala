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
import org.apache.gluten.execution.{BasicScanExecTransformer, BatchScanExecTransformerBase, FileSourceScanExecTransformer, FilterExecTransformerBase, FilterHandler}

import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

/**
 * Vanilla spark just push down part of filter condition into scan, however gluten can push down all
 * filters.
 */
object PushDownFilterToScan extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case filter: FilterExecTransformerBase if filter.enablePushDownFilter =>
      filter.child match {
        case fileScan: FileSourceScanExecTransformer =>
          val pushDownFilters =
            BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
              splitConjunctivePredicates(filter.cond),
              fileScan)
          val newScan = fileScan.copy(dataFilters = pushDownFilters)
          buildNewFilter(newScan, filter)
        case batchScan: BatchScanExecTransformerBase =>
          val pushDownFilters =
            BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
              splitConjunctivePredicates(filter.cond),
              batchScan)
          // If BatchScanExecTransformerBase's parent is filter, pushdownFilters can't be None.
          batchScan.setPushDownFilters(Seq.empty)
          val newScan = batchScan
          if (pushDownFilters.nonEmpty) {
            newScan.setPushDownFilters(pushDownFilters)
            buildNewFilter(newScan, filter)
          } else {
            filter
          }
        case _ => filter
      }
  }

  private def buildNewFilter(
      scan: BasicScanExecTransformer,
      filter: FilterExecTransformerBase): SparkPlan = {
    if (scan.doValidate().ok()) {
      val pushedFilter = scan.filterExprs()
      if (pushedFilter.nonEmpty) {
        // Build new scan with new output based on pushed filters.
        val newScan =
          scan.withNewOutput(FilterExecTransformerBase.buildNewOutput(scan.output, pushedFilter))

        // Build new filter with remaining filters.
        val remainingFilters =
          FilterHandler.getRemainingFilters(pushedFilter, splitConjunctivePredicates(filter.cond))
        val newCond = remainingFilters.reduceLeftOption(And).orNull
        // If all the filters have been pushed down, remove the filter node.
        if (newCond == null) {
          newScan
        } else {
          BackendsApiManager.getSparkPlanExecApiInstance.genFilterExecTransformer(newCond, newScan)
        }
      } else {
        // If no filters have been pushed down, return the original filter node.
        filter
      }
    } else {
      filter
    }
  }
}
