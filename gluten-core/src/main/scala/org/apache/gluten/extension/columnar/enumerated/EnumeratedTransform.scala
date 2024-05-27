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

import org.apache.gluten.extension.columnar.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.transition.ConventionReq
import org.apache.gluten.planner.GlutenOptimization
import org.apache.gluten.planner.cost.GlutenCostModel
import org.apache.gluten.planner.property.Conv
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

case class EnumeratedTransform(session: SparkSession, outputsColumnar: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  private val rules = List(
    new PushFilterToScan(RasOffload.validator),
    RemoveFilter
  )

  // TODO: Should obey ReplaceSingleNode#applyScanNotTransformable to select
  //  (vanilla) scan with cheaper sub-query plan through cost model.
  private val offloadRules = List(
    RasOffload.from[Exchange](OffloadExchange()).toRule,
    RasOffload.from[BaseJoinExec](OffloadJoin()).toRule,
    RasOffloadHashAggregate.toRule,
    RasOffloadFilter.toRule,
    RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()).toRule,
    RasOffload.from[DataSourceScanExec](OffloadOthers()).toRule,
    RasOffload
      .from(
        (node: SparkPlan) => HiveTableScanExecTransformer.isHiveTableScan(node),
        OffloadOthers())
      .toRule,
    RasOffload.from[CoalesceExec](OffloadOthers()).toRule,
    RasOffload.from[ProjectExec](OffloadOthers()).toRule,
    RasOffload.from[SortAggregateExec](OffloadOthers()).toRule,
    RasOffload.from[ObjectHashAggregateExec](OffloadOthers()).toRule,
    RasOffload.from[UnionExec](OffloadOthers()).toRule,
    RasOffload.from[ExpandExec](OffloadOthers()).toRule,
    RasOffload.from[WriteFilesExec](OffloadOthers()).toRule,
    RasOffload.from[SortExec](OffloadOthers()).toRule,
    RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()).toRule,
    RasOffload.from[WindowExec](OffloadOthers()).toRule,
    RasOffload
      .from(
        (node: SparkPlan) => SparkShimLoader.getSparkShims.isWindowGroupLimitExec(node),
        OffloadOthers())
      .toRule,
    RasOffload.from[LimitExec](OffloadOthers()).toRule,
    RasOffload.from[GenerateExec](OffloadOthers()).toRule,
    RasOffload.from[EvalPythonExec](OffloadOthers()).toRule
  )

  private val optimization = {
    GlutenOptimization
      .builder()
      .costModel(GlutenCostModel.find())
      .addRules(rules ++ offloadRules)
      .create()
  }

  private val reqConvention = Conv.any

  private val altConventions = {
    val rowBased: Conv = Conv.req(ConventionReq.row)
    val backendBatchBased: Conv = Conv.req(ConventionReq.backendBatch)
    Seq(rowBased, backendBatchBased)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val constraintSet = PropertySet(List(reqConvention))
    val altConstraintSets =
      altConventions.map(altConv => PropertySet(List(altConv)))
    val planner = optimization.newPlanner(plan, constraintSet, altConstraintSets)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {}
