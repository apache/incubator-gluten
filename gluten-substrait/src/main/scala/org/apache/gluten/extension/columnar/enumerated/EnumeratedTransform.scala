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
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.planner.GlutenOptimization
import org.apache.gluten.planner.cost.GlutenCostModel
import org.apache.gluten.planner.property.Conv
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
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

  private val validator: Validator = Validators
    .builder()
    .fallbackByHint()
    .fallbackIfScanOnly()
    .fallbackComplexExpressions()
    .fallbackByBackendSettings()
    .fallbackByUserOptions()
    .fallbackByTestInjects()
    .build()

  private val rules = List(
    RemoveSort
  )

  // TODO: Should obey ReplaceSingleNode#applyScanNotTransformable to select
  //  (vanilla) scan with cheaper sub-query plan through cost model.
  private val offloadRules =
    Seq(
      RasOffload.from[Exchange](OffloadExchange()),
      RasOffload.from[BaseJoinExec](OffloadJoin()),
      RasOffload.from[FilterExec](OffloadOthers()),
      RasOffload.from[ProjectExec](OffloadOthers()),
      RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()),
      RasOffload.from[DataSourceScanExec](OffloadOthers()),
      RasOffload
        .from(HiveTableScanExecTransformer.isHiveTableScan, OffloadOthers()),
      RasOffload.from[CoalesceExec](OffloadOthers()),
      RasOffload.from[HashAggregateExec](OffloadOthers()),
      RasOffload.from[SortAggregateExec](OffloadOthers()),
      RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
      RasOffload.from[UnionExec](OffloadOthers()),
      RasOffload.from[ExpandExec](OffloadOthers()),
      RasOffload.from[WriteFilesExec](OffloadOthers()),
      RasOffload.from[SortExec](OffloadOthers()),
      RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
      RasOffload.from[WindowExec](OffloadOthers()),
      RasOffload
        .from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec, OffloadOthers()),
      RasOffload.from[LimitExec](OffloadOthers()),
      RasOffload.from[GenerateExec](OffloadOthers()),
      RasOffload.from[EvalPythonExec](OffloadOthers()),
      RasOffload.from[SampleExec](OffloadOthers())
    ).map(RasOffload.Rule(_, validator))

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
