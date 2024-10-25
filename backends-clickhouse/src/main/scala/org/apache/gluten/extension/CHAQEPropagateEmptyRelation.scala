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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ClickHouseBuildSideRelation}

case class CHAQEPropagateEmptyRelation(session: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!(session.conf.get(CHBackendSettings.GLUTEN_AQE_PROPAGATEEMPTY, "true").toBoolean)) {
      plan
    } else {
      plan.transform {
        case bhj @ BroadcastHashJoinExec(_, _, joinType, _, _, left, right, isNullAwareAntiJoin)
            if (joinType == LeftAnti) && isNullAwareAntiJoin =>
          right match {
            case BroadcastQueryStageExec(_, plan: SparkPlan, _) =>
              val columnarBroadcast = plan match {
                case c: ColumnarBroadcastExchangeExec => c
                case ReusedExchangeExec(_, c: ColumnarBroadcastExchangeExec) => c
              }
              val chBuildSideRelation = columnarBroadcast.relationFuture.get().value
              chBuildSideRelation match {
                case c: ClickHouseBuildSideRelation if c.hasNullKeyValues =>
                  LocalTableScanExec(bhj.output, Seq.empty)
                case _ => bhj
              }
            case o => bhj
          }
        case other => other
      }
    }
  }
}
