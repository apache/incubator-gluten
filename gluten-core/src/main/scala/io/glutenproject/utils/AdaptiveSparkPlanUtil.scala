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

package io.glutenproject.utils

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * Mostly ported from spark source code for checking whether a plan supports adaptive.
 * Since spark-3.2, AQE can work for DPP, so no need to exclude DPP plan in the check.
 * This part of code may need update for supporting higher versions of spark.
 */
object AdaptiveSparkPlanUtil {

  def sanityCheck(plan: SparkPlan): Boolean = plan.logicalLink.isDefined

  def supportAdaptiveWithExchangeConsidered(plan: SparkPlan): Boolean = {
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case _: Exchange => true
      case _ => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled &&
        (sanityCheck(plan) &&
            !plan.logicalLink.exists(_.isStreaming) &&
            plan.children.forall(supportAdaptiveWithExchangeConsidered)))
  }

}
