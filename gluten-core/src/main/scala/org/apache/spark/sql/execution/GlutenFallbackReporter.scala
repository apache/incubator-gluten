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

package org.apache.spark.sql.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.extension.columnar.{TRANSFORM_UNSUPPORTED, TransformHints}
import io.glutenproject.utils.LogLevelUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This rule is used to collect all fallback reason.
 * 1. print fallback reason for each plan node
 * 2. TODO: post all fallback reason using one event
 */
case class GlutenFallbackReporter(
    glutenConfig: GlutenConfig,
    spark: SparkSession) extends Rule[SparkPlan] with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConfig.enableFallbackReport) {
      return plan
    }
    printFallbackReason(plan)
    plan
  }

  private def printFallbackReason(plan: SparkPlan): Unit = {
    val validateFailureLogLevel = glutenConfig.validateFailureLogLevel
    plan.foreach {
      case _: GlutenPlan => // ignore
      case plan: SparkPlan =>
        TransformHints.getHint(plan) match {
          case TRANSFORM_UNSUPPORTED(reason) =>
            logOnLevel(validateFailureLogLevel,
              s"Validation failed for plan: ${plan.nodeName}, due to: $reason.")
          case _ =>
        }
    }
  }
}
