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

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.{ExpandExec, FileSourceScanExec, FilterExec, GenerateExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec

/**
 * TODO: Remove this then implement API #isRewritable in rewrite rules.
 *
 * Since https://github.com/apache/incubator-gluten/pull/4645
 */
object RewriteEligibility {
  def isRewritable(plan: SparkPlan): Boolean = plan match {
    case _: SortExec => true
    case _: TakeOrderedAndProjectExec => true
    case _: BaseAggregateExec => true
    case _: BaseJoinExec => true
    case _: WindowExec => true
    case _: FilterExec => true
    case _: FileSourceScanExec => true
    case _: ExpandExec => true
    case _: GenerateExec => true
    case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) => true
    case _: ArrowEvalPythonExec => true
    case _ => false
  }

}
