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

import org.apache.gluten.execution.CHColumnarToCarrierRowExec
import org.apache.gluten.extension.columnar.transition.ColumnarToRowLike

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}

// Remove the topmost columnar-to-row conversion
// Primarily for structured streaming and delta deletion vector
//
// Sometimes, the code uses dataFrame.queryExecution.toRdd as the data source.
//   queryExecution will use columnar-to-row (c2r) and row-to-columnar (r2c)
//   conversions for the next operation.
// This rule aims to eliminate the redundant double conversion.
case class CHRemoveTopmostColumnarToRow(session: SparkSession, isAdaptiveContext: Boolean)
  extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val removeTopmostColumnarToRow = CHRemoveTopmostColumnarToRow.isRemoveTopmostC2R(session)

    if (!removeTopmostColumnarToRow) {
      return plan
    }

    plan match {
      case shuffleExchangeLike @ ColumnarToRowLike(_: ShuffleExchangeLike) =>
        shuffleExchangeLike
      case broadcastExchangeLike @ ColumnarToRowLike(_: BroadcastExchangeLike) =>
        broadcastExchangeLike
      case broadcastQueryStageExec @ ColumnarToRowLike(_: BroadcastQueryStageExec) =>
        broadcastQueryStageExec
      case ColumnarToRowLike(child) => wrapperColumnarRowAdaptor(child)
      case other => other
    }
  }

  private def wrapperColumnarRowAdaptor(plan: SparkPlan): SparkPlan = {
    CHColumnarToCarrierRowExec.enforce(plan)
  }
}

object CHRemoveTopmostColumnarToRow {
  val REMOVE_TOPMOST_COLUMNAR_TO_ROW: String = "gluten.removeTopmostColumnarToRow"

  def isRemoveTopmostC2R(spark: SparkSession): Boolean = {
    Option(spark.sparkContext.getLocalProperty(REMOVE_TOPMOST_COLUMNAR_TO_ROW)).exists(_.toBoolean)
  }

  def setRemoveTopmostC2R(value: Boolean, spark: SparkSession): Unit = {
    spark.sparkContext.setLocalProperty(REMOVE_TOPMOST_COLUMNAR_TO_ROW, value.toString)
  }
}
