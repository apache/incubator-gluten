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

package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.GlutenPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}

abstract class GlutenColumnarToRowExecBase(child: SparkPlan) extends ColumnarToRowTransition {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genColumnarToRowMetrics(sparkContext)

  def doValidate(): Boolean = {
    try {
      if (!child.isInstanceOf[GlutenPlan]) {
        return false
      }
      buildCheck()
    } catch {
      case _: Throwable =>
        logInfo("NativeColumnarToRow : Falling back to ColumnarToRow...")
        return false
    }
    true
  }

  def buildCheck(): Unit

  def doExecuteInternal(): RDD[InternalRow]

  override def doExecute(): RDD[InternalRow] = {
    doExecuteInternal()
  }
}

