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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

abstract class NativeColumnarToRowExec(child: SparkPlan) extends ColumnarToRowTransition
  with CodegenSupport {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
  )
  // A flag used to check whether child is wholestage transformer.
  // Different backends may have different behaviours according to this flag.
  val wsChild = child.isInstanceOf[WholeStageTransformerExec]

  def doValidate(): Boolean = {
    try {
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

