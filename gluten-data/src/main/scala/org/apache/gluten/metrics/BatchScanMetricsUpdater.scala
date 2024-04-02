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
package org.apache.gluten.metrics

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper

class BatchScanMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    inputMetrics.bridgeIncBytesRead(metrics("rawInputBytes").value)
    inputMetrics.bridgeIncRecordsRead(metrics("rawInputRows").value)
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      metrics("numInputRows") += operatorMetrics.inputRows
      metrics("inputVectors") += operatorMetrics.inputVectors
      metrics("inputBytes") += operatorMetrics.inputBytes
      metrics("rawInputRows") += operatorMetrics.rawInputRows
      metrics("rawInputBytes") += operatorMetrics.rawInputBytes
      metrics("numOutputRows") += operatorMetrics.outputRows
      metrics("outputVectors") += operatorMetrics.outputVectors
      metrics("outputBytes") += operatorMetrics.outputBytes
      metrics("cpuCount") += operatorMetrics.cpuCount
      metrics("scanTime") += operatorMetrics.scanTime
      metrics("wallNanos") += operatorMetrics.wallNanos
      metrics("peakMemoryBytes") += operatorMetrics.peakMemoryBytes
      metrics("numMemoryAllocations") += operatorMetrics.numMemoryAllocations
      // Number of dynamic filters received.
      metrics("numDynamicFiltersAccepted") += operatorMetrics.numDynamicFiltersAccepted
      metrics("skippedSplits") += operatorMetrics.skippedSplits
      metrics("processedSplits") += operatorMetrics.processedSplits
      metrics("skippedStrides") += operatorMetrics.skippedStrides
      metrics("processedStrides") += operatorMetrics.processedStrides
      metrics("remainingFilterTime") += operatorMetrics.remainingFilterTime
      metrics("ioWaitTime") += operatorMetrics.ioWaitTime
      metrics("preloadSplits") += operatorMetrics.preloadSplits
    }
  }
}
