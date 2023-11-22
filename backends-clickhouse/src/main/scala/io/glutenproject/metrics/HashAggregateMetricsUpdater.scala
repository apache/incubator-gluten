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
package io.glutenproject.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._

class HashAggregateMetricsUpdater(val metrics: Map[String, SQLMetric])
  extends MetricsUpdater
  with Logging {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    try {
      if (opMetrics != null) {
        val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
        if (!operatorMetrics.metricsList.isEmpty && operatorMetrics.aggParams != null) {
          val aggregationParams = operatorMetrics.aggParams
          var currentIdx = operatorMetrics.metricsList.size() - 1
          var totalTime = 0L

          // read rel
          if (aggregationParams.isReadRel) {
            metrics("iterReadTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }

          // pre projection
          if (aggregationParams.preProjectionNeeded) {
            metrics("preProjectTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }

          // aggregating
          val aggMetricsData = operatorMetrics.metricsList.get(currentIdx)
          metrics("aggregatingTime") += (aggMetricsData.time / 1000L).toLong
          metrics("outputVectors") += aggMetricsData.outputVectors
          metrics("inputWaitTime") += (aggMetricsData.inputWaitTime / 1000L).toLong
          metrics("outputWaitTime") += (aggMetricsData.outputWaitTime / 1000L).toLong
          totalTime += aggMetricsData.time

          MetricsUtil.updateExtraTimeMetric(
            aggMetricsData,
            metrics("extraTime"),
            metrics("outputRows"),
            metrics("outputBytes"),
            metrics("inputRows"),
            metrics("inputBytes"),
            HashAggregateMetricsUpdater.INCLUDING_PROCESSORS,
            HashAggregateMetricsUpdater.CH_PLAN_NODE_NAME
          )

          val resizeStep = aggMetricsData.steps.asScala
            .flatMap(_.processors.asScala)
            .find(s => s.getName.equalsIgnoreCase("Resize"))
          if (!resizeStep.isEmpty) {
            metrics("resizeInputRows") += resizeStep.get.inputRows
            metrics("resizeOutputRows") += aggMetricsData.getOutputRows
            // The input rows of the Resize is included in the input rows of the Aggregating
            metrics("inputRows") += -resizeStep.get.inputRows
          }

          currentIdx -= 1

          // post projection
          if (aggregationParams.postProjectionNeeded) {
            metrics("postProjectTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }
          metrics("totalTime") += (totalTime / 1000L).toLong
        }
      }
    } catch {
      case e: Throwable =>
        logError(s"Updating native metrics failed due to ${e.getCause}.")
        throw e
    }
  }
}

object HashAggregateMetricsUpdater {
  val INCLUDING_PROCESSORS = Array("AggregatingTransform", "MergingAggregatedTransform")
  val CH_PLAN_NODE_NAME = Array("AggregatingTransform", "MergingAggregatedTransform")
}
