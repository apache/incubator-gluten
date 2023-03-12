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

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper

/**
 * Note: "val metrics" is made transient to avoid sending driver-side metrics to tasks, e.g.
 * "pruning time" from scan.
 */
class FileSourceScanMetricsUpdater(@transient val metrics: Map[String, SQLMetric])
  extends MetricsUpdater {

  val scanTime: SQLMetric = metrics("scanTime")
  val outputRows: SQLMetric = metrics("outputRows")
  val outputVectors: SQLMetric = metrics("outputVectors")
  val extraTime: SQLMetric = metrics("extraTime")
  val extraNullSourceTime: SQLMetric = metrics("extraNullSourceTime")
  val extraExpressionTransformTime: SQLMetric = metrics("extraExpressionTransformTime")
  val extraSourceFromJavaIterTime: SQLMetric = metrics("extraSourceFromJavaIterTime")
  val extraConvertingAggregatedToChunksTransformTime: SQLMetric =
    metrics("extraConvertingAggregatedToChunksTransformTime")
  val extraConvertingAggregatedToChunksSourceTime: SQLMetric =
    metrics("extraConvertingAggregatedToChunksSourceTime")

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    // inputMetrics.bridgeIncBytesRead(metrics("inputBytes").value)
    // inputMetrics.bridgeIncRecordsRead(metrics("inputRows").value)
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      scanTime +=
        (operatorMetrics.metric
          .getOrDefault("SubstraitFileSource_0_elapsed", 0L) / 1000L).toLong
      scanTime +=
        (operatorMetrics.metric
          .getOrDefault("MergeTreeInOrder_0_elapsed", 0L) / 1000L).toLong
      outputRows += operatorMetrics.metric.getOrDefault("outputRows", 0L)
      outputVectors += operatorMetrics.metric.getOrDefault("outputVectors", 0L)
      extraTime +=
        (operatorMetrics.metric
          .getOrDefault("extra_elapsed", 0L) / 1000L).toLong
      extraNullSourceTime +=
        (operatorMetrics.metric
          .getOrDefault("NullSource_elapsed", 0L) / 1000L).toLong
      extraExpressionTransformTime +=
        (operatorMetrics.metric
          .getOrDefault("ExpressionTransform_elapsed", 0L) / 1000L).toLong
      extraSourceFromJavaIterTime +=
        (operatorMetrics.metric
          .getOrDefault("SourceFromJavaIter_elapsed", 0L) / 1000L).toLong
      extraConvertingAggregatedToChunksTransformTime +=
        (operatorMetrics.metric
          .getOrDefault("ConvertingAggregatedToChunksTransform_elapsed", 0L) / 1000L).toLong
      extraConvertingAggregatedToChunksSourceTime +=
        (operatorMetrics.metric
          .getOrDefault("ConvertingAggregatedToChunksSource_elapsed", 0L) / 1000L).toLong
    }
  }
}

object FileSourceScanMetricsUpdater {
  val METRICS_MAP = Map(
    "SubstraitFileSource" -> "scanTime",
    "MergeTreeInOrder" -> "scanTime"
  )
}
