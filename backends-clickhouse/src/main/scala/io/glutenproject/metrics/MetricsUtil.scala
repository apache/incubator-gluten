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

import io.glutenproject.execution._
import io.glutenproject.substrait.{AggregationParams, JoinParams}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import java.util

object MetricsUtil extends Logging {

  val COMMON_METRICS = Map(
    "outputRows" -> "outputRows",
    "outputVectors" -> "outputVectors",
    "extra_elapsed" -> "extraTime",
    "NullSource_elapsed" -> "extraNullSourceTime",
    "ExpressionTransform_elapsed" -> "extraExpressionTransformTime",
    "SourceFromJavaIter_elapsed" -> "extraSourceFromJavaIterTime",
    "ConvertingAggregatedToChunksTransform_elapsed" ->
      "extraConvertingAggregatedToChunksTransformTime",
    "ConvertingAggregatedToChunksSource_elapsed" -> "extraConvertingAggregatedToChunksSourceTime"
  )

  /**
   * Update metrics fetched from certain iterator to transformers.
   *
   * @param child
   *   the child spark plan
   * @param relMap
   *   the map between operator index and its rels
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def updateNativeMetrics(
      child: SparkPlan,
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): IMetrics => Unit = {

    var mutList = new util.ArrayList[MetricsUpdater]()
    def treeifyMetricsUpdaters(plan: SparkPlan): Unit = {
      plan match {
        case j: CHShuffledHashJoinExecTransformer =>
          val updater = j.metricsUpdater()
          // Update JoiningTransform
          mutList.add(updater)
          // Update FillingRightJoinSide
          mutList.add(updater)
          if (!j.condition.isEmpty) {
            // Update FilterTransform of the join
            mutList.add(updater)
          }
          treeifyMetricsUpdaters(j.streamedPlan)
        case bhj: CHBroadcastHashJoinExecTransformer =>
          val updater = bhj.metricsUpdater()
          // Update JoiningTransform
          mutList.add(updater)
          if (!bhj.condition.isEmpty) {
            // Update FilterTransform of the join
            mutList.add(updater)
          }
          treeifyMetricsUpdaters(bhj.streamedPlan)
        case b: BasicScanExecTransformer =>
          mutList.add(b.metricsUpdater())
        case f: FilterExecTransformer =>
          mutList.add(f.metricsUpdater())
          f.children.map(treeifyMetricsUpdaters)
        case agg: HashAggregateExecBaseTransformer =>
          mutList.add(agg.metricsUpdater())
          treeifyMetricsUpdaters(agg.child)
        case t: TransformSupport => t.children.map(treeifyMetricsUpdaters)
        case _ =>
      }
    }
    treeifyMetricsUpdaters(child)

    updateTransformerMetrics(mutList, relMap, joinParamsMap, aggParamsMap)
  }

  /** A recursive function updating the metrics of one transformer and its child. */
  def updateTransformerMetrics(
      mutList: util.ArrayList[MetricsUpdater],
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): IMetrics => Unit = {
    imetrics =>
      try {
        val metrics = imetrics.asInstanceOf[NativeMetrics]
        if (metrics.metrics.size() == 0) {
          ()
        } else if (mutList.isEmpty) {
          ()
        } else {
          updateTransformerMetricsInternal(mutList, relMap, metrics, joinParamsMap, aggParamsMap)
        }
      } catch {
        case e: Throwable =>
          logWarning(s"Updating native metrics failed due to ${e.getCause}.")
          ()
      }
  }

  def updateTransformerMetricsInternal(
      mutList: util.ArrayList[MetricsUpdater],
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      metrics: NativeMetrics,
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): Unit = {
    val mutLength = mutList.size()
    for (idx <- 0 until mutLength) {
      val metricsIdx = mutLength - idx - 1
      mutList.get(idx).updateNativeMetrics(metrics.getOperatorMetric("", metricsIdx))
      if (idx == 0) {
        // TODO: currently place the below metrics in the last operator,
        for (key <- MetricsUtil.COMMON_METRICS.keySet) {
          metrics.metrics.remove(key)
        }
      }
    }
  }

  /** Set the metrics for each operator */
  def updateOperatorMetrics(
      metrics: Map[String, SQLMetric],
      metricsMap: Map[String, String],
      operatorMetrics: OperatorMetrics): Unit = {
    for (keyValue <- metricsMap) {
      if (metrics.contains(keyValue._2)) {
        val metricKey = keyValue._1 + "_" + operatorMetrics.currMetricIdx.toString + "_elapsed"
        metrics(keyValue._2) +=
          (operatorMetrics.metric.getOrDefault(metricKey, 0L) / 1000L).toLong
      }
    }
    for (keyValue <- MetricsUtil.COMMON_METRICS) {
      if (metrics.contains(keyValue._2)) {
        val value = if (keyValue._1.endsWith("_elapsed")) {
          (operatorMetrics.metric.getOrDefault(keyValue._1, 0L) / 1000L).toLong
        } else {
          operatorMetrics.metric.getOrDefault(keyValue._1, 0L).toLong
        }
        metrics(keyValue._2) += value
      }
    }
  }

}
