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
package io.glutenproject.backendsapi

import io.glutenproject.metrics.{IMetrics, MetricsUpdater}
import io.glutenproject.substrait.{AggregationParams, JoinParams}

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SparkPlan

trait MetricsApi extends Serializable {

  def genWholeStageTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

  def writeMetadataUpdatingFunction(child: SparkPlan): Unit

  def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): IMetrics => Unit

  def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genBatchScanTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genFileSourceScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genFileSourceScanTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genFilterTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genProjectTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genCoalesceBatchesMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genHashAggregateTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genHashAggregateTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genColumnarShuffleExchangeMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genSortMergeJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genSortMergeJoinTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genColumnarBroadcastExchangeMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genColumnarSubqueryBroadcastMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]

  def genHashJoinTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater

  def genColumnarInMemoryTableMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
}
