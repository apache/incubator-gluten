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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.backendsapi.MetricsApi
import io.glutenproject.metrics.{ExpandMetricsUpdater, LimitMetricsUpdater, _}
import io.glutenproject.substrait.{AggregationParams, JoinParams}
import io.glutenproject.utils.LogLevelUtil

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.{lang, util}

class CHMetricsApi extends MetricsApi with Logging with LogLevelUtil {
  override def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: util.HashMap[lang.Long, util.ArrayList[lang.Long]],
      joinParamsMap: util.HashMap[lang.Long, JoinParams],
      aggParamsMap: util.HashMap[lang.Long, AggregationParams]): IMetrics => Unit = {
    MetricsUtil.updateNativeMetrics(child, relMap, joinParamsMap, aggParamsMap)
  }

  override def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time")
    )

  override def genBatchScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new BatchScanMetricsUpdater(metrics)

  override def genFileSourceScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time")
    )

  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new FileSourceScanMetricsUpdater(metrics)

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genFilterTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new FilterMetricsUpdater(metrics)

  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new ProjectMetricsUpdater(metrics)

  override def genCoalesceBatchesMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "collectTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "total time to collect batch"),
      "concatTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "total time to coalesce batch"),
      "avgCoalescedNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg coalesced batch num rows")
    )

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater =
    new HashAggregateMetricsUpdater(metrics)

  override def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new ExpandMetricsUpdater(metrics)

  override def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def genColumnarShuffleExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
      "computePidTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "totaltime to compute pid"),
      "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to split"),
      "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to spill"),
      "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to compress"),
      "prepareTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to prepare"),
      "avgReadBatchNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputRows" -> SQLMetrics
        .createMetric(sparkContext, "number of output rows"),
      "inputBatches" -> SQLMetrics
        .createMetric(sparkContext, "number of input batches"),
      "outputBatches" -> SQLMetrics
        .createMetric(sparkContext, "number of output batches")
    )

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new WindowMetricsUpdater(metrics)

  override def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
    )

  override def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time to convert")
    )

  override def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new LimitMetricsUpdater(metrics)

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new SortMetricsUpdater(metrics)

  override def genSortMergeJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genSortMergeJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new SortMergeJoinMetricsUpdater(metrics)

  override def genColumnarBroadcastExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
      "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")
    )

  override def genColumnarSubqueryBroadcastMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
      "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "extraNullSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra null source time"),
      "extraExpressionTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra project time"),
      "extraSourceFromJavaIterTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra read iter time"),
      "extraConvertingAggregatedToChunksTransformTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks transform time"),
      "extraConvertingAggregatedToChunksSourceTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "extra convert chunks source time"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "probe time"),
      "fillingRightJoinSideTime" -> SQLMetrics.createTimingMetric(
        sparkContext,
        "filling right join side time"),
      "conditionTime" -> SQLMetrics.createTimingMetric(sparkContext, "join condition time")
    )

  override def genHashJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HashJoinMetricsUpdater(metrics)
}
