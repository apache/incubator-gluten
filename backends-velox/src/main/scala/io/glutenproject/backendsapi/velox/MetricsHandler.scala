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
package io.glutenproject.backendsapi.velox

import io.glutenproject.backendsapi.MetricsApi
import io.glutenproject.metrics._
import io.glutenproject.substrait.{AggregationParams, JoinParams}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.{lang, util}

class MetricsHandler extends MetricsApi with Logging {
  override def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: util.HashMap[lang.Long, util.ArrayList[lang.Long]],
      joinParamsMap: util.HashMap[lang.Long, JoinParams],
      aggParamsMap: util.HashMap[lang.Long, AggregationParams]): IMetrics => Unit = {
    MetricsUtil.updateNativeMetrics(child, relMap, joinParamsMap, aggParamsMap)
  }

  override def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "inputRows" -> SQLMetrics.createMetric(sparkContext, "input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "raw input bytes"),
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "batch scan time"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "dynamic filters accepted"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "processed splits"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "processed row groups")
    )

  override def genBatchScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new BatchScanMetricsUpdater(metrics)

  override def genHiveTableScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "raw input bytes"),
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "scan"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "files size read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "dynamic filters accepted"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "processed splits"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "processed row groups")
    )

  override def genHiveTableScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HiveTableScanMetricsUpdater(metrics)

  override def genFileSourceScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "raw input bytes"),
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "scan"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "files size read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "dynamic filters accepted"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "processed splits"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "processed row groups")
    )

  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new FileSourceScanMetricsUpdater(metrics)

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations")
    )

  override def genFilterTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new FilterMetricsUpdater(metrics)

  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "project"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations")
    )

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new ProjectMetricsUpdater(metrics)

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "aggOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "aggOutputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "aggOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "aggCpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "aggWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "aggregation time"),
      "aggPeakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "aggNumMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations"),
      "aggSpilledBytes" -> SQLMetrics.createMetric(sparkContext, "spilled bytes"),
      "aggSpilledRows" -> SQLMetrics.createMetric(sparkContext, "spilled rows"),
      "aggSpilledPartitions" -> SQLMetrics.createMetric(sparkContext, "spilled partitions"),
      "aggSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "spilled files"),
      "flushRowCount" -> SQLMetrics.createMetric(sparkContext, "flushed rows"),
      "preProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "preProjection cpu wall time count"),
      "preProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "preProjection time"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "postProjection cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "postProjection time"),
      "extractionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "extraction cpu wall time count"),
      "extractionWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "extraction time"),
      "finalOutputRows" -> SQLMetrics.createMetric(sparkContext, "final output rows"),
      "finalOutputVectors" -> SQLMetrics.createMetric(sparkContext, "final output vectors")
    )

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater =
    new HashAggregateMetricsUpdaterImpl(metrics)

  override def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "expand time"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations")
    )

  override def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new ExpandMetricsUpdater(metrics)

  override def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"))

  override def genColumnarShuffleExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
      "splitBufferSize" -> SQLMetrics.createSizeMetric(sparkContext, "split buffer size"),
      "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "split time"),
      "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "spill time"),
      "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "compress time"),
      "prepareTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "prepare time"),
      "decompressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "decompress time"),
      "ipcTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "ipc time"),
      "deserializeTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "deserialize time"),
      "avgReadBatchNumRows" -> SQLMetrics.createAverageMetric(
        sparkContext,
        "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "input rows"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "inputBatches" -> SQLMetrics.createMetric(sparkContext, "input batches")
    )

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "window time"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations")
    )

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new WindowMetricsUpdater(metrics)

  override def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "c2r convert time")
    )

  override def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "input rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "r2c convert time")
    )

  override def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "limit time"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations")
    )

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new LimitMetricsUpdater(metrics)

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "outputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "sort time"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "memory allocations"),
      "spilledBytes" -> SQLMetrics.createMetric(sparkContext, "spilled bytes"),
      "spilledRows" -> SQLMetrics.createMetric(sparkContext, "spilled rows"),
      "spilledPartitions" -> SQLMetrics.createMetric(sparkContext, "spilled partitions"),
      "spilledFiles" -> SQLMetrics.createMetric(sparkContext, "spilled files")
    )

  override def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new SortMetricsUpdater(metrics)

  override def genSortMergeJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output batches"),
      "prepareTime" -> SQLMetrics.createTimingMetric(sparkContext, "prepare time"),
      "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "process time"),
      "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "merge join time"),
      "totaltimeSortmergejoin" -> SQLMetrics
        .createTimingMetric(sparkContext, "sort merge join time")
    )

  override def genSortMergeJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new SortMergeJoinMetricsUpdater(metrics)

  override def genColumnarBroadcastExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "output rows"),
      "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "collect time"),
      "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "broadcast time")
    )

  override def genColumnarSubqueryBroadcastMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size(bytes)"),
      "collectTime" -> SQLMetrics.createMetric(sparkContext, "collect time(ms)"))

  override def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "hashBuildInputRows" -> SQLMetrics.createMetric(sparkContext, "hash build input rows"),
      "hashBuildOutputRows" -> SQLMetrics.createMetric(sparkContext, "hash build output rows"),
      "hashBuildOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "hash build output vectors"),
      "hashBuildOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash build output bytes"),
      "hashBuildCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "hash build cpu wall time count"),
      "hashBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "hash build time"),
      "hashBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash build peak memory bytes"),
      "hashBuildNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "hash build memory allocations"),
      "hashBuildSpilledBytes" -> SQLMetrics.createMetric(sparkContext, "hash build spilled bytes"),
      "hashBuildSpilledRows" -> SQLMetrics.createMetric(sparkContext, "hash build spilled rows"),
      "hashBuildSpilledPartitions" -> SQLMetrics.createMetric(
        sparkContext,
        "hash build spilled partitions"),
      "hashBuildSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "hash build spilled files"),
      "hashProbeInputRows" -> SQLMetrics.createMetric(sparkContext, "hash probe input rows"),
      "hashProbeOutputRows" -> SQLMetrics.createMetric(sparkContext, "hash probe output rows"),
      "hashProbeOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe output vectors"),
      "hashProbeOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash probe output bytes"),
      "hashProbeCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe cpu wall time count"),
      "hashProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "hash probe time"),
      "hashProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash probe peak memory bytes"),
      "hashProbeNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe memory allocations"),
      "hashProbeSpilledBytes" -> SQLMetrics.createMetric(sparkContext, "hash probe spilled bytes"),
      "hashProbeSpilledRows" -> SQLMetrics.createMetric(sparkContext, "hash probe spilled rows"),
      "hashProbeSpilledPartitions" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe spilled partitions"),
      "hashProbeSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "hash probe spilled files"),
      "hashProbeReplacedWithDynamicFilterRows" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe replaced with dynamic filter rows"),
      "hashProbeDynamicFiltersProduced" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe dynamic filters produced"),
      "streamCpuCount" -> SQLMetrics.createMetric(sparkContext, "stream input cpu wall time count"),
      "streamWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "stream input time"),
      "streamVeloxToArrow" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "velox2arrow converter time"),
      "streamPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "stream preProject cpu wall time count"),
      "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "stream preProjection"),
      "buildCpuCount" -> SQLMetrics.createMetric(sparkContext, "build input cpu wall time count"),
      "buildWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "build input time"),
      "buildPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "preProject cpu wall time count"),
      "buildPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "build preProjection"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "postProject cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "postProjection"),
      "postProjectionOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "postProjection output rows"),
      "postProjectionOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "postProjection output vectors"),
      "finalOutputRows" -> SQLMetrics.createMetric(sparkContext, "final output rows"),
      "finalOutputVectors" -> SQLMetrics.createMetric(sparkContext, "final output vectors")
    )

  override def genHashJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HashJoinMetricsUpdaterImpl(metrics)
}
