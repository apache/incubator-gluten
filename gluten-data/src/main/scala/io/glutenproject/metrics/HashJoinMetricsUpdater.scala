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

import io.glutenproject.metrics.Metrics.SingleMetric
import io.glutenproject.substrait.JoinParams
import org.apache.spark.sql.execution.metric.SQLMetric

trait HashJoinMetricsUpdater extends MetricsUpdater {
  def updateJoinMetrics(joinMetrics: java.util.ArrayList[OperatorMetrics],
                        singleMetrics: SingleMetric,
                        joinParams: JoinParams): Unit
}

class HashJoinMetricsUpdaterImpl(
    val metrics: Map[String, SQLMetric]) extends HashJoinMetricsUpdater {
  val hashBuildInputRows: SQLMetric = metrics("hashBuildInputRows")
  val hashBuildOutputRows: SQLMetric = metrics("hashBuildOutputRows")
  val hashBuildOutputVectors: SQLMetric = metrics("hashBuildOutputVectors")
  val hashBuildOutputBytes: SQLMetric = metrics("hashBuildOutputBytes")
  val hashBuildCpuCount: SQLMetric = metrics("hashBuildCpuCount")
  val hashBuildWallNanos: SQLMetric = metrics("hashBuildWallNanos")
  val hashBuildPeakMemoryBytes: SQLMetric = metrics("hashBuildPeakMemoryBytes")
  val hashBuildNumMemoryAllocations: SQLMetric = metrics("hashBuildNumMemoryAllocations")
  val hashBuildSpilledBytes: SQLMetric = metrics("hashBuildSpilledBytes")
  val hashBuildSpilledRows: SQLMetric = metrics("hashBuildSpilledRows")
  val hashBuildSpilledPartitions: SQLMetric = metrics("hashBuildSpilledPartitions")
  val hashBuildSpilledFiles: SQLMetric = metrics("hashBuildSpilledFiles")

  val hashProbeInputRows: SQLMetric = metrics("hashProbeInputRows")
  val hashProbeOutputRows: SQLMetric = metrics("hashProbeOutputRows")
  val hashProbeOutputVectors: SQLMetric = metrics("hashProbeOutputVectors")
  val hashProbeOutputBytes: SQLMetric = metrics("hashProbeOutputBytes")
  val hashProbeCpuCount: SQLMetric = metrics("hashProbeCpuCount")
  val hashProbeWallNanos: SQLMetric = metrics("hashProbeWallNanos")
  val hashProbePeakMemoryBytes: SQLMetric = metrics("hashProbePeakMemoryBytes")
  val hashProbeNumMemoryAllocations: SQLMetric = metrics("hashProbeNumMemoryAllocations")
  val hashProbeSpilledBytes: SQLMetric = metrics("hashProbeSpilledBytes")
  val hashProbeSpilledRows: SQLMetric = metrics("hashProbeSpilledRows")
  val hashProbeSpilledPartitions: SQLMetric = metrics("hashProbeSpilledPartitions")
  val hashProbeSpilledFiles: SQLMetric = metrics("hashProbeSpilledFiles")

  // The number of rows which were passed through without any processing
  // after filter was pushed down.
  val hashProbeReplacedWithDynamicFilterRows: SQLMetric =
  metrics("hashProbeReplacedWithDynamicFilterRows")

  // The number of dynamic filters this join generated for push down.
  val hashProbeDynamicFiltersProduced: SQLMetric =
    metrics("hashProbeDynamicFiltersProduced")

  val streamCpuCount: SQLMetric = metrics("streamCpuCount")
  val streamWallNanos: SQLMetric = metrics("streamWallNanos")
  val streamVeloxToArrow: SQLMetric = metrics("streamVeloxToArrow")

  val streamPreProjectionCpuCount: SQLMetric = metrics("streamPreProjectionCpuCount")
  val streamPreProjectionWallNanos: SQLMetric = metrics("streamPreProjectionWallNanos")

  val buildCpuCount: SQLMetric = metrics("buildCpuCount")
  val buildWallNanos: SQLMetric = metrics("buildWallNanos")

  val buildPreProjectionCpuCount: SQLMetric = metrics("buildPreProjectionCpuCount")
  val buildPreProjectionWallNanos: SQLMetric = metrics("buildPreProjectionWallNanos")

  val postProjectionCpuCount: SQLMetric = metrics("postProjectionCpuCount")
  val postProjectionWallNanos: SQLMetric = metrics("postProjectionWallNanos")
  val postProjectionOutputRows: SQLMetric = metrics("postProjectionOutputRows")
  val postProjectionOutputVectors: SQLMetric = metrics("postProjectionOutputVectors")

  val finalOutputRows: SQLMetric = metrics("finalOutputRows")
  val finalOutputVectors: SQLMetric = metrics("finalOutputVectors")

  override def updateJoinMetrics(joinMetrics: java.util.ArrayList[OperatorMetrics],
                                 singleMetrics: SingleMetric,
                                 joinParams: JoinParams): Unit = {
    var idx = 0
    if (joinParams.postProjectionNeeded) {
      val postProjectMetrics = joinMetrics.get(idx)
      postProjectionCpuCount += postProjectMetrics.cpuCount
      postProjectionWallNanos += postProjectMetrics.wallNanos
      postProjectionOutputRows += postProjectMetrics.outputRows
      postProjectionOutputVectors += postProjectMetrics.outputVectors
      idx += 1
    }

    // HashProbe
    val hashProbeMetrics = joinMetrics.get(idx)
    hashProbeInputRows += hashProbeMetrics.inputRows
    hashProbeOutputRows += hashProbeMetrics.outputRows
    hashProbeOutputVectors += hashProbeMetrics.outputVectors
    hashProbeOutputBytes += hashProbeMetrics.outputBytes
    hashProbeCpuCount += hashProbeMetrics.cpuCount
    hashProbeWallNanos += hashProbeMetrics.wallNanos
    hashProbePeakMemoryBytes += hashProbeMetrics.peakMemoryBytes
    hashProbeNumMemoryAllocations += hashProbeMetrics.numMemoryAllocations
    hashProbeSpilledBytes += hashProbeMetrics.spilledBytes
    hashProbeSpilledRows += hashProbeMetrics.spilledRows
    hashProbeSpilledPartitions += hashProbeMetrics.spilledPartitions
    hashProbeSpilledFiles += hashProbeMetrics.spilledFiles
    hashProbeReplacedWithDynamicFilterRows += hashProbeMetrics.numReplacedWithDynamicFilterRows
    hashProbeDynamicFiltersProduced += hashProbeMetrics.numDynamicFiltersProduced
    idx += 1

    // HashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.inputRows
    hashBuildOutputRows += hashBuildMetrics.outputRows
    hashBuildOutputVectors += hashBuildMetrics.outputVectors
    hashBuildOutputBytes += hashBuildMetrics.outputBytes
    hashBuildCpuCount += hashBuildMetrics.cpuCount
    hashBuildWallNanos += hashBuildMetrics.wallNanos
    hashBuildPeakMemoryBytes += hashBuildMetrics.peakMemoryBytes
    hashBuildNumMemoryAllocations += hashBuildMetrics.numMemoryAllocations
    hashBuildSpilledBytes += hashProbeMetrics.spilledBytes
    hashBuildSpilledRows += hashProbeMetrics.spilledRows
    hashBuildSpilledPartitions += hashProbeMetrics.spilledPartitions
    hashBuildSpilledFiles += hashProbeMetrics.spilledFiles
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      buildPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      buildPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }

    if (joinParams.isBuildReadRel) {
      buildCpuCount += joinMetrics.get(idx).cpuCount
      buildWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      streamPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      streamPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }

    if (joinParams.isStreamedReadRel) {
      val streamMetrics = joinMetrics.get(idx)
      streamCpuCount += streamMetrics.cpuCount
      streamWallNanos += streamMetrics.wallNanos
      streamVeloxToArrow += singleMetrics.veloxToArrow
      idx += 1
    }
  }
}
