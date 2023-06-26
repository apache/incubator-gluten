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

package io.glutenproject.metrics;

public class OperatorMetrics implements IOperatorMetrics {
  public long inputRows;
  public long inputVectors;
  public long inputBytes;
  public long rawInputRows;
  public long rawInputBytes;
  public long outputRows;
  public long outputVectors;
  public long outputBytes;
  public long cpuCount;
  public long wallNanos;
  public long scanTime;
  public long peakMemoryBytes;
  public long numMemoryAllocations;
  public long spilledBytes;
  public long spilledRows;
  public long spilledPartitions;
  public long spilledFiles;
  public long numDynamicFiltersProduced;
  public long numDynamicFiltersAccepted;
  public long numReplacedWithDynamicFilterRows;
  public long flushRowCount;
  public long skippedSplits;
  public long processedSplits;
  public long skippedStrides;
  public long processedStrides;

  /**
   * Create an instance for operator metrics.
   */
  public OperatorMetrics(
      long inputRows,
      long inputVectors,
      long inputBytes,
      long rawInputRows,
      long rawInputBytes,
      long outputRows,
      long outputVectors,
      long outputBytes,
      long cpuCount,
      long wallNanos,
      long peakMemoryBytes,
      long numMemoryAllocations,
      long spilledBytes,
      long spilledRows,
      long spilledPartitions,
      long spilledFiles,
      long numDynamicFiltersProduced,
      long numDynamicFiltersAccepted,
      long numReplacedWithDynamicFilterRows,
      long flushRowCount,
      long scanTime,
      long skippedSplits,
      long processedSplits,
      long skippedStrides,
      long processedStrides) {
    this.inputRows = inputRows;
    this.inputVectors = inputVectors;
    this.inputBytes = inputBytes;
    this.rawInputRows = rawInputRows;
    this.rawInputBytes = rawInputBytes;
    this.outputRows = outputRows;
    this.outputVectors = outputVectors;
    this.outputBytes = outputBytes;
    this.cpuCount = cpuCount;
    this.wallNanos = wallNanos;
    this.scanTime = scanTime;
    this.peakMemoryBytes = peakMemoryBytes;
    this.numMemoryAllocations = numMemoryAllocations;
    this.spilledBytes = spilledBytes;
    this.spilledRows = spilledRows;
    this.spilledPartitions = spilledPartitions;
    this.spilledFiles = spilledFiles;
    this.numDynamicFiltersProduced = numDynamicFiltersProduced;
    this.numDynamicFiltersAccepted = numDynamicFiltersAccepted;
    this.numReplacedWithDynamicFilterRows = numReplacedWithDynamicFilterRows;
    this.flushRowCount = flushRowCount;
    this.skippedSplits = skippedSplits;
    this.processedSplits = processedSplits;
    this.skippedStrides = skippedStrides;
    this.processedStrides = processedStrides;
  }

  /**
   * useless getter/setter to make code scan tool happy
   *
   */
  public long getInputRows() {
    return this.inputRows;
  }

  public long getInputVectors() {
    return this.inputVectors;
  }

  public long getInputBytes() {
    return this.inputBytes;
  }

  public long getRawInputRows() {
    return this.rawInputRows;
  }

  public long getRawInputBytes() {
    return this.rawInputBytes;
  }

  public long getOutputRows() {
    return this.outputRows;
  }

  public long getOutputVectors() {
    return this.outputVectors;
  }

  public long getOutputBytes() {
    return this.outputBytes;
  }

  public long getCpuCount() {
    return this.cpuCount;
  }

  public long getWallNanos() {
    return this.wallNanos;
  }

  public long getScaTime() {
    return this.scanTime;
  }

  public long getPeakMemoryBytes() {
    return this.peakMemoryBytes;
  }

  public long getNumMemoryAllocations() {
    return this.numMemoryAllocations;
  }

  public long getSpilledBytes() {
    return this.spilledBytes;
  }

  public long getSpilledRows() {
    return this.spilledRows;
  }

  public long getSpilledPartitions() {
    return this.spilledPartitions;
  }

  public long getSpilledFiles() {
    return this.spilledFiles;
  }

  public long getNumDynamicFiltersProduced() {
    return this.numDynamicFiltersProduced;
  }

  public long getNumDynamicFiltersAccepted() {
    return this.numDynamicFiltersAccepted;
  }

  public long getNumReplacedWithDynamicFilterRows() {
    return this.numReplacedWithDynamicFilterRows;
  }

  public long getFlushRowCount() {
    return this.flushRowCount;
  }

  public long getSkippedSplits() {
    return this.skippedSplits;
  }

  public long getProcessedSplits() {
    return this.processedSplits;
  }

  public long getSkippedStrides() {
    return this.skippedStrides;
  }

  public long getProcessedStrides() {
    return this.processedStrides;
  }
}
