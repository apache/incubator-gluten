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
package org.apache.gluten.metrics;

import org.apache.gluten.exception.GlutenException;

public class Metrics implements IMetrics {
  public long[] inputRows;
  public long[] inputVectors;
  public long[] inputBytes;
  public long[] rawInputRows;
  public long[] rawInputBytes;
  public long[] outputRows;
  public long[] outputVectors;
  public long[] outputBytes;
  public long[] cpuCount;
  public long[] wallNanos;
  public long[] scanTime;
  public long[] peakMemoryBytes;
  public long[] numMemoryAllocations;
  public long[] spilledBytes;
  public long[] spilledRows;
  public long[] spilledPartitions;
  public long[] spilledFiles;
  public long[] numDynamicFiltersProduced;
  public long[] numDynamicFiltersAccepted;
  public long[] numReplacedWithDynamicFilterRows;
  public long[] flushRowCount;
  public long[] skippedSplits;
  public long[] processedSplits;
  public long[] skippedStrides;
  public long[] processedStrides;
  public long[] remainingFilterTime;
  public long[] ioWaitTime;
  public long[] preloadSplits;

  public long[] physicalWrittenBytes;

  public long[] numWrittenFiles;

  public SingleMetric singleMetric = new SingleMetric();

  /** Create an instance for native metrics. */
  public Metrics(
      long[] inputRows,
      long[] inputVectors,
      long[] inputBytes,
      long[] rawInputRows,
      long[] rawInputBytes,
      long[] outputRows,
      long[] outputVectors,
      long[] outputBytes,
      long[] cpuCount,
      long[] wallNanos,
      long veloxToArrow,
      long[] peakMemoryBytes,
      long[] numMemoryAllocations,
      long[] spilledBytes,
      long[] spilledRows,
      long[] spilledPartitions,
      long[] spilledFiles,
      long[] numDynamicFiltersProduced,
      long[] numDynamicFiltersAccepted,
      long[] numReplacedWithDynamicFilterRows,
      long[] flushRowCount,
      long[] scanTime,
      long[] skippedSplits,
      long[] processedSplits,
      long[] skippedStrides,
      long[] processedStrides,
      long[] remainingFilterTime,
      long[] ioWaitTime,
      long[] preloadSplits,
      long[] physicalWrittenBytes,
      long[] numWrittenFiles) {
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
    this.singleMetric.veloxToArrow = veloxToArrow;
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
    this.remainingFilterTime = remainingFilterTime;
    this.ioWaitTime = ioWaitTime;
    this.preloadSplits = preloadSplits;
    this.physicalWrittenBytes = physicalWrittenBytes;
    this.numWrittenFiles = numWrittenFiles;
  }

  public OperatorMetrics getOperatorMetrics(int index) {
    if (index >= inputRows.length) {
      throw new GlutenException("Invalid index.");
    }

    return new OperatorMetrics(
        inputRows[index],
        inputVectors[index],
        inputBytes[index],
        rawInputRows[index],
        rawInputBytes[index],
        outputRows[index],
        outputVectors[index],
        outputBytes[index],
        cpuCount[index],
        wallNanos[index],
        peakMemoryBytes[index],
        numMemoryAllocations[index],
        spilledBytes[index],
        spilledRows[index],
        spilledPartitions[index],
        spilledFiles[index],
        numDynamicFiltersProduced[index],
        numDynamicFiltersAccepted[index],
        numReplacedWithDynamicFilterRows[index],
        flushRowCount[index],
        scanTime[index],
        skippedSplits[index],
        processedSplits[index],
        skippedStrides[index],
        processedStrides[index],
        remainingFilterTime[index],
        ioWaitTime[index],
        preloadSplits[index],
        physicalWrittenBytes[index],
        numWrittenFiles[index]);
  }

  public SingleMetric getSingleMetrics() {
    return singleMetric;
  }

  public static class SingleMetric {
    public long veloxToArrow;
  }
}
