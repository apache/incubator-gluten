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

package io.glutenproject.vectorized;

public class OperatorMetrics {
  public long inputRows;
  public long inputVectors;
  public long inputBytes;
  public long rawInputRows;
  public long rawInputBytes;
  public long outputRows;
  public long outputVectors;
  public long outputBytes;
  public long cpuNanos;
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
  public long partialAggregationPctSum;
  public long partialAggregationPctCount;

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
      long cpuNanos,
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
      long scanTime) {
    this.inputRows = inputRows;
    this.inputVectors = inputVectors;
    this.inputBytes = inputBytes;
    this.rawInputRows = rawInputRows;
    this.rawInputBytes = rawInputBytes;
    this.outputRows = outputRows;
    this.outputVectors = outputVectors;
    this.outputBytes = outputBytes;
    this.cpuNanos = cpuNanos;
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
  }
}
