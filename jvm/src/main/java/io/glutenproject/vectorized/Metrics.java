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

public class Metrics {
  public long[] inputRows;
  public long[] inputVectors;
  public long[] inputBytes;
  public long[] rawInputRows;
  public long[] rawInputBytes;
  public long[] outputRows;
  public long[] outputVectors;
  public long[] outputBytes;
  public long[] count;
  public long[] wallNanos;
  public long[] peakMemoryBytes;
  public long[] numMemoryAllocations;

  /**
   * Create an instance for native metrics.
   */
  public Metrics(long[] inputRows, long[] inputVectors, long[] inputBytes, long[] rawInputRows,
                 long[] rawInputBytes, long[] outputRows, long[] outputVectors, long[] outputBytes,
                 long[] count, long[] wallNanos, long[] peakMemoryBytes, long[] numMemoryAllocations) {
    this.inputRows = inputRows;
    this.inputVectors = inputVectors;
    this.inputBytes = inputBytes;
    this.rawInputRows = rawInputRows;
    this.rawInputBytes = rawInputBytes;
    this.outputRows = outputRows;
    this.outputVectors = outputVectors;
    this.outputBytes = outputBytes;
    this.count = count;
    this.wallNanos = wallNanos;
    this.peakMemoryBytes = peakMemoryBytes;
    this.numMemoryAllocations = numMemoryAllocations;
  }

  public OperatorMetrics getOperatorMetrics(int index) {
    if (index >= inputRows.length) {
      throw new RuntimeException("Invalid index.");
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
            count[index],
            wallNanos[index],
            peakMemoryBytes[index],
            numMemoryAllocations[index]);
  }
}
