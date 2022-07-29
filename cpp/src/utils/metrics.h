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

#pragma once

struct Metrics {
  int numMetrics = 0;

  long* inputRows;
  long* inputVectors;
  long* inputBytes;
  long* rawInputRows;
  long* rawInputBytes;
  long* outputRows;
  long* outputVectors;
  long* outputBytes;

  // CpuWallTiming
  long* count;
  long* wallNanos;

  long* peakMemoryBytes;
  long* numMemoryAllocations;

  Metrics(int size) : numMetrics(size) {
    inputRows = new long[numMetrics]();
    inputVectors = new long[numMetrics]();
    inputBytes = new long[numMetrics]();
    rawInputRows = new long[numMetrics]();
    rawInputBytes = new long[numMetrics]();
    outputRows = new long[numMetrics]();
    outputVectors = new long[numMetrics]();
    outputBytes = new long[numMetrics]();
    count = new long[numMetrics]();
    wallNanos = new long[numMetrics]();
    peakMemoryBytes = new long[numMetrics]();
    numMemoryAllocations = new long[numMetrics]();
  }

  ~Metrics() {
    delete[] inputRows;
    delete[] inputVectors;
    delete[] inputBytes;
    delete[] rawInputRows;
    delete[] rawInputBytes;
    delete[] outputRows;
    delete[] outputVectors;
    delete[] outputBytes;
    delete[] count;
    delete[] wallNanos;
    delete[] peakMemoryBytes;
    delete[] numMemoryAllocations;
  }
};
