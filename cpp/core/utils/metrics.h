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

#include <memory>

namespace gluten {

struct Metrics {
  unsigned int numMetrics = 0;
  long veloxToArrow = 0;

  std::unique_ptr<long[]> array; // underlying memory buffer
  long* arrayRawPtr = nullptr; // point to array.get() after array created

  enum TYPE {
    kBegin = 0, // begin from 0

    kInputRows = kBegin,
    kInputVectors,
    kInputBytes,

    kRawInputRows,
    kRawInputBytes,

    kOutputRows,
    kOutputVectors,
    kOutputBytes,

    // CpuWallTiming.
    kCpuCount,
    kWallNanos,

    kPeakMemoryBytes,
    kNumMemoryAllocations,

    // Spill
    kSpilledBytes,
    kSpilledRows,
    kSpilledPartitions,
    kSpilledFiles,

    // Runtime metrics.
    kNumDynamicFiltersProduced,
    kNumDynamicFiltersAccepted,
    kNumReplacedWithDynamicFilterRows,
    kFlushRowCount,
    kScanTime,
    kSkippedSplits,
    kProcessedSplits,
    kSkippedStrides,
    kProcessedStrides,

    kEnd,
    kNum = kEnd - kBegin
  };

  Metrics(unsigned int numMetrics) : numMetrics(numMetrics), array(new long[numMetrics * kNum]) {
    arrayRawPtr = array.get();
  }

  Metrics(const Metrics&) = delete;
  Metrics(Metrics&&) = delete;
  Metrics& operator=(const Metrics&) = delete;
  Metrics& operator=(Metrics&&) = delete;

  long* get(TYPE type) {
    assert((int)type >= (int)kBegin && (int)type < (int)kEnd);
    auto offset = ((int)type - (int)kBegin) * numMetrics;
    return &arrayRawPtr[offset];
  }
};

} // namespace gluten
