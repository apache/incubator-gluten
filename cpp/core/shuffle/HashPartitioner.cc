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

#include "shuffle/HashPartitioner.h"

namespace gluten {

int32_t computePid(const int32_t* pidArr, int64_t i, int32_t numPartitions) {
  auto pid = pidArr[i] % numPartitions;
#if defined(__x86_64__)
  // force to generate ASM
  __asm__(
      "lea (%[num_partitions],%[pid],1),%[tmp]\n"
      "test %[pid],%[pid]\n"
      "cmovs %[tmp],%[pid]\n"
      : [pid] "+r"(pid)
      : [num_partitions] "r"(numPartitions), [tmp] "r"(0));
#else
  if (pid < 0) {
    pid += numPartitions;
  }
#endif
  return pid;
}

arrow::Status
gluten::HashPartitioner::compute(const int32_t* pidArr, const int64_t numRows, std::vector<uint32_t>& row2partition) {
  row2partition.resize(numRows);
  for (auto i = 0; i < numRows; ++i) {
    auto pid = computePid(pidArr, i, numPartitions_);
    row2partition[i] = pid;
  }
  return arrow::Status::OK();
}

arrow::Status gluten::HashPartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    const int32_t vectorIndex,
    std::unordered_map<int32_t, std::vector<int64_t>>& rowVectorIndexMap) {
  auto index = static_cast<int64_t>(vectorIndex) << 32;
  for (auto i = 0; i < numRows; ++i) {
    auto pid = computePid(pidArr, i, numPartitions_);
    int64_t combined = index | (static_cast<int64_t>(i) & 0xFFFFFFFFLL);
    auto& vec = rowVectorIndexMap[pid];
    vec.push_back(combined);
  }

  return arrow::Status::OK();
}

} // namespace gluten
