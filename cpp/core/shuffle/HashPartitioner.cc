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

arrow::Status gluten::HashPartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2partition,
    std::vector<uint32_t>& partition2RowCount) {
  row2partition.resize(numRows);
  std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);

  for (auto i = 0; i < numRows; ++i) {
    auto pid = pidArr[i] % numPartitions_;
#if defined(__x86_64__)
    // force to generate ASM
    __asm__(
        "lea (%[num_partitions],%[pid],1),%[tmp]\n"
        "test %[pid],%[pid]\n"
        "cmovs %[tmp],%[pid]\n"
        : [pid] "+r"(pid)
        : [num_partitions] "r"(numPartitions_), [tmp] "r"(0));
#else
    if (pid < 0) {
      pid += numPartitions_;
    }
#endif
    row2partition[i] = pid;
  }

  for (auto& pid : row2partition) {
    partition2RowCount[pid]++;
  }

  return arrow::Status::OK();
}

} // namespace gluten
