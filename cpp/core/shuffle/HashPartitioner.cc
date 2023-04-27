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

arrow::Status gluten::HashPartitioner::Compute(
    const int32_t* pid_arr,
    const int64_t num_rows,
    std::vector<uint16_t>& partition_id,
    std::vector<uint32_t>& partition_id_cnt) {
  partition_id.resize(num_rows);
  std::fill(std::begin(partition_id_cnt), std::end(partition_id_cnt), 0);

  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i] % num_partitions_;
#if defined(__x86_64__)
    // force to generate ASM
    __asm__(
        "lea (%[num_partitions],%[pid],1),%[tmp]\n"
        "test %[pid],%[pid]\n"
        "cmovs %[tmp],%[pid]\n"
        : [pid] "+r"(pid)
        : [num_partitions] "r"(num_partitions_), [tmp] "r"(0));
#else
    if (pid < 0) {
      pid += num_partitions_;
    }
#endif
    partition_id[i] = pid;
    partition_id_cnt[pid]++;
  }
  return arrow::Status::OK();
}

} // namespace gluten
