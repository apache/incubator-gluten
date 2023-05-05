#include "FallbackRangePartitioner.h"
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

#include "shuffle/FallbackRangePartitioner.h"

namespace gluten {
arrow::Status gluten::FallbackRangePartitioner::Compute(
    const int32_t* pid_arr,
    const int64_t num_rows,
    std::vector<uint16_t>& partition_id,
    std::vector<uint32_t>& partition_id_cnt) {
  partition_id.resize(num_rows);
  std::fill(std::begin(partition_id_cnt), std::end(partition_id_cnt), 0);
  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i];
    if (pid >= num_partitions_) {
      return arrow::Status::Invalid(
          "Partition id ", std::to_string(pid), " is equal or greater than ", std::to_string(num_partitions_));
    }
    partition_id[i] = pid;
    partition_id_cnt[pid]++;
  }
  return arrow::Status::OK();
}

} // namespace gluten
