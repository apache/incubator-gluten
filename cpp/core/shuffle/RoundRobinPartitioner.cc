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

#include "shuffle/RoundRobinPartitioner.h"

namespace gluten {

arrow::Status gluten::RoundRobinPartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2Partition,
    std::vector<uint32_t>& partition2RowCount) {
  std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);
  row2Partition.resize(numRows);

  for (int32_t i = 0; i < numRows; ++i) {
    row2Partition[i] = pidSelection_;
    pidSelection_ = (pidSelection_ + 1) % numPartitions_;
  }

  for (auto& pid : row2Partition) {
    partition2RowCount[pid]++;
  }

  return arrow::Status::OK();
}

} // namespace gluten
