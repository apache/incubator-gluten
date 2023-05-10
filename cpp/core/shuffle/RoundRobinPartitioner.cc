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
    std::vector<uint16_t>& partitionId,
    std::vector<uint32_t>& partitionIdCnt) {
  std::fill(std::begin(partitionIdCnt), std::end(partitionIdCnt), 0);
  partitionId.resize(numRows);
  for (auto& pid : partitionId) {
    pid = pidSelection_;
    partitionIdCnt[pidSelection_]++;
    pidSelection_ = (pidSelection_ + 1) == numPartitions_ ? 0 : (pidSelection_ + 1);
  }
  return arrow::Status::OK();
}
} // namespace gluten
