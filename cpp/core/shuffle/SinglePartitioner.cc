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

#include "shuffle/SinglePartitioner.h"

namespace gluten {

arrow::Status
gluten::SinglePartitioner::compute(const int32_t* pidArr, const int64_t numRows, std::vector<uint32_t>& row2partition) {
  return arrow::Status::Invalid("SinglePartitioner doesn't support computing partition id.");
}

arrow::Status gluten::SinglePartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    const int32_t vectorIndex,
    std::unordered_map<int32_t, std::vector<int64_t>>& rowVectorIndexMap) {
  // nothing is need do here
  return arrow::Status::OK();
}

} // namespace gluten
