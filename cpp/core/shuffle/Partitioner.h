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

#include <arrow/result.h>

#include <memory>
#include <unordered_map>
#include <vector>
#include "shuffle/Partitioning.h"

namespace gluten {

class Partitioner {
 public:
  static std::shared_ptr<Partitioner> make(Partitioning partitioning, int32_t numPartitions, int32_t startPartitionId);

  // Whether the first column is partition key.
  bool hasPid() const {
    return hasPid_;
  }

  virtual arrow::Status compute(const int32_t* pidArr, const int64_t numRows, std::vector<uint32_t>& row2partition) = 0;

  virtual arrow::Status compute(
      const int32_t* pidArr,
      const int64_t numRows,
      const int32_t vectorIndex,
      std::unordered_map<int32_t, std::vector<int64_t>>& rowVectorIndexMap) = 0;

 protected:
  Partitioner(int32_t numPartitions, bool hasPid) : numPartitions_(numPartitions), hasPid_(hasPid) {}

  Partitioner() : numPartitions_(1), hasPid_(false) {}

  virtual ~Partitioner() = default;

  int32_t numPartitions_;
  bool hasPid_;
};

} // namespace gluten
