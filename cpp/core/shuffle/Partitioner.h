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

#include "shuffle/ShuffleWriter.h"

namespace gluten {

class ShuffleWriter::Partitioner {
 public:
  template <typename Partitioner>
  static std::shared_ptr<Partitioner> create(int32_t numPartitions, bool hasPid) {
    return std::make_shared<Partitioner>(numPartitions, hasPid);
  }

  static arrow::Result<std::shared_ptr<ShuffleWriter::Partitioner>> make(
      const std::string& name,
      int32_t numPartitions);
  // whether the first column is partition key
  bool hasPid() {
    return hasPid_;
  }

  virtual arrow::Status compute(
      const int32_t* pidArr,
      const int64_t numRows,
      std::vector<uint16_t>& row2partition,
      std::vector<uint32_t>& partition2RowCount) = 0;

 protected:
  Partitioner(int32_t numPartitions, bool hasPid) : numPartitions_(numPartitions), hasPid_(hasPid) {}
  virtual ~Partitioner() = default;

  int32_t numPartitions_;

  // if the first column is partition key
  bool hasPid_;
};

} // namespace gluten
