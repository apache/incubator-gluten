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

#include "shuffle/Partitioner.h"

namespace gluten {

class HashPartitioner final : public ShuffleWriter::Partitioner {
 public:
  HashPartitioner(int32_t num_partitions, bool has_pid) : Partitioner(num_partitions, has_pid) {}

  arrow::Status Compute(
      const int32_t* pid_arr,
      const int64_t num_rows,
      std::vector<uint16_t>& partition_id,
      std::vector<uint32_t>& partition_id_cnt) override;
};

} // namespace gluten
