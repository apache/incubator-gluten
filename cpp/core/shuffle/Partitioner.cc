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

#include "shuffle/Partitioner.h"
#include "shuffle/FallbackRangePartitioner.h"
#include "shuffle/HashPartitioner.h"
#include "shuffle/RandomPartitioner.h"
#include "shuffle/RoundRobinPartitioner.h"
#include "shuffle/SinglePartitioner.h"

namespace gluten {

arrow::Result<std::shared_ptr<Partitioner>>
Partitioner::make(Partitioning partitioning, int32_t numPartitions, int32_t startPartitionId) {
  switch (partitioning) {
    case Partitioning::kHash:
      return std::make_shared<HashPartitioner>(numPartitions);
    case Partitioning::kRoundRobin:
      return std::make_shared<RoundRobinPartitioner>(numPartitions, startPartitionId);
    case Partitioning::kSingle:
      return std::make_shared<SinglePartitioner>();
    case Partitioning::kRange:
      return std::make_shared<FallbackRangePartitioner>(numPartitions);
    case Partitioning::kRandom:
      return std::make_shared<RandomPartitioner>(numPartitions);
    default:
      return arrow::Status::Invalid("Unsupported partitioning type: " + std::to_string(partitioning));
  }
}

} // namespace gluten
