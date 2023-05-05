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
#include "shuffle/RoundRobinPartitioner.h"
#include "shuffle/SinglePartPartitioner.h"

namespace gluten {
arrow::Result<std::shared_ptr<ShuffleWriter::Partitioner>> ShuffleWriter::Partitioner::Make(
    const std::string& name,
    int32_t num_partitions) {
  std::shared_ptr<ShuffleWriter::Partitioner> partitioner = nullptr;
  if (name == "hash") {
    partitioner = ShuffleWriter::Partitioner::Create<HashPartitioner>(num_partitions, true);
  } else if (name == "rr") {
    partitioner = ShuffleWriter::Partitioner::Create<RoundRobinPartitioner>(num_partitions, false);
  } else if (name == "range") {
    partitioner = ShuffleWriter::Partitioner::Create<FallbackRangePartitioner>(num_partitions, true);
  } else if (name == "single") {
    partitioner = ShuffleWriter::Partitioner::Create<SinglePartPartitioner>(num_partitions, false);
  }

  if (!partitioner) {
    return arrow::Status::NotImplemented("Partitioning " + name + " not supported yet.");
  } else {
    return partitioner;
  }
}

} // namespace gluten
