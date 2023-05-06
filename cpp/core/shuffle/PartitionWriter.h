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

class ShuffleWriter::PartitionWriter {
 public:
  static arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(
      ShuffleWriter* shuffleWriter,
      int32_t numPartitions);

 public:
  PartitionWriter(ShuffleWriter* shuffleWriter, int32_t numPartitions)
      : shuffle_writer_(shuffleWriter), num_partitions_(numPartitions) {}
  virtual ~PartitionWriter() = default;

  virtual arrow::Status init() = 0;

  virtual arrow::Status evictPartition(int32_t partitionId) = 0;

  virtual arrow::Status stop() = 0;

  ShuffleWriter* shuffle_writer_;
  uint32_t num_partitions_;
};

} // namespace gluten
