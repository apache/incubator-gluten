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

#include "VeloxHashShuffleWriter.h"

namespace gluten {

class VeloxGpuHashShuffleWriter : public VeloxHashShuffleWriter {
 public:
  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options,
      MemoryManager* memoryManager);

  VeloxGpuHashShuffleWriter(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<GpuHashShuffleWriterOptions>& options,
      MemoryManager* memoryManager)
      : VeloxHashShuffleWriter(numPartitions, partitionWriter, options, memoryManager) {}

 private:
  // Split the bool to byte.
  void splitBoolValueType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) override;

  uint64_t valueBufferSizeForBool(uint32_t newSize) override {
    return newSize;
  }

  bool boolIsBit() override {
    return false;
  }

  arrow::Status splitTimestamp(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) override;

  uint64_t valueBufferSizeForTimestamp(uint32_t newSize) override {
    return sizeof(int64_t) * newSize;
  }
};
} // namespace gluten
