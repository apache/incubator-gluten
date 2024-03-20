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

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/util/compression.h>
#include <list>

#include "shuffle/Payload.h"
#include "utils/macros.h"

namespace gluten {

class Spill final {
 public:
  enum SpillType { kSequentialSpill, kBatchedSpill };

  Spill(SpillType type, uint32_t numPartitions, const std::string& spillFile);

  ~Spill();

  SpillType type() const;

  bool hasNextPayload(uint32_t partitionId);

  std::unique_ptr<Payload> nextPayload(uint32_t partitionId);

  void insertPayload(
      uint32_t partitionId,
      Payload::Type payloadType,
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      uint64_t rawSize,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec);

 private:
  struct PartitionPayload {
    uint32_t partitionId{};
    std::unique_ptr<Payload> payload{};
  };

  SpillType type_;
  std::shared_ptr<arrow::io::MemoryMappedFile> is_;
  std::list<PartitionPayload> partitionPayloads_{};
  std::shared_ptr<arrow::io::MemoryMappedFile> inputStream_{};
  std::string spillFile_;

  arrow::io::InputStream* rawIs_;

  void openSpillFile();
};
} // namespace gluten