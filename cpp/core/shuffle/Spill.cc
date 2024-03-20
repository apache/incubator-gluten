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

#include <iostream>

#include "shuffle/Spill.h"

namespace gluten {

Spill::Spill(Spill::SpillType type, uint32_t /* numPartitions */, const std::string& spillFile)
    : type_(type), spillFile_(spillFile) {}

Spill::~Spill() {
  if (is_) {
    (void)is_->Close();
  }
}

bool Spill::hasNextPayload(uint32_t partitionId) {
  return !partitionPayloads_.empty() && partitionPayloads_.front().partitionId == partitionId;
}

std::unique_ptr<Payload> Spill::nextPayload(uint32_t partitionId) {
  openSpillFile();
  if (!hasNextPayload(partitionId)) {
    return nullptr;
  }
  auto payload = std::move(partitionPayloads_.front().payload);
  partitionPayloads_.pop_front();
  return payload;
}

void Spill::insertPayload(
    uint32_t partitionId,
    Payload::Type payloadType,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    uint64_t rawSize,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec) {
  // TODO: Add compression threshold.
  switch (payloadType) {
    case Payload::Type::kUncompressed:
    case Payload::Type::kToBeCompressed:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<UncompressedDiskBlockPayload>(
               payloadType, numRows, isValidityBuffer, rawIs_, rawSize, pool, codec)});
      break;
    case Payload::Type::kCompressed:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<CompressedDiskBlockPayload>(numRows, isValidityBuffer, rawIs_, rawSize, pool)});
      break;
  }
}

void Spill::openSpillFile() {
  if (!is_) {
    GLUTEN_ASSIGN_OR_THROW(is_, arrow::io::MemoryMappedFile::Open(spillFile_, arrow::io::FileMode::READ));
    rawIs_ = is_.get();
  }
}

Spill::SpillType Spill::type() const {
  return type_;
}
} // namespace gluten
