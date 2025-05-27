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

Spill::~Spill() {
  if (is_) {
    static_cast<void>(is_->Close());
  }
}

bool Spill::hasNextPayload(uint32_t partitionId) {
  return !partitionPayloads_.empty() && partitionPayloads_.front().partitionId == partitionId;
}

std::unique_ptr<Payload> Spill::nextPayload(uint32_t partitionId) {
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
    int64_t rawSize,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec) {
  switch (payloadType) {
    case Payload::Type::kUncompressed:
    case Payload::Type::kToBeCompressed:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<UncompressedDiskBlockPayload>(
               payloadType, numRows, isValidityBuffer, rawIs_, rawSize, pool, codec)});
      break;
    case Payload::Type::kCompressed:
    case Payload::Type::kRaw:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<CompressedDiskBlockPayload>(numRows, isValidityBuffer, rawIs_, rawSize, pool)});
      break;
    default:
      throw GlutenException("Unreachable.");
  }
}

void Spill::openForRead(uint64_t shuffleFileBufferSize) {
  if (!is_) {
    GLUTEN_ASSIGN_OR_THROW(is_, MmapFileStream::open(spillFile_, shuffleFileBufferSize));
    rawIs_ = is_.get();
  }
}

void Spill::setSpillFile(const std::string& spillFile) {
  spillFile_ = spillFile;
}

void Spill::setSpillTime(int64_t spillTime) {
  spillTime_ = spillTime;
}

void Spill::setCompressTime(int64_t compressTime) {
  compressTime_ = compressTime;
}

std::string Spill::spillFile() const {
  return spillFile_;
}

int64_t Spill::spillTime() const {
  return spillTime_;
}

int64_t Spill::compressTime() const {
  return compressTime_;
}
} // namespace gluten
