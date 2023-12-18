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
#include "utils/DebugOut.h"

namespace gluten {

InMemorySpill::InMemorySpill(
    SpillType type,
    uint32_t numPartitions,
    uint32_t batchSize,
    uint32_t compressionThreshold,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    std::unordered_map<uint32_t, std::list<std::unique_ptr<BlockPayload>>> partitionToPayloads)
    : Spill(type, numPartitions),
      partitionToPayloads_(std::move(partitionToPayloads)),
      batchSize_(batchSize),
      compressionThreshold_(compressionThreshold),
      pool_(pool),
      codec_(codec) {}

bool InMemorySpill::hasNextPayload(uint32_t partitionId) {
  return partitionToPayloads_.find(partitionId) != partitionToPayloads_.end() &&
      !partitionToPayloads_[partitionId].empty();
}

std::unique_ptr<Payload> InMemorySpill::nextPayload(uint32_t partitionId) {
  if (!hasNextPayload(partitionId)) {
    return nullptr;
  }
  auto front = std::move(partitionToPayloads_[partitionId].front());
  partitionToPayloads_[partitionId].pop_front();
  return front;
}

arrow::Result<std::unique_ptr<Spill>> InMemorySpill::toDiskSpill(const std::string& spillFile) {
  ARROW_ASSIGN_OR_RAISE(auto os, arrow::io::FileOutputStream::Open(spillFile, true));
  auto diskSpill = std::make_unique<DiskSpill>(Spill::SpillType::kSequentialSpill, numPartitions_, spillFile);
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    while (hasNextPayload(pid)) {
      auto payload = std::move(partitionToPayloads_[pid].front());
      partitionToPayloads_[pid].pop_front();
      ARROW_ASSIGN_OR_RAISE(auto start, os->Tell());
      auto originalType = payload->giveUpCompression();
      RETURN_NOT_OK(payload->serialize(os.get()));
      ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());
      DEBUG_OUT << "InMemorySpill: Spilled partition " << pid << " file start: " << start << ", file end: " << end
                << ", file: " << spillFile << std::endl;
      diskSpill->insertPayload(
          pid, originalType, payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
    }
  }
  return diskSpill;
}

DiskSpill::DiskSpill(Spill::SpillType type, uint32_t numPartitions, const std::string& spillFile)
    : Spill(type, numPartitions), spillFile_(spillFile) {}

DiskSpill::~DiskSpill() {
  if (is_) {
    (void)is_->Close();
  }
}

bool DiskSpill::hasNextPayload(uint32_t partitionId) {
  return !partitionPayloads_.empty() && partitionPayloads_.front().partitionId == partitionId;
}

std::unique_ptr<Payload> DiskSpill::nextPayload(uint32_t partitionId) {
  openSpillFile();
  if (!hasNextPayload(partitionId)) {
    return nullptr;
  }
  auto payload = std::move(partitionPayloads_.front().payload);
  partitionPayloads_.pop_front();
  return payload;
}

void DiskSpill::insertPayload(
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

void DiskSpill::openSpillFile() {
  if (!is_) {
    GLUTEN_ASSIGN_OR_THROW(is_, arrow::io::MemoryMappedFile::Open(spillFile_, arrow::io::FileMode::READ));
    rawIs_ = is_.get();
  }
}
} // namespace gluten
