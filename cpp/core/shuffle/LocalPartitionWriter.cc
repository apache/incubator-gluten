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

#include <filesystem>
#include <random>
#include <thread>

#include <boost/stacktrace.hpp>
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Payload.h"
#include "shuffle/Spill.h"
#include "shuffle/Utils.h"
#include "utils/StringUtil.h"
#include "utils/Timer.h"

namespace gluten {

class LocalPartitionWriter::LocalEvictor {
 public:
  LocalEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : options_(options), numPartitions_(numPartitions), spillFile_(spillFile), pool_(pool), codec_(codec) {}

  virtual ~LocalEvictor() {}

  virtual arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) = 0;

  virtual arrow::Status spill() = 0;

  virtual Evict::type evictType() = 0;

  virtual arrow::Result<std::shared_ptr<Spill>> finish() = 0;

  int64_t getEvictTime() {
    return evictTime_;
  }

 protected:
  ShuffleWriterOptions* options_;
  uint32_t numPartitions_;
  std::string spillFile_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  int64_t evictTime_{0};
};

class LocalPartitionWriter::PayloadMerger {
 public:
  PayloadMerger(ShuffleWriterOptions* options, arrow::MemoryPool* pool, arrow::util::Codec* codec, bool hasComplexType)
      : options_(options), pool_(pool), codec_(codec), hasComplexType_(hasComplexType) {}

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> merge(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    if (hasComplexType_) {
      // TODO: Merging complex type is currently not supported.
      ARROW_ASSIGN_OR_RAISE(
          auto blockPayload, createBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
      return blockPayload;
    }

    MergeGuard mergeGuard(partitionInMerge_, partitionId);

    if (!hasMerged(partitionId)) {
      if (numRows < options_->compression_threshold) {
        // Save for merge.
        ARROW_ASSIGN_OR_RAISE(
            partitionMergePayload_[partitionId],
            createMergeBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
        DEBUG_OUT << "Create new merge for partition: " << partitionId << ", initial numRows: " << numRows << std::endl;
        return std::nullopt;
      }
      // If already reach merging threshold (currently uses compression threshold, also applicable for uncompressed),
      // return BlockPayload.
      ARROW_ASSIGN_OR_RAISE(
          auto blockPayload, createBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
      return blockPayload;
    }
    auto lastPayload = std::move(partitionMergePayload_[partitionId]);
    auto mergedRows = numRows + lastPayload->numRows();
    DEBUG_OUT << "Merged partition: " << partitionId << ", numRows before: " << lastPayload->numRows()
              << ", numRows appended: " << numRows << ", numRows after: " << mergedRows << std::endl;
    ARROW_ASSIGN_OR_RAISE(
        auto merged, MergeBlockPayload::merge(std::move(lastPayload), numRows, std::move(buffers), pool_, codec_));
    if (mergedRows < options_->compression_threshold) {
      // Still not reach compression threshold, save for next merge.
      partitionMergePayload_[partitionId] = std::move(merged);
      return std::nullopt;
    }
    return merged->toBlockPayload(codec_ == nullptr ? Payload::kUncompressed : Payload::kToBeCompressed);
  }

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> finish(uint32_t partitionId, bool spill) {
    if (spill) {
      // If it's triggered by spill, we need to check whether the spill source is from compressing the merged buffers.
      if ((partitionInMerge_.has_value() && *partitionInMerge_ == partitionId) || !hasMerged(partitionId)) {
        return std::nullopt;
      }
      auto payload = std::move(partitionMergePayload_[partitionId]);
      return payload->toBlockPayload(Payload::kUncompressed);
    }
    if (!hasMerged(partitionId)) {
      return std::nullopt;
    }
    auto numRows = partitionMergePayload_[partitionId]->numRows();
    auto payloadType = (codec_ == nullptr || numRows < options_->compression_threshold) ? Payload::kUncompressed
                                                                                        : Payload::kToBeCompressed;
    auto payload = std::move(partitionMergePayload_[partitionId]);
    return payload->toBlockPayload(payloadType);
  }

  bool hasMerged(uint32_t partitionId) {
    return partitionMergePayload_.find(partitionId) != partitionMergePayload_.end() &&
        partitionMergePayload_[partitionId] != nullptr;
  }

 private:
  ShuffleWriterOptions* options_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  bool hasComplexType_;
  std::unordered_map<uint32_t, std::unique_ptr<MergeBlockPayload>> partitionMergePayload_;
  std::optional<uint32_t> partitionInMerge_;

  class MergeGuard {
   public:
    MergeGuard(std::optional<uint32_t>& partitionInMerge, uint32_t partitionId) : partitionInMerge_(partitionInMerge) {
      partitionInMerge_ = partitionId;
    }

    ~MergeGuard() {
      partitionInMerge_ = std::nullopt;
    }

   private:
    std::optional<uint32_t>& partitionInMerge_;
  };

  arrow::Status copyBuffers(std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
    // Copy.
    std::vector<std::shared_ptr<arrow::Buffer>> copies;
    for (auto& buffer : buffers) {
      if (!buffer) {
        continue;
      }
      if (buffer->size() == 0) {
        buffer = zeroLengthNullBuffer();
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool_));
      memcpy(copy->mutable_data(), buffer->data(), buffer->size());
      buffer = std::move(copy);
    }
    return arrow::Status::OK();
  }

  arrow::Result<std::unique_ptr<MergeBlockPayload>> createMergeBlockPayload(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    if (reuseBuffers) {
      // This is the first payload, therefore need copy.
      RETURN_NOT_OK(copyBuffers(buffers));
    }
    return std::make_unique<MergeBlockPayload>(numRows, std::move(buffers), isValidityBuffer, pool_, codec_);
  }

  arrow::Result<std::unique_ptr<BlockPayload>> createBlockPayload(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    auto createUncompressed = codec_ == nullptr || numRows < options_->compression_threshold;
    if (reuseBuffers && createUncompressed) {
      // For uncompressed buffers, need to copy before caching.
      RETURN_NOT_OK(copyBuffers(buffers));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        BlockPayload::fromBuffers(
            createUncompressed ? Payload::kUncompressed : Payload::kCompressed,
            numRows,
            std::move(buffers),
            isValidityBuffer,
            pool_,
            codec_));
    return payload;
  }
};

class CacheEvictor : public LocalPartitionWriter::LocalEvictor {
 public:
  CacheEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillFile, pool, codec) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) override {
    if (partitionCachedPayload_.find(partitionId) == partitionCachedPayload_.end()) {
      partitionCachedPayload_[partitionId] = std::list<std::unique_ptr<BlockPayload>>{};
    }
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::Status spill() override {
    ScopedTimer timer(evictTime_);

    ARROW_ASSIGN_OR_RAISE(auto os, arrow::io::FileOutputStream::Open(spillFile_, true));
    ARROW_ASSIGN_OR_RAISE(auto start, os->Tell());
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (partitionCachedPayload_.find(pid) != partitionCachedPayload_.end()) {
        auto payloads = std::move(partitionCachedPayload_[pid]);
        while (!payloads.empty()) {
          auto payload = std::move(payloads.front());
          payloads.pop_front();

          // Spill to disk. Do not compress.
          auto originalType = payload->giveUpCompression();
          RETURN_NOT_OK(payload->serialize(os.get()));

          if (UNLIKELY(!diskSpill_)) {
            diskSpill_ = std::make_unique<DiskSpill>(Spill::SpillType::kBatchedSpill, numPartitions_, spillFile_);
          }
          ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());
          DEBUG_OUT << "CacheEvictor: Spilled partition " << pid << " file start: " << start << ", file end: " << end
                    << ", file: " << spillFile_ << std::endl;
          diskSpill_->insertPayload(
              pid, originalType, payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
          start = end;
        }
      }
    }
    RETURN_NOT_OK(os->Close());
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> finish() override {
    if (finished_) {
      return arrow::Status::Invalid("Calling toBlockPayload() on a finished CacheEvictor.");
    }
    finished_ = true;

    if (diskSpill_) {
      return std::move(diskSpill_);
    }
    // No spill on disk. Delete the empty spill file.
    if (!spillFile_.empty() && std::filesystem::exists(spillFile_)) {
      std::filesystem::remove(spillFile_);
    }

    ARROW_ASSIGN_OR_RAISE(auto spill, createSpill());
    return std::move(spill);
  }

  Evict::type evictType() override {
    return Evict::kCache;
  }

 private:
  bool finished_{false};
  std::shared_ptr<DiskSpill> diskSpill_{nullptr};
  std::unordered_map<uint32_t, std::list<std::unique_ptr<BlockPayload>>> partitionCachedPayload_;

  arrow::Result<std::shared_ptr<InMemorySpill>> createSpill() {
    if (partitionCachedPayload_.empty()) {
      return arrow::Status::Invalid("CacheEvictor cannot create InMemorySpill: empty cached payloads.");
    }
    auto spill = std::make_unique<InMemorySpill>(
        Spill::SpillType::kBatchedInMemorySpill,
        numPartitions_,
        options_->buffer_size,
        options_->compression_threshold,
        pool_,
        codec_,
        std::move(partitionCachedPayload_));
    partitionCachedPayload_.clear();
    return spill;
  }
};

class SpillEvictor final : public LocalPartitionWriter::LocalEvictor {
 public:
  SpillEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillFile, pool, codec) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) override {
    ScopedTimer timer(evictTime_);
    if (!opened_) {
      opened_ = true;
      ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillFile_, true));
      diskSpill_ = std::make_unique<DiskSpill>(Spill::SpillType::kSequentialSpill, numPartitions_, spillFile_);
    }

    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(payload->serialize(os_.get()));
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DEBUG_OUT << "SpillEvictor: Spilled partition " << partitionId << " file start: " << start << ", file end: " << end
              << ", file: " << spillFile_ << std::endl;
    diskSpill_->insertPayload(
        partitionId, payload->type(), payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> finish() override {
    if (finished_) {
      return arrow::Status::Invalid("Calling toBlockPayload() on a finished SpillEvictor.");
    }
    finished_ = true;

    if (!opened_) {
      return arrow::Status::Invalid("SpillEvictor has no data spilled.");
    }
    RETURN_NOT_OK(os_->Close());
    return std::move(diskSpill_);
  }

  arrow::Status spill() override {
    return arrow::Status::OK();
  }

  Evict::type evictType() override {
    return Evict::kSpill;
  }

 private:
  bool opened_{false};
  bool finished_{false};
  std::shared_ptr<DiskSpill> diskSpill_{nullptr};
  std::shared_ptr<arrow::io::FileOutputStream> os_;
};

LocalPartitionWriter::LocalPartitionWriter(
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs,
    ShuffleWriterOptions* options)
    : PartitionWriter(numPartitions, options), dataFile_(dataFile), localDirs_(localDirs) {
  init();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(localDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % options_->num_sub_dirs;
  dirSelection_ = (dirSelection_ + 1) % localDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(dataFile_));
  if (options_->buffered_write) {
    // Output stream buffer is neither partition buffer memory nor ipc memory.
    ARROW_ASSIGN_OR_RAISE(dataFileOs_, arrow::io::BufferedOutputStream::Create(16384, options_->memory_pool, fout));
  } else {
    dataFileOs_ = fout;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  // When buffered_write = true, dataFileOs_->Close doesn't release underlying buffer.
  dataFileOs_.reset();
  return arrow::Status::OK();
}

void LocalPartitionWriter::init() {
  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

  // Shuffle the configured local directories. This prevents each task from using the same directory for spilled
  // files.
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::shuffle(localDirs_.begin(), localDirs_.end(), engine);
  subDirSelection_.assign(localDirs_.size(), 0);
}

arrow::Status LocalPartitionWriter::mergeSpills(uint32_t partitionId) {
  auto spillId = 0;
  auto spillIter = spills_.begin();
  while (spillIter != spills_.end()) {
    ARROW_ASSIGN_OR_RAISE(auto st, dataFileOs_->Tell());
    // Read if partition exists in the spilled file and write to the final file.
    while (auto payload = (*spillIter)->nextPayload(partitionId)) {
      // May trigger spill during compression.
      RETURN_NOT_OK(payload->serialize(dataFileOs_.get()));
    }
    ++spillIter;
    ARROW_ASSIGN_OR_RAISE(auto ed, dataFileOs_->Tell());
    std::cout << "Partition " << partitionId << " spilled from spillResult " << spillId++ << " of bytes " << ed - st
              << std::endl;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;

  auto writeTimer = Timer();
  writeTimer.start();

  // Finish the evictor. No compression, no spill.
  if (evictor_) {
    auto evictor = std::move(evictor_);
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), evictor->finish());
    evictTime_ += evictor->getEvictTime();
  }

  // Open final data file.
  // If options_.buffered_write is set, it will acquire 16KB memory that can trigger spill.
  RETURN_NOT_OK(openDataFile());

  int64_t endInFinalFile = 0;
  DEBUG_OUT << "Total spills: " << spills_.size() << std::endl;
  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    // Record start offset.
    auto startInFinalFile = endInFinalFile;
    // Iterator over all spilled files.
    // Reading and compressing toBeCompressed payload can trigger spill.
    RETURN_NOT_OK(mergeSpills(pid));
    if (merger_) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid, false));
      if (merged) {
        // Compressing merged payload can trigger spill.
        RETURN_NOT_OK((*merged)->serialize(dataFileOs_.get()));
      }
    }
    ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
    if (endInFinalFile != startInFinalFile && options_->write_eos) {
      // Write EOS if any payload written.
      int64_t bytes;
      RETURN_NOT_OK(writeEos(dataFileOs_.get(), &bytes));
      endInFinalFile += bytes;
    }
    partitionLengths_[pid] = endInFinalFile - startInFinalFile;
    DEBUG_OUT << "Partition " << pid << " partition length " << partitionLengths_[pid] << std::endl;
  }

  for (const auto& spill : spills_) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (spill->hasNextPayload(pid)) {
        return arrow::Status::Invalid("Merging from spill is not exhausted.");
      }
    }
  }

  writeTimer.stop();
  writeTime_ = writeTimer.realTimeUsed();
  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());

  // Close Final file, Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestEvict(Evict::type evictType) {
  if (evictor_ && evictor_->evictType() == evictType) {
    return arrow::Status::OK();
  }
  if (evictor_) {
    auto evictor = std::move(evictor_);
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), evictor->finish());
    evictTime_ += evictor->getEvictTime();
  }

  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  ARROW_ASSIGN_OR_RAISE(evictor_, createEvictor(evictType, spilledFile));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evict(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    const std::vector<bool>* isValidityBuffer,
    bool reuseBuffers,
    Evict::type evictType,
    bool hasComplexType) {
  rawPartitionLengths_[partitionId] += getBufferSize(buffers);

  if (evictType == Evict::kSpill) {
    RETURN_NOT_OK(requestEvict(evictType));
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        BlockPayload::fromBuffers(
            Payload::kUncompressed, numRows, std::move(buffers), isValidityBuffer, payloadPool_.get(), nullptr));
    RETURN_NOT_OK(evictor_->evict(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  // Otherwise do cache evict.
  if (!merger_) {
    merger_ =
        std::make_shared<PayloadMerger>(options_, payloadPool_.get(), codec_ ? codec_.get() : nullptr, hasComplexType);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto merged, merger_->merge(partitionId, numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
  if (merged.has_value()) {
    RETURN_NOT_OK(requestEvict(Evict::kCache));
    RETURN_NOT_OK(evictor_->evict(partitionId, std::move(*merged)));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::populateMetrics(ShuffleWriterMetrics* metrics) {
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += evictTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted_;
  metrics->totalBytesWritten += totalBytesWritten_;
  metrics->partitionLengths = std::move(partitionLengths_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<LocalPartitionWriter::LocalEvictor>> LocalPartitionWriter::createEvictor(
    Evict::type evictType,
    const std::string& spillFile) {
  switch (evictType) {
    case Evict::kSpill:
      return std::make_unique<SpillEvictor>(numPartitions_, options_, spillFile, payloadPool_.get(), codec_.get());
    case Evict::kCache:
      return std::make_unique<CacheEvictor>(numPartitions_, options_, spillFile, payloadPool_.get(), codec_.get());
    default:
      return arrow::Status::Invalid("Cannot create Evictor from type .", std::to_string(evictType));
  }
}

arrow::Status LocalPartitionWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  int64_t reclaimed = 0;
  // If not stopped, can reclaim from cached evictor and merger.
  // If stopped, can reclaim from the InMemorySpill and merger.
  if (!stopped_) {
    // First toBlockPayload existing evictor and spill.
    if (evictor_) {
      auto beforeSpill = payloadPool_->bytes_allocated();
      auto evictor = std::move(evictor_);
      RETURN_NOT_OK(evictor->spill());
      spills_.emplace_back();
      ARROW_ASSIGN_OR_RAISE(spills_.back(), evictor->finish());
      evictTime_ += evictor->getEvictTime();
      reclaimed += beforeSpill - payloadPool_->bytes_allocated();
      if (reclaimed >= size) {
        *actual = reclaimed;
        return arrow::Status::OK();
      }
    }
  } else {
    // Convert InMemorySpill to DiskSpill.
    if (auto lastSpill = std::dynamic_pointer_cast<InMemorySpill>(spills_.back())) {
      auto beforeSpill = payloadPool_->bytes_allocated();
      ARROW_ASSIGN_OR_RAISE(auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
      spills_.emplace_back();
      ARROW_ASSIGN_OR_RAISE(spills_.back(), lastSpill->toDiskSpill(spillFile));
      reclaimed += beforeSpill - payloadPool_->bytes_allocated();
      if (reclaimed >= size) {
        *actual = reclaimed;
        return arrow::Status::OK();
      }
    }
  }

  // Then spill payloads from merger. Create uncompressed payloads.
  if (merger_) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid, true));
      if (merged.has_value()) {
        RETURN_NOT_OK(requestEvict(Evict::kSpill));
        RETURN_NOT_OK(evictor_->evict(pid, std::move(*merged)));
      }
    }
    if (evictor_) {
      auto evictor = std::move(evictor_);
      spills_.emplace_back();
      ARROW_ASSIGN_OR_RAISE(spills_.back(), evictor->finish());
      evictTime_ += evictor->getEvictTime();
    }
    // This could not be accurate. When the evicted partition buffers are not copied, the merged ones
    // are resized from the original buffers thus allocated from partitionBufferPool.
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
  }
  *actual = reclaimed;
  return arrow::Status::OK();
}
} // namespace gluten
