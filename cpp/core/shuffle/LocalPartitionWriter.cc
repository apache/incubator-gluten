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
#include <glog/logging.h>
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Payload.h"
#include "shuffle/Spill.h"
#include "shuffle/Utils.h"

namespace gluten {

class LocalPartitionWriter::LocalSpiller {
 public:
  LocalSpiller(
      std::shared_ptr<arrow::io::OutputStream> os,
      std::string spillFile,
      uint32_t compressionThreshold,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : os_(os),
        spillFile_(std::move(spillFile)),
        compressionThreshold_(compressionThreshold),
        pool_(pool),
        codec_(codec),
        diskSpill_(std::make_unique<Spill>(Spill::SpillType::kSequentialSpill)) {}

  arrow::Status spill(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) {
    // Check spill Type.
    ARROW_RETURN_IF(
        payload->type() == Payload::kToBeCompressed,
        arrow::Status::Invalid("Cannot spill payload of type: " + payload->toString()));
    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(payload->serialize(os_.get()));
    compressTime_ += payload->getCompressTime();
    spillTime_ += payload->getWriteTime();
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DLOG(INFO) << "LocalSpiller: Spilled partition " << partitionId << " file start: " << start << ", file end: " << end
               << ", file: " << spillFile_;
    if (payload->type() == Payload::kRaw) {
      diskSpill_->insertPayload(partitionId, Payload::kRaw, 0, nullptr, end - start, pool_, nullptr);
      return arrow::Status::OK();
    }

    auto payloadType = payload->type();
    if (payloadType == Payload::kUncompressed && codec_ != nullptr && payload->numRows() >= compressionThreshold_) {
      payloadType = Payload::kToBeCompressed;
    }
    diskSpill_->insertPayload(
        partitionId, payloadType, payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> finish(bool close) {
    ARROW_RETURN_IF(finished_, arrow::Status::Invalid("Calling finish() on a finished LocalSpiller."));
    ARROW_RETURN_IF(os_->closed(), arrow::Status::Invalid("Spill file os has been closed."));

    finished_ = true;
    if (close) {
      RETURN_NOT_OK(os_->Close());
    }
    diskSpill_->setSpillFile(spillFile_);
    diskSpill_->setSpillTime(spillTime_);
    diskSpill_->setCompressTime(compressTime_);
    return std::move(diskSpill_);
  }

  bool finished() const {
    return finished_;
  }

 private:
  std::shared_ptr<arrow::io::OutputStream> os_;
  std::string spillFile_;
  uint32_t compressionThreshold_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  bool finished_{false};
  std::shared_ptr<Spill> diskSpill_{nullptr};
  int64_t spillTime_{0};
  int64_t compressTime_{0};
};

class LocalPartitionWriter::PayloadMerger {
 public:
  PayloadMerger(
      const PartitionWriterOptions& options,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      bool hasComplexType)
      : pool_(pool),
        codec_(codec),
        hasComplexType_(hasComplexType),
        compressionThreshold_(options.compressionThreshold),
        mergeBufferSize_(options.mergeBufferSize),
        mergeBufferMinSize_(options.mergeBufferSize * options.mergeThreshold) {}

  arrow::Result<std::vector<std::unique_ptr<BlockPayload>>>
  merge(uint32_t partitionId, std::unique_ptr<InMemoryPayload> append, bool reuseBuffers) {
    std::vector<std::unique_ptr<BlockPayload>> merged{};
    if (hasComplexType_) {
      // TODO: Merging complex type is currently not supported.
      merged.emplace_back();
      ARROW_ASSIGN_OR_RAISE(merged.back(), createBlockPayload(std::move(append), reuseBuffers));
      return merged;
    }

    MergeGuard mergeGuard(partitionInMerge_, partitionId);

    auto cacheOrFinish = [&]() {
      if (append->numRows() <= mergeBufferMinSize_) {
        // Save for merge.
        if (reuseBuffers) {
          // This is the first append, therefore need copy.
          RETURN_NOT_OK(append->copyBuffers(pool_));
        }
        partitionMergePayload_[partitionId] = std::move(append);
        return arrow::Status::OK();
      }
      merged.emplace_back();
      // If current buffer rows reaches merging threshold, create BlockPayload.
      ARROW_ASSIGN_OR_RAISE(merged.back(), createBlockPayload(std::move(append), reuseBuffers));
      return arrow::Status::OK();
    };

    if (!hasMerged(partitionId)) {
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    auto lastPayload = std::move(partitionMergePayload_[partitionId]);
    auto mergedRows = append->numRows() + lastPayload->numRows();
    if (mergedRows > mergeBufferSize_ || append->numRows() > mergeBufferMinSize_) {
      merged.emplace_back();
      ARROW_ASSIGN_OR_RAISE(
          merged.back(),
          lastPayload->toBlockPayload(
              codec_ != nullptr && lastPayload->numRows() >= compressionThreshold_ ? Payload::kCompressed
                                                                                   : Payload::kUncompressed,
              pool_,
              codec_));
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    // Merge buffers.
    DLOG(INFO) << "Merged partition: " << partitionId << ", numRows before: " << lastPayload->numRows()
               << ", numRows appended: " << append->numRows() << ", numRows after: " << mergedRows;
    ARROW_ASSIGN_OR_RAISE(auto payload, InMemoryPayload::merge(std::move(lastPayload), std::move(append), pool_));
    if (mergedRows < mergeBufferSize_) {
      // Still not reach merging threshold, save for next merge.
      partitionMergePayload_[partitionId] = std::move(payload);
      return merged;
    }
    // mergedRows == mergeBufferSize_
    merged.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        merged.back(),
        payload->toBlockPayload(
            codec_ != nullptr && payload->numRows() >= compressionThreshold_ ? Payload::kCompressed
                                                                             : Payload::kUncompressed,
            pool_,
            codec_));
    return merged;
  }

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> finishForSpill(
      uint32_t partitionId,
      int64_t& totalBytesToEvict) {
    // We need to check whether the spill source is from compressing/copying the merged buffers.
    if ((partitionInMerge_.has_value() && *partitionInMerge_ == partitionId) || !hasMerged(partitionId)) {
      return std::nullopt;
    }
    auto payload = std::move(partitionMergePayload_[partitionId]);
    totalBytesToEvict += payload->rawSize();
    return payload->toBlockPayload(Payload::kUncompressed, pool_, codec_);
  }

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> finish(uint32_t partitionId) {
    if (!hasMerged(partitionId)) {
      return std::nullopt;
    }
    auto numRows = partitionMergePayload_[partitionId]->numRows();
    // Because this is the last BlockPayload, delay the compression before writing to the final data file.
    auto payloadType =
        (codec_ != nullptr && numRows >= compressionThreshold_) ? Payload::kToBeCompressed : Payload::kUncompressed;
    auto payload = std::move(partitionMergePayload_[partitionId]);
    return payload->toBlockPayload(payloadType, pool_, codec_);
  }

  bool hasMerged(uint32_t partitionId) {
    return partitionMergePayload_.find(partitionId) != partitionMergePayload_.end() &&
        partitionMergePayload_[partitionId] != nullptr;
  }

 private:
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  bool hasComplexType_;
  int32_t compressionThreshold_;
  int32_t mergeBufferSize_;
  int32_t mergeBufferMinSize_;
  std::unordered_map<uint32_t, std::unique_ptr<InMemoryPayload>> partitionMergePayload_;
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

  arrow::Result<std::unique_ptr<BlockPayload>> createBlockPayload(
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      bool reuseBuffers) {
    auto createCompressed = codec_ != nullptr && inMemoryPayload->numRows() >= compressionThreshold_;
    if (reuseBuffers && !createCompressed) {
      // For uncompressed buffers, need to copy before caching.
      RETURN_NOT_OK(inMemoryPayload->copyBuffers(pool_));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        inMemoryPayload->toBlockPayload(
            createCompressed ? Payload::kCompressed : Payload::kUncompressed, pool_, codec_));
    return payload;
  }
};

class LocalPartitionWriter::PayloadCache {
 public:
  PayloadCache(uint32_t numPartitions) : numPartitions_(numPartitions) {}

  arrow::Status cache(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) {
    if (partitionCachedPayload_.find(partitionId) == partitionCachedPayload_.end()) {
      partitionCachedPayload_[partitionId] = std::list<std::unique_ptr<BlockPayload>>{};
    }
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  bool hasCachedPayloads(uint32_t partitionId) {
    return partitionCachedPayload_.find(partitionId) != partitionCachedPayload_.end() &&
        !partitionCachedPayload_[partitionId].empty();
  }

  arrow::Status write(uint32_t partitionId, arrow::io::OutputStream* os) {
    if (hasCachedPayloads(partitionId)) {
      auto& payloads = partitionCachedPayload_[partitionId];
      while (!payloads.empty()) {
        auto payload = std::move(payloads.front());
        payloads.pop_front();
        // Write the cached payload to disk.
        RETURN_NOT_OK(payload->serialize(os));
        compressTime_ += payload->getCompressTime();
        writeTime_ += payload->getWriteTime();
      }
    }
    return arrow::Status::OK();
  }

  bool canSpill() {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (hasCachedPayloads(pid)) {
        return true;
      }
    }
    return false;
  }

  arrow::Result<std::shared_ptr<Spill>> spillAndClose(
      std::shared_ptr<arrow::io::OutputStream> os,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      int64_t& totalBytesToEvict) {
    std::shared_ptr<Spill> diskSpill = nullptr;
    ARROW_ASSIGN_OR_RAISE(auto start, os->Tell());
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (hasCachedPayloads(pid)) {
        auto& payloads = partitionCachedPayload_[pid];
        while (!payloads.empty()) {
          auto payload = std::move(payloads.front());
          payloads.pop_front();
          totalBytesToEvict += payload->rawSize();
          // Spill the cached payload to disk.
          RETURN_NOT_OK(payload->serialize(os.get()));
          compressTime_ += payload->getCompressTime();
          spillTime_ += payload->getWriteTime();

          if (UNLIKELY(!diskSpill)) {
            diskSpill = std::make_unique<Spill>(Spill::SpillType::kBatchedSpill);
          }
          ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());
          DLOG(INFO) << "PayloadCache: Spilled partition " << pid << " file start: " << start << ", file end: " << end
                     << ", file: " << spillFile;
          diskSpill->insertPayload(
              pid, payload->type(), payload->numRows(), payload->isValidityBuffer(), end - start, pool, codec);
          start = end;
        }
      }
    }
    RETURN_NOT_OK(os->Close());
    diskSpill->setSpillFile(spillFile);
    return diskSpill;
  }

  int64_t getCompressTime() const {
    return compressTime_;
  }

  int64_t getSpillTime() const {
    return spillTime_;
  }

  int64_t getWriteTime() const {
    return writeTime_;
  }

 private:
  uint32_t numPartitions_;
  int64_t compressTime_{0};
  int64_t spillTime_{0};
  int64_t writeTime_{0};
  std::unordered_map<uint32_t, std::list<std::unique_ptr<BlockPayload>>> partitionCachedPayload_;
};

LocalPartitionWriter::LocalPartitionWriter(
    uint32_t numPartitions,
    PartitionWriterOptions options,
    arrow::MemoryPool* pool,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs)
    : PartitionWriter(numPartitions, std::move(options), pool), dataFile_(dataFile), localDirs_(localDirs) {
  init();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getShuffleSpillDir(localDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % options_.numSubDirs;
  dirSelection_ = (dirSelection_ + 1) % localDirs_.size();
  return spilledFileDir;
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>> LocalPartitionWriter::openFile(const std::string& file) {
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  auto fd = open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  fchmod(fd, 0644);
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(fd));
  if (options_.bufferedWrite) {
    // The `shuffleFileBufferSize` bytes is a temporary allocation and will be freed with file close.
    // Use default memory pool and count treat the memory as executor memory overhead to avoid unnecessary spill.
    return arrow::io::BufferedOutputStream::Create(options_.shuffleFileBufferSize, arrow::default_memory_pool(), fout);
  }
  return fout;
}

arrow::Status LocalPartitionWriter::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  // When bufferedWrite = true, dataFileOs_->Close doesn't release underlying buffer.
  dataFileOs_.reset();
  return arrow::Status::OK();
}

void LocalPartitionWriter::init() {
  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);

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
    (*spillIter)->openForRead(options_.shuffleFileBufferSize);
    // Read if partition exists in the spilled file and write to the final file.
    while (auto payload = (*spillIter)->nextPayload(partitionId)) {
      // May trigger spill during compression.
      RETURN_NOT_OK(payload->serialize(dataFileOs_.get()));
      compressTime_ += payload->getCompressTime();
      writeTime_ += payload->getWriteTime();
    }
    ++spillIter;
    ARROW_ASSIGN_OR_RAISE(auto ed, dataFileOs_->Tell());
    DLOG(INFO) << "Partition " << partitionId << " spilled from spillResult " << spillId++ << " of bytes " << ed - st;
    totalBytesEvicted_ += (ed - st);
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;

  if (useSpillFileAsDataFile_) {
    RETURN_NOT_OK(finishSpill(false));
    // The last spill has been written to data file.
    auto spill = std::move(spills_.back());
    spills_.pop_back();

    // Merge the remaining partitions from spills.
    if (spills_.size() > 0) {
      for (auto pid = lastEvictPid_ + 1; pid < numPartitions_; ++pid) {
        auto bytesEvicted = totalBytesEvicted_;
        RETURN_NOT_OK(mergeSpills(pid));
        partitionLengths_[pid] = totalBytesEvicted_ - bytesEvicted;
      }
    }

    for (auto pid = 0; pid < numPartitions_; ++pid) {
      while (auto payload = spill->nextPayload(pid)) {
        partitionLengths_[pid] += payload->rawSize();
      }
    }
    writeTime_ = spill->spillTime();
    compressTime_ += spill->compressTime();
  } else {
    RETURN_NOT_OK(finishSpill(true));
    ARROW_ASSIGN_OR_RAISE(dataFileOs_, openFile(dataFile_));

    int64_t endInFinalFile = 0;
    DLOG(INFO) << "LocalPartitionWriter stopped. Total spills: " << spills_.size();
    // Iterator over pid.
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      // Record start offset.
      auto startInFinalFile = endInFinalFile;
      // Iterator over all spilled files.
      // Reading and compressing toBeCompressed payload can trigger spill.
      RETURN_NOT_OK(mergeSpills(pid));
      if (payloadCache_ && payloadCache_->hasCachedPayloads(pid)) {
        RETURN_NOT_OK(payloadCache_->write(pid, dataFileOs_.get()));
      }
      if (merger_) {
        ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid));
        if (merged) {
          // Compressing merged payload can trigger spill.
          RETURN_NOT_OK((*merged)->serialize(dataFileOs_.get()));
          compressTime_ += (*merged)->getCompressTime();
          writeTime_ += (*merged)->getWriteTime();
        }
      }
      ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
      partitionLengths_[pid] = endInFinalFile - startInFinalFile;
    }
  }
  // Close Final file. Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  // Check all spills are merged.
  auto s = 0;
  for (const auto& spill : spills_) {
    compressTime_ += spill->compressTime();
    spillTime_ += spill->spillTime();
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (spill->hasNextPayload(pid)) {
        return arrow::Status::Invalid(
            "Merging from spill " + std::to_string(s) + " is not exhausted. pid: " + std::to_string(pid));
      }
    }
    if (std::filesystem::exists(spill->spillFile()) && !std::filesystem::remove(spill->spillFile())) {
      LOG(WARNING) << "Error while deleting spill file " << spill->spillFile();
    }
    ++s;
  }
  spills_.clear();

  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestSpill(bool isFinal) {
  if (!spiller_ || spiller_->finished()) {
    std::string spillFile;
    std::shared_ptr<arrow::io::OutputStream> os;
    if (isFinal) {
      ARROW_ASSIGN_OR_RAISE(dataFileOs_, openFile(dataFile_));
      spillFile = dataFile_;
      os = dataFileOs_;
      useSpillFileAsDataFile_ = true;
    } else {
      ARROW_ASSIGN_OR_RAISE(spillFile, createTempShuffleFile(nextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(os, openFile(spillFile));
    }
    spiller_ = std::make_unique<LocalSpiller>(
        os, std::move(spillFile), options_.compressionThreshold, payloadPool_.get(), codec_.get());
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::finishSpill(bool close) {
  // Finish the spiller. No compression, no spill.
  if (spiller_ && !spiller_->finished()) {
    auto spiller = std::move(spiller_);
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), spiller->finish(close));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::hashEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    bool hasComplexType) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();

  if (evictType == Evict::kSpill) {
    RETURN_NOT_OK(requestSpill(false));
    ARROW_ASSIGN_OR_RAISE(
        auto payload, inMemoryPayload->toBlockPayload(Payload::kUncompressed, payloadPool_.get(), nullptr));
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  if (!merger_) {
    merger_ =
        std::make_shared<PayloadMerger>(options_, payloadPool_.get(), codec_ ? codec_.get() : nullptr, hasComplexType);
  }
  ARROW_ASSIGN_OR_RAISE(auto merged, merger_->merge(partitionId, std::move(inMemoryPayload), reuseBuffers));
  if (!merged.empty()) {
    if (UNLIKELY(!payloadCache_)) {
      payloadCache_ = std::make_shared<PayloadCache>(numPartitions_);
    }
    for (auto& payload : merged) {
      RETURN_NOT_OK(payloadCache_->cache(partitionId, std::move(payload)));
    }
    merged.clear();
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::sortEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    std::shared_ptr<arrow::Buffer> compressed,
    bool isFinal) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();

  if (lastEvictPid_ != -1 && (partitionId < lastEvictPid_ || (isFinal && !dataFileOs_))) {
    lastEvictPid_ = -1;
    RETURN_NOT_OK(finishSpill(true));
  }
  RETURN_NOT_OK(requestSpill(isFinal));

  auto payloadType = codec_ ? Payload::Type::kCompressed : Payload::Type::kUncompressed;
  ARROW_ASSIGN_OR_RAISE(
      auto payload,
      inMemoryPayload->toBlockPayload(
          payloadType, payloadPool_.get(), codec_ ? codec_.get() : nullptr, std::move(compressed)));
  if (!isFinal) {
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(payload)));
  } else {
    if (spills_.size() > 0) {
      for (auto pid = lastEvictPid_ + 1; pid <= partitionId; ++pid) {
        auto bytesEvicted = totalBytesEvicted_;
        RETURN_NOT_OK(mergeSpills(pid));
        partitionLengths_[pid] = totalBytesEvicted_ - bytesEvicted;
      }
    }
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(payload)));
  }
  lastEvictPid_ = partitionId;
  return arrow::Status::OK();
}

// FIXME: Remove this code path for local partition writer.
arrow::Status LocalPartitionWriter::evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool stop) {
  rawPartitionLengths_[partitionId] += blockPayload->rawSize();

  if (lastEvictPid_ != -1 && partitionId < lastEvictPid_) {
    RETURN_NOT_OK(finishSpill(true));
    lastEvictPid_ = -1;
  }
  RETURN_NOT_OK(requestSpill(stop));

  if (!stop) {
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(blockPayload)));
  } else {
    if (spills_.size() > 0) {
      for (auto pid = lastEvictPid_ + 1; pid <= partitionId; ++pid) {
        auto bytesEvicted = totalBytesEvicted_;
        RETURN_NOT_OK(mergeSpills(pid));
        partitionLengths_[pid] = totalBytesEvicted_ - bytesEvicted;
      }
    }
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(blockPayload)));
  }
  lastEvictPid_ = partitionId;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill(true));

  int64_t reclaimed = 0;
  // Reclaim memory from payloadCache.
  if (payloadCache_ && payloadCache_->canSpill()) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    ARROW_ASSIGN_OR_RAISE(auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
    ARROW_ASSIGN_OR_RAISE(auto os, openFile(spillFile));
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        spills_.back(),
        payloadCache_->spillAndClose(os, spillFile, payloadPool_.get(), codec_.get(), totalBytesToEvict_));
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
    if (reclaimed >= size) {
      *actual = reclaimed;
      return arrow::Status::OK();
    }
  }
  // Then spill payloads from merger. Create uncompressed payloads.
  if (merger_) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finishForSpill(pid, totalBytesToEvict_));
      if (merged.has_value()) {
        RETURN_NOT_OK(requestSpill(false));
        RETURN_NOT_OK(spiller_->spill(pid, std::move(*merged)));
      }
    }
    // This is not accurate. When the evicted partition buffers are not copied, the merged ones
    // are resized from the original buffers thus allocated from partitionBufferPool.
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
    RETURN_NOT_OK(finishSpill(true));
  }
  *actual = reclaimed;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::populateMetrics(ShuffleWriterMetrics* metrics) {
  if (payloadCache_) {
    spillTime_ += payloadCache_->getSpillTime();
    writeTime_ += payloadCache_->getWriteTime();
    compressTime_ += payloadCache_->getCompressTime();
  }

  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesToEvict += totalBytesToEvict_;
  metrics->totalBytesEvicted += totalBytesEvicted_;
  metrics->totalBytesWritten += std::filesystem::file_size(dataFile_);
  metrics->partitionLengths = std::move(partitionLengths_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}
} // namespace gluten
