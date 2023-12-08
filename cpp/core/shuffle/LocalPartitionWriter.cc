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

#include "shuffle/LocalPartitionWriter.h"
#include <random>
#include <thread>
#include "shuffle/Utils.h"
#include "utils/DebugOut.h"
#include "utils/StringUtil.h"
#include "utils/Timer.h"

namespace gluten {

class LocalPartitionWriter::LocalEvictor : public Evictor {
 public:
  LocalEvictor(uint32_t numPartitions, ShuffleWriterOptions* options, const std::shared_ptr<SpillInfo>& spillInfo)
      : Evictor(options), numPartitions_(numPartitions), spillInfo_(spillInfo) {}

  static arrow::Result<std::unique_ptr<LocalEvictor>> create(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::shared_ptr<SpillInfo>& spillInfo,
      Evictor::Type evictType);

  virtual arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) = 0;

  virtual Type evictType() = 0;

 protected:
  uint32_t numPartitions_;
  std::shared_ptr<SpillInfo> spillInfo_;
  std::shared_ptr<arrow::io::FileOutputStream> os_;
};

class CacheEvictor final : public LocalPartitionWriter::LocalEvictor {
 public:
  CacheEvictor(uint32_t numPartitions, ShuffleWriterOptions* options, const std::shared_ptr<SpillInfo>& spillInfo)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillInfo) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override {
    if (partitionCachedPayload_.find(partitionId) == partitionCachedPayload_.end()) {
      partitionCachedPayload_.emplace(partitionId, std::vector<std::unique_ptr<arrow::ipc::IpcPayload>>{});
    }
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::Status finish() override {
    if (partitionCachedPayload_.empty()) {
      return arrow::Status::OK();
    }

    ScopedTimer timer(evictTime_);
    ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillInfo_->spilledFile, true));
    int64_t start = 0;
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (partitionCachedPayload_.find(pid) != partitionCachedPayload_.end()) {
        RETURN_NOT_OK(flushInternal(pid, os_.get()));
        ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
        spillInfo_->partitionSpillInfos.push_back({pid, end - start});
        start = end;
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto written, os_->Tell());
    RETURN_NOT_OK(os_->Close());
    if (written > 0) {
      spillInfo_->empty = false;
    }
    partitionCachedPayload_.clear();
    return arrow::Status::OK();
  }

  arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) override {
    if (partitionCachedPayload_.find(partitionId) != partitionCachedPayload_.end()) {
      RETURN_NOT_OK(flushInternal(partitionId, os));
    }
    return arrow::Status::OK();
  }

  Type evictType() override {
    return Type::kCache;
  }

 private:
  std::unordered_map<uint32_t, std::vector<std::unique_ptr<arrow::ipc::IpcPayload>>> partitionCachedPayload_;

  arrow::Status flushInternal(uint32_t partitionId, arrow::io::OutputStream* os) {
    ScopedTimer timer(evictTime_);
    int32_t metadataLength = 0; // unused
    auto payloads = std::move(partitionCachedPayload_[partitionId]);
    partitionCachedPayload_.erase(partitionId);
    for (auto& payload : payloads) {
      RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(*payload, options_->ipc_write_options, os, &metadataLength));
    }
    return arrow::Status::OK();
  }
};

class FlushOnSpillEvictor final : public LocalPartitionWriter::LocalEvictor {
 public:
  FlushOnSpillEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::shared_ptr<SpillInfo>& spillInfo)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillInfo) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override {
    ScopedTimer timer(evictTime_);
    if (!os_) {
      ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillInfo_->spilledFile, true));
    }
    int32_t metadataLength = 0; // unused.

    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(*payload, options_->ipc_write_options, os_.get(), &metadataLength));
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DEBUG_OUT << "Spilled partition " << partitionId << " file start: " << start << ", file end: " << end << std::endl;
    spillInfo_->partitionSpillInfos.push_back({partitionId, end - start});
    return arrow::Status::OK();
  }

  arrow::Status finish() override {
    if (os_ && !os_->closed()) {
      RETURN_NOT_OK(os_->Close());
      spillInfo_->empty = false;
    }
    return arrow::Status::OK();
  }

  arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) override {
    return arrow::Status::OK();
  }

  Type evictType() override {
    return Type::kFlush;
  }
};

arrow::Result<std::unique_ptr<LocalPartitionWriter::LocalEvictor>> LocalPartitionWriter::LocalEvictor::create(
    uint32_t numPartitions,
    ShuffleWriterOptions* options,
    const std::shared_ptr<SpillInfo>& spillInfo,
    Evictor::Type evictType) {
  switch (evictType) {
    case Evictor::Type::kFlush:
      return std::make_unique<FlushOnSpillEvictor>(numPartitions, options, spillInfo);
    case Type::kCache:
      return std::make_unique<CacheEvictor>(numPartitions, options, spillInfo);
    default:
      return arrow::Status::Invalid("Cannot create Evictor from type Evictor::Type::kStop.");
  }
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(configuredDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % options_->num_sub_dirs;
  dirSelection_ = (dirSelection_ + 1) % configuredDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::setLocalDirs() {
  configuredDirs_ = splitPaths(options_->local_dirs);
  // Shuffle the configured local directories. This prevents each task from using the same directory for spilled files.
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::shuffle(configuredDirs_.begin(), configuredDirs_.end(), engine);
  subDirSelection_.assign(configuredDirs_.size(), 0);
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(options_->data_file));
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
  spills_.clear();
  cachedPartitionBuffers_.clear();
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::init() {
  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
  RETURN_NOT_OK(setLocalDirs());
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::mergeSpills(uint32_t partitionId) {
  for (auto spill : spills_) {
    // Read if partition exists in the spilled file and write to the final file.
    if (spill->mergePos < spill->partitionSpillInfos.size() &&
        spill->partitionSpillInfos[spill->mergePos].partitionId == partitionId) { // A hit.
      if (!spill->inputStream) {
        // Open spilled file.
        ARROW_ASSIGN_OR_RAISE(
            spill->inputStream, arrow::io::MemoryMappedFile::Open(spill->spilledFile, arrow::io::FileMode::READ));
        // Add evict metrics.
        ARROW_ASSIGN_OR_RAISE(auto spilledSize, spill->inputStream->GetSize());
        totalBytesEvicted_ += spilledSize;
      }

      auto spillInfo = spill->partitionSpillInfos[spill->mergePos];
      ARROW_ASSIGN_OR_RAISE(auto raw, spill->inputStream->Read(spillInfo.length));
      RETURN_NOT_OK(dataFileOs_->Write(raw));
      // Goto next partition in this spillInfo.
      spill->mergePos++;
    }
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;

  // Open final file.
  // If options_.buffered_write is set, it will acquire 16KB memory that might trigger spill.
  RETURN_NOT_OK(openDataFile());

  auto writeTimer = Timer();
  writeTimer.start();

  int64_t endInFinalFile = 0;
  int32_t metadataLength = 0; // Unused.
  auto cachedPartitionBuffersIter = cachedPartitionBuffers_.begin();
  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    // Record start offset.
    auto startInFinalFile = endInFinalFile;
    // Iterator over all spilled files.
    RETURN_NOT_OK(mergeSpills(pid));
    // Write cached batches.
    if (evictor_) {
      RETURN_NOT_OK(evictor_->flushCachedPayloads(pid, dataFileOs_.get()));
    }
    // Compress and write the last payload.
    // Stop the timer to prevent counting the compression time into write time.
    if (cachedPartitionBuffersIter != cachedPartitionBuffers_.end() &&
        std::get<0>(*cachedPartitionBuffersIter) == pid) {
      writeTimer.stop();
      ARROW_ASSIGN_OR_RAISE(
          auto payload,
          createPayloadFromBuffers(
              std::get<1>(*cachedPartitionBuffersIter), std::move(std::get<2>(*cachedPartitionBuffersIter))));
      writeTimer.start();
      RETURN_NOT_OK(
          arrow::ipc::WriteIpcPayload(*payload, options_->ipc_write_options, dataFileOs_.get(), &metadataLength));
      cachedPartitionBuffersIter++;
    }
    ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
    if (endInFinalFile != startInFinalFile && options_->write_eos) {
      // Write EOS if any payload written.
      int64_t bytes;
      RETURN_NOT_OK(writeEos(dataFileOs_.get(), &bytes));
      endInFinalFile += bytes;
    }
    partitionLengths_[pid] = endInFinalFile - startInFinalFile;
  }
  RETURN_NOT_OK(finishEvict());

  for (auto spill : spills_) {
    // Check if all spilled data are merged.
    if (spill->mergePos != spill->partitionSpillInfos.size()) {
      return arrow::Status::Invalid("Merging from spilled file out of bound: " + spill->spilledFile);
    }
    // Close spilled file streams and delete the spilled file.
    if (spill->inputStream) {
      RETURN_NOT_OK(spill->inputStream->Close());
    }
    RETURN_NOT_OK(fs_->DeleteFile(spill->spilledFile));
  }
  // Check if all partition buffers are merged.
  ARROW_RETURN_IF(
      cachedPartitionBuffersIter != cachedPartitionBuffers_.end(),
      arrow::Status::Invalid("Not all partition buffers are merged."));

  writeTimer.stop();
  writeTime_ = writeTimer.realTimeUsed();
  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());

  // Close Final file, Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestEvict(Evictor::Type evictType) {
  if (evictor_ && evictor_->evictType() == evictType) {
    return arrow::Status::OK();
  }
  RETURN_NOT_OK(finishEvict());

  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  auto spillInfo = std::make_shared<SpillInfo>(spilledFile);
  spills_.push_back(spillInfo);
  ARROW_ASSIGN_OR_RAISE(evictor_, LocalEvictor::create(numPartitions_, options_, spillInfo, evictType));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::finishEvict() {
  if (evictor_) {
    RETURN_NOT_OK(evictor_->finish());
    evictTime_ += evictor_->getEvictTime();
    auto lastSpillInfo = spills_.back();
    if (lastSpillInfo->empty) {
      RETURN_NOT_OK(fs_->DeleteFile(lastSpillInfo->spilledFile));
      spills_.pop_back();
    }
    evictor_ = nullptr;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evict(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    Evictor::Type evictType) {
  rawPartitionLengths_[partitionId] += getBufferSize(buffers);
  if (evictType == Evictor::Type::kStop) {
    cachedPartitionBuffers_.emplace_back(partitionId, numRows, std::move(buffers));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto payload, createPayloadFromBuffers(numRows, std::move(buffers)));
    RETURN_NOT_OK(requestEvict(evictType));
    RETURN_NOT_OK(evictor_->evict(partitionId, std::move(payload)));
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

arrow::Status LocalPartitionWriter::evictFixedSize(int64_t size, int64_t* actual) {
  auto beforeShrink = options_->memory_pool->bytes_allocated();
  for (auto& item : cachedPartitionBuffers_) {
    auto& buffers = std::get<2>(item);
    for (auto& buffer : buffers) {
      if (!buffer) {
        continue;
      }
      if (auto parent = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffer->parent())) {
        RETURN_NOT_OK(parent->Resize(buffer->size()));
      }
    }
  }
  *actual = beforeShrink - options_->memory_pool->bytes_allocated();
  return arrow::Status::OK();
}

LocalPartitionWriterCreator::LocalPartitionWriterCreator() : PartitionWriterCreator() {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> LocalPartitionWriterCreator::make(
    uint32_t numPartitions,
    ShuffleWriterOptions* options) {
  auto partitionWriter = std::make_shared<LocalPartitionWriter>(numPartitions, options);
  RETURN_NOT_OK(partitionWriter->init());
  return partitionWriter;
}
} // namespace gluten
