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

class LocalPartitionWriter::LocalEvictHandle : public EvictHandle {
 public:
  LocalEvictHandle(
      uint32_t numPartitions,
      const arrow::ipc::IpcWriteOptions& options,
      const std::shared_ptr<SpillInfo>& spillInfo)
      : numPartitions_(numPartitions), options_(options), spillInfo_(spillInfo) {}

  static std::shared_ptr<LocalEvictHandle> create(
      uint32_t numPartitions,
      const arrow::ipc::IpcWriteOptions& options,
      const std::shared_ptr<SpillInfo>& spillInfo,
      bool flush);

  bool finished() {
    return finished_;
  };

  virtual arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) = 0;

 protected:
  uint32_t numPartitions_;
  arrow::ipc::IpcWriteOptions options_;
  std::shared_ptr<SpillInfo> spillInfo_;

  std::shared_ptr<arrow::io::FileOutputStream> os_;
  bool finished_{false};
};

class CacheEvictHandle final : public LocalPartitionWriter::LocalEvictHandle {
 public:
  CacheEvictHandle(
      uint32_t numPartitions,
      const arrow::ipc::IpcWriteOptions& options,
      const std::shared_ptr<SpillInfo>& spillInfo)
      : LocalPartitionWriter::LocalEvictHandle(numPartitions, options, spillInfo) {
    partitionCachedPayload_.resize(numPartitions);
  }

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override {
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::Status finish() override {
    if (!finished_) {
      ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillInfo_->spilledFile, true));
      int64_t start = 0;
      for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        if (!partitionCachedPayload_[pid].empty()) {
          RETURN_NOT_OK(flushCachedPayloads(pid, os_.get()));
          ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
          spillInfo_->partitionSpillInfos.push_back({pid, end - start});
          start = end;
        }
      }
      partitionCachedPayload_.clear();
      ARROW_ASSIGN_OR_RAISE(auto written, os_->Tell());
      RETURN_NOT_OK(os_->Close());
      if (written > 0) {
        spillInfo_->empty = false;
      }

      finished_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) override {
    if (partitionCachedPayload_[partitionId].empty()) {
      return arrow::Status::OK();
    }

    int32_t metadataLength = 0; // unused
    auto payloads = std::move(partitionCachedPayload_[partitionId]);
    // Clear cached batches before creating the payloads, to avoid spilling this partition.
    partitionCachedPayload_[partitionId].clear();
    for (auto& payload : payloads) {
      RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(*payload, options_, os, &metadataLength));
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<std::vector<std::unique_ptr<arrow::ipc::IpcPayload>>> partitionCachedPayload_;
};

class FlushOnSpillEvictHandle final : public LocalPartitionWriter::LocalEvictHandle {
 public:
  FlushOnSpillEvictHandle(
      uint32_t numPartitions,
      const arrow::ipc::IpcWriteOptions& options,
      const std::shared_ptr<SpillInfo>& spillInfo)
      : LocalPartitionWriter::LocalEvictHandle(numPartitions, options, spillInfo) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override {
    if (!os_) {
      ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillInfo_->spilledFile, true));
    }
    int32_t metadataLength = 0; // unused.

    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(*payload, options_, os_.get(), &metadataLength));
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DEBUG_OUT << "Spilled partition " << partitionId << " file start: " << start << ", file end: " << end << std::endl;
    spillInfo_->partitionSpillInfos.push_back({partitionId, end - start});
    return arrow::Status::OK();
  }

  arrow::Status finish() override {
    if (!finished_) {
      if (os_) {
        RETURN_NOT_OK(os_->Close());
        spillInfo_->empty = false;
      }
      finished_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status flushCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) override {
    return arrow::Status::OK();
  }
};

std::shared_ptr<LocalPartitionWriter::LocalEvictHandle> LocalPartitionWriter::LocalEvictHandle::create(
    uint32_t numPartitions,
    const arrow::ipc::IpcWriteOptions& options,
    const std::shared_ptr<SpillInfo>& spillInfo,
    bool flush) {
  if (flush) {
    return std::make_shared<FlushOnSpillEvictHandle>(numPartitions, options, spillInfo);
  } else {
    return std::make_shared<CacheEvictHandle>(numPartitions, options, spillInfo);
  }
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(configuredDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % shuffleWriter_->options().num_sub_dirs;
  dirSelection_ = (dirSelection_ + 1) % configuredDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::setLocalDirs() {
  configuredDirs_ = splitPaths(shuffleWriter_->options().local_dirs);
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
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(shuffleWriter_->options().data_file));
  if (shuffleWriter_->options().buffered_write) {
    // Output stream buffer is neither partition buffer memory nor ipc memory.
    ARROW_ASSIGN_OR_RAISE(
        dataFileOs_, arrow::io::BufferedOutputStream::Create(16384, shuffleWriter_->options().memory_pool, fout));
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
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::init() {
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
  RETURN_NOT_OK(setLocalDirs());
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop() {
  int64_t totalBytesEvicted = 0;
  int64_t totalBytesWritten = 0;
  auto numPartitions = shuffleWriter_->numPartitions();

  // Open final file.
  // If options_.buffered_write is set, it will acquire 16KB memory that might trigger spill.
  RETURN_NOT_OK(openDataFile());

  auto writeTimer = Timer();
  writeTimer.start();

  int64_t endInFinalFile = 0;
  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions; ++pid) {
    // Record start offset.
    auto startInFinalFile = endInFinalFile;
    // Iterator over all spilled files.
    for (auto spill : spills_) {
      // Read if partition exists in the spilled file and write to the final file.
      if (spill->mergePos < spill->partitionSpillInfos.size() &&
          spill->partitionSpillInfos[spill->mergePos].partitionId == pid) { // A hit.
        if (!spill->inputStream) {
          // Open spilled file.
          ARROW_ASSIGN_OR_RAISE(
              spill->inputStream, arrow::io::MemoryMappedFile::Open(spill->spilledFile, arrow::io::FileMode::READ));
          // Add evict metrics.
          ARROW_ASSIGN_OR_RAISE(auto spilledSize, spill->inputStream->GetSize());
          totalBytesEvicted += spilledSize;
        }

        auto spillInfo = spill->partitionSpillInfos[spill->mergePos];
        ARROW_ASSIGN_OR_RAISE(auto raw, spill->inputStream->Read(spillInfo.length));
        RETURN_NOT_OK(dataFileOs_->Write(raw));
        // Goto next partition in this spillInfo.
        spill->mergePos++;
      }
    }
    // Write cached batches.
    if (evictHandle_ && !evictHandle_->finished()) {
      RETURN_NOT_OK(evictHandle_->flushCachedPayloads(pid, dataFileOs_.get()));
    }
    // Compress and write the last payload.
    // Stop the timer to prevent counting the compression time into write time.
    writeTimer.stop();
    ARROW_ASSIGN_OR_RAISE(auto lastPayload, shuffleWriter_->createPayloadFromBuffer(pid, false));
    writeTimer.start();
    if (lastPayload) {
      int32_t metadataLength = 0; // unused
      RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
          *lastPayload, shuffleWriter_->options().ipc_write_options, dataFileOs_.get(), &metadataLength));
    }
    ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
    if (endInFinalFile != startInFinalFile && shuffleWriter_->options().write_eos) {
      // Write EOS if any payload written.
      int64_t bytes;
      RETURN_NOT_OK(writeEos(dataFileOs_.get(), &bytes));
      endInFinalFile += bytes;
    }

    shuffleWriter_->setPartitionLengths(pid, endInFinalFile - startInFinalFile);
  }

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

  ARROW_ASSIGN_OR_RAISE(totalBytesWritten, dataFileOs_->Tell());

  writeTimer.stop();

  shuffleWriter_->setTotalWriteTime(writeTimer.realTimeUsed());
  shuffleWriter_->setTotalBytesEvicted(totalBytesEvicted);
  shuffleWriter_->setTotalBytesWritten(totalBytesWritten);

  // Close Final file, Clear buffered resources.
  RETURN_NOT_OK(clearResource());

  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestNextEvict(bool flush) {
  RETURN_NOT_OK(finishEvict());
  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  auto spillInfo = std::make_shared<SpillInfo>(spilledFile);
  spills_.push_back(spillInfo);
  evictHandle_ = LocalEvictHandle::create(
      shuffleWriter_->numPartitions(), shuffleWriter_->options().ipc_write_options, spillInfo, flush);
  return arrow::Status::OK();
}

EvictHandle* LocalPartitionWriter::getEvictHandle() {
  if (evictHandle_ && !evictHandle_->finished()) {
    return evictHandle_.get();
  }
  return nullptr;
}

arrow::Status LocalPartitionWriter::finishEvict() {
  if (auto handle = getEvictHandle()) {
    RETURN_NOT_OK(handle->finish());
    // The spilled file should not be empty. However, defensively
    // discard the last SpillInfo to avoid iterating over invalid ones.
    auto lastSpillInfo = spills_.back();
    if (lastSpillInfo->empty) {
      RETURN_NOT_OK(fs_->DeleteFile(lastSpillInfo->spilledFile));
      spills_.pop_back();
    }
  }
  return arrow::Status::OK();
}

LocalPartitionWriterCreator::LocalPartitionWriterCreator() : PartitionWriterCreator() {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> LocalPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  auto partitionWriter = std::make_shared<LocalPartitionWriter>(shuffleWriter);
  RETURN_NOT_OK(partitionWriter->init());
  return partitionWriter;
}
} // namespace gluten
