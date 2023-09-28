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
#include <thread>
#include "shuffle/Utils.h"
#include "utils/DebugOut.h"
#include "utils/Timer.h"

namespace gluten {

std::string LocalPartitionWriterBase::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(configuredDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % shuffleWriter_->options().num_sub_dirs;
  dirSelection_ = (dirSelection_ + 1) % configuredDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriterBase::setLocalDirs() {
  ARROW_ASSIGN_OR_RAISE(configuredDirs_, getConfiguredLocalDirs());
  subDirSelection_.assign(configuredDirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (shuffleWriter_->options().data_file.length() == 0) {
    std::string dataFileTemp;
    size_t id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % configuredDirs_.size();
    ARROW_ASSIGN_OR_RAISE(shuffleWriter_->options().data_file, createTempShuffleFile(configuredDirs_[id]));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriterBase::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(shuffleWriter_->options().data_file, true));
  if (shuffleWriter_->options().buffered_write) {
    // Output stream buffer is neither partition buffer memory nor ipc memory.
    ARROW_ASSIGN_OR_RAISE(
        dataFileOs_, arrow::io::BufferedOutputStream::Create(16384, shuffleWriter_->options().memory_pool, fout));
  } else {
    dataFileOs_ = fout;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriterBase::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::init() {
  partitionCachedPayload_.resize(shuffleWriter_->numPartitions());
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
  RETURN_NOT_OK(setLocalDirs());
  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::spill() {
  int64_t evictTime = 0;
  TIME_NANO_START(evictTime)

  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  SpillInfo spillInfo{spilledFile};

  // Spill all cached batches into one file, record their start and length.
  ARROW_ASSIGN_OR_RAISE(auto spilledFileOs, arrow::io::FileOutputStream::Open(spilledFile, true));
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    if (partitionCachedPayload_[pid].size() > 0) {
      ARROW_ASSIGN_OR_RAISE(auto start, spilledFileOs->Tell());
      RETURN_NOT_OK(flushCachedPayloads(pid, spilledFileOs.get()));
      ARROW_ASSIGN_OR_RAISE(auto end, spilledFileOs->Tell());
      spillInfo.partitionSpillInfos.push_back({pid, end - start});
      DEBUG_OUT << "Spilled partition " << pid << " file start: " << start << ", file end: " << end << std::endl;
    }
  }
  RETURN_NOT_OK(spilledFileOs->Close());

  TIME_NANO_END(evictTime)
  shuffleWriter_->setTotalEvictTime(shuffleWriter_->totalEvictTime() + evictTime);

  if (!spillInfo.partitionSpillInfos.empty()) {
    spills_.push_back(std::move(spillInfo));
  } else {
    // No data spilled to this file. Delete the file and discard this SpillInfo.
    RETURN_NOT_OK(fs_->DeleteFile(spilledFile));
  }

  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::stop() {
  int64_t totalBytesEvicted = 0;
  int64_t totalBytesWritten = 0;
  auto numPartitions = shuffleWriter_->numPartitions();

  // Open final file.
  // If options_.buffered_write is set, it will acquire 16KB memory that might trigger spill.
  RETURN_NOT_OK(openDataFile());

  auto writeTimer = Timer();
  writeTimer.start();

  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions; ++pid) {
    bool firstWrite = true;
    // Record start offset.
    ARROW_ASSIGN_OR_RAISE(auto startInFinalFile, dataFileOs_->Tell());
    // Iterator over all spilled files.
    for (auto& spill : spills_) {
      // Read if partition exists in the spilled file and write to the final file.
      if (spill.mergePos < spill.partitionSpillInfos.size() &&
          spill.partitionSpillInfos[spill.mergePos].partitionId == pid) { // A hit.
        if (!spill.inputStream) {
          // Open spilled file.
          ARROW_ASSIGN_OR_RAISE(
              spill.inputStream, arrow::io::MemoryMappedFile::Open(spill.spilledFile, arrow::io::FileMode::READ));
          // Add evict metrics.
          ARROW_ASSIGN_OR_RAISE(auto spilledSize, spill.inputStream->GetSize());
          totalBytesEvicted += spilledSize;
        }

        firstWrite = false;
        auto spillInfo = spill.partitionSpillInfos[spill.mergePos];
        ARROW_ASSIGN_OR_RAISE(auto raw, spill.inputStream->Read(spillInfo.length));
        RETURN_NOT_OK(dataFileOs_->Write(raw));
        // Goto next partition in this spillInfo.
        spill.mergePos++;
      }
    }
    // Write cached batches.
    if (!partitionCachedPayload_[pid].empty()) {
      firstWrite = false;
      RETURN_NOT_OK(flushCachedPayloads(pid, dataFileOs_.get()));
    }
    // Write the last payload.
    writeTimer.stop();
    ARROW_ASSIGN_OR_RAISE(auto lastPayload, shuffleWriter_->createPayloadFromBuffer(pid, false));
    writeTimer.start();
    if (lastPayload) {
      firstWrite = false;
      int32_t metadataLength = 0; // unused
      RETURN_NOT_OK(flushCachedPayload(dataFileOs_.get(), std::move(lastPayload), &metadataLength));
    }
    // Write EOS if any payload written.
    if (shuffleWriter_->options().write_eos && !firstWrite) {
      RETURN_NOT_OK(writeEos(dataFileOs_.get()));
    }
    ARROW_ASSIGN_OR_RAISE(auto endInFinalFile, dataFileOs_->Tell());

    shuffleWriter_->setPartitionLengths(pid, endInFinalFile - startInFinalFile);
  }

  // Close spilled file streams and delete the file.
  for (auto i = 0; i < spills_.size(); ++i) {
    // Check if all spilled data are merged.
    if (spills_[i].mergePos != spills_[i].partitionSpillInfos.size()) {
      return arrow::Status::Invalid("Merging from spilled file NO." + std::to_string(i) + " is out of bound.");
    }
    if (!spills_[i].inputStream) {
      return arrow::Status::Invalid("Spilled file NO. " + std::to_string(i) + " has not been merged.");
    }
    RETURN_NOT_OK(spills_[i].inputStream->Close());
    RETURN_NOT_OK(fs_->DeleteFile(spills_[i].spilledFile));
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

arrow::Status PreferCachePartitionWriter::clearResource() {
  RETURN_NOT_OK(LocalPartitionWriterBase::clearResource());
  spills_.clear();
  return arrow::Status::OK();
}
arrow::Status PreferCachePartitionWriter::processPayload(
    uint32_t partitionId,
    std::unique_ptr<arrow::ipc::IpcPayload> payload) {
  partitionCachedPayload_[partitionId].push_back(std::move(payload));
  return arrow::Status::OK();
}

LocalPartitionWriterCreator::LocalPartitionWriterCreator() : PartitionWriterCreator() {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> LocalPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  auto partitionWriter = std::make_shared<PreferCachePartitionWriter>(shuffleWriter);
  RETURN_NOT_OK(partitionWriter->init());
  return partitionWriter;
}
} // namespace gluten
