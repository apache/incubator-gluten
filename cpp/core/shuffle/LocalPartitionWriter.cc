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
    ARROW_ASSIGN_OR_RAISE(shuffleWriter_->options().data_file, createTempShuffleFile(configuredDirs_[0]));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriterBase::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(shuffleWriter_->options().data_file, true));
  if (shuffleWriter_->options().buffered_write) {
    ARROW_ASSIGN_OR_RAISE(
        dataFileOs_, arrow::io::BufferedOutputStream::Create(16384, shuffleWriter_->options().memory_pool.get(), fout));
  } else {
    dataFileOs_ = fout;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriterBase::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  schemaPayload_.reset();
  if (shuffleWriter_->combineBuffer() != nullptr) {
    shuffleWriter_->combineBuffer().reset();
  }
  shuffleWriter_->partitionBuffer().clear();
  return arrow::Status::OK();
}

class PreferEvictPartitionWriter::LocalPartitionWriterInstance {
 public:
  LocalPartitionWriterInstance(
      PreferEvictPartitionWriter* partitionWriter,
      ShuffleWriter* shuffleWriter,
      uint32_t partitionId)
      : partitionWriter_(partitionWriter), shuffleWriter_(shuffleWriter), partitionId_(partitionId) {}

  arrow::Status spill() {
#ifndef SKIPWRITE
    RETURN_NOT_OK(ensureOpened());
#endif
    RETURN_NOT_OK(writeRecordBatchPayload(spilledFileOs_.get()));
    clearCache();
    return arrow::Status::OK();
  }

  arrow::Status writeCachedRecordBatchAndClose() {
    const auto& dataFileOs = partitionWriter_->dataFileOs_;
    ARROW_ASSIGN_OR_RAISE(auto before_write, dataFileOs->Tell());

    if (shuffleWriter_->options().write_schema) {
      RETURN_NOT_OK(writeSchemaPayload(dataFileOs.get()));
    }

    if (spilledFileOpened_) {
      RETURN_NOT_OK(spilledFileOs_->Close());
      RETURN_NOT_OK(mergeSpilled());
    } else {
      if (shuffleWriter_->partitionCachedRecordbatchSize()[partitionId_] == 0) {
        return arrow::Status::Invalid("Partition writer got empty partition");
      }
    }

    RETURN_NOT_OK(writeRecordBatchPayload(dataFileOs.get()));
    RETURN_NOT_OK(writeEos(dataFileOs.get()));
    clearCache();

    ARROW_ASSIGN_OR_RAISE(auto after_write, dataFileOs->Tell());
    partition_length = after_write - before_write;

    return arrow::Status::OK();
  }

  // metrics
  int64_t bytes_spilled = 0;
  int64_t partition_length = 0;
  int64_t compress_time = 0;

 private:
  arrow::Status ensureOpened() {
    if (!spilledFileOpened_) {
      ARROW_ASSIGN_OR_RAISE(spilledFile_, createTempShuffleFile(partitionWriter_->nextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(spilledFileOs_, arrow::io::FileOutputStream::Open(spilledFile_, true));
      spilledFileOpened_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status mergeSpilled() {
    ARROW_ASSIGN_OR_RAISE(
        auto spilled_file_is_, arrow::io::MemoryMappedFile::Open(spilledFile_, arrow::io::FileMode::READ));
    // copy spilled data blocks
    ARROW_ASSIGN_OR_RAISE(auto nbytes, spilled_file_is_->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, spilled_file_is_->Read(nbytes));
    RETURN_NOT_OK(partitionWriter_->dataFileOs_->Write(buffer));

    // close spilled file streams and delete the file
    RETURN_NOT_OK(spilled_file_is_->Close());
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    RETURN_NOT_OK(fs->DeleteFile(spilledFile_));
    bytes_spilled += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status writeSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, partitionWriter_->getSchemaPayload(shuffleWriter_->schema()));
    int32_t metadataLength = 0; // unused
    RETURN_NOT_OK(
        arrow::ipc::WriteIpcPayload(*payload, shuffleWriter_->options().ipc_write_options, os, &metadataLength));
    return arrow::Status::OK();
  }

  arrow::Status writeRecordBatchPayload(arrow::io::OutputStream* os) {
    int32_t metadataLength = 0; // unused
#ifndef SKIPWRITE
    for (auto& payload : shuffleWriter_->partitionCachedRecordbatch()[partitionId_]) {
      RETURN_NOT_OK(
          arrow::ipc::WriteIpcPayload(*payload, shuffleWriter_->options().ipc_write_options, os, &metadataLength));
      payload = nullptr;
    }
#endif
    return arrow::Status::OK();
  }

  arrow::Status writeEos(arrow::io::OutputStream* os) {
    // write EOS
    constexpr int32_t kZeroLength = 0;
    RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(int32_t)));
    RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(int32_t)));
    return arrow::Status::OK();
  }

  void clearCache() {
    shuffleWriter_->partitionCachedRecordbatch()[partitionId_].clear();
    shuffleWriter_->setPartitionCachedRecordbatchSize(partitionId_, 0);
  }

  PreferEvictPartitionWriter* partitionWriter_;
  ShuffleWriter* shuffleWriter_;
  uint32_t partitionId_;
  std::string spilledFile_;
  std::shared_ptr<arrow::io::FileOutputStream> spilledFileOs_;

  bool spilledFileOpened_ = false;
};

arrow::Status PreferEvictPartitionWriter::init() {
  partitionWriterInstances_.resize(shuffleWriter_->numPartitions());
  RETURN_NOT_OK(setLocalDirs());
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> LocalPartitionWriterBase::getSchemaPayload(
    std::shared_ptr<arrow::Schema> schema) {
  if (schemaPayload_ != nullptr) {
    return schemaPayload_;
  }
  schemaPayload_ = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::ipc::DictionaryFieldMapper dictFileMapper; // unused
  RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(
      *schema, shuffleWriter_->options().ipc_write_options, dictFileMapper, schemaPayload_.get()));
  return schemaPayload_;
}

arrow::Status PreferEvictPartitionWriter::evictPartition(int32_t partitionId) {
  if (partitionWriterInstances_[partitionId] == nullptr) {
    partitionWriterInstances_[partitionId] =
        std::make_shared<LocalPartitionWriterInstance>(this, shuffleWriter_, partitionId);
  }
  int64_t tempTotalEvictTime = 0;
  TIME_NANO_OR_RAISE(tempTotalEvictTime, partitionWriterInstances_[partitionId]->spill());
  shuffleWriter_->setTotalEvictTime(shuffleWriter_->totalEvictTime() + tempTotalEvictTime);

  return arrow::Status::OK();
}

arrow::Status PreferEvictPartitionWriter::stop() {
  RETURN_NOT_OK(openDataFile());
  // stop PartitionWriter and collect metrics
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    RETURN_NOT_OK(shuffleWriter_->createRecordBatchFromBuffer(pid, true));
    if (shuffleWriter_->partitionCachedRecordbatchSize()[pid] > 0) {
      if (partitionWriterInstances_[pid] == nullptr) {
        partitionWriterInstances_[pid] = std::make_shared<LocalPartitionWriterInstance>(this, shuffleWriter_, pid);
      }
    }
    if (partitionWriterInstances_[pid] != nullptr) {
      const auto& writer = partitionWriterInstances_[pid];
      int64_t tempTotalWriteTime = 0;
      TIME_NANO_OR_RAISE(tempTotalWriteTime, writer->writeCachedRecordBatchAndClose());
      shuffleWriter_->setTotalWriteTime(tempTotalWriteTime);
      shuffleWriter_->setPartitionLengths(pid, writer->partition_length);
      shuffleWriter_->setTotalBytesWritten(shuffleWriter_->totalBytesWritten() + writer->partition_length);
      shuffleWriter_->setTotalBytesEvicted(shuffleWriter_->totalBytesEvicted() + writer->bytes_spilled);
    } else {
      shuffleWriter_->setPartitionLengths(pid, 0);
    }
  }
  RETURN_NOT_OK(clearResource());
  return arrow::Status::OK();
}

arrow::Status PreferEvictPartitionWriter::clearResource() {
  RETURN_NOT_OK(LocalPartitionWriterBase::clearResource());
  partitionWriterInstances_.clear();
  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::init() {
  RETURN_NOT_OK(setLocalDirs());
  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::evictPartition(int32_t partitionId /* not used */) {
  // TODO: Remove this check.
  if (partitionId != -1) {
    return arrow::Status::Invalid("Cannot spill single partition. Invalid code path.");
  }

  if (inStop_) {
    return arrow::Status::OutOfMemory("Cannot evict partition ", partitionId, " because writer is stopped.");
  }

  if (shuffleWriter_->totalCachedPayloadSize() <= 0) {
    return arrow::Status::OutOfMemory("No partition to evict.");
  }

  int64_t evictTime = 0;
  TIME_NANO_START(evictTime)
  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  SpillInfo spillInfo = {spilledFile};
  // Spill all cached batches into one file, record their start and length.
  ARROW_ASSIGN_OR_RAISE(auto spilledFileOs, arrow::io::FileOutputStream::Open(spilledFile, true));
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    auto cachedPayloadSize = shuffleWriter_->partitionCachedRecordbatchSize()[pid];
    if (cachedPayloadSize > 0) {
      ARROW_ASSIGN_OR_RAISE(auto start, spilledFileOs->Tell());
      RETURN_NOT_OK(flushCachedPayloads(spilledFileOs.get(), shuffleWriter_->partitionCachedRecordbatch()[pid]));
      ARROW_ASSIGN_OR_RAISE(auto end, spilledFileOs->Tell());
      spillInfo.partitionSpillInfos.push_back({pid, start, end - start});
#ifdef GLUTEN_PRINT_DEBUG
      std::cout << "Spilled partition " << pid << " file start: " << start << ", file end: " << end
                << ", cachedPayloadSize: " << cachedPayloadSize << std::endl;
#endif
      // clearCache();
      shuffleWriter_->partitionCachedRecordbatch()[pid].clear();
      shuffleWriter_->setPartitionCachedRecordbatchSize(pid, 0);
    }
  }
  RETURN_NOT_OK(spilledFileOs->Close());
  spills_.push_back(std::move(spillInfo));

  TIME_NANO_END(evictTime)
  shuffleWriter_->setTotalEvictTime(shuffleWriter_->totalEvictTime() + evictTime);

  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::stop() {
  inStop_ = true;

  int64_t totalWriteTime = 0;
  int64_t totalBytesEvicted = 0;
  int64_t totalBytesWritten = 0;
  auto numPartitions = shuffleWriter_->numPartitions();
  auto writeSchema = shuffleWriter_->options().write_schema;

  TIME_NANO_START(totalWriteTime)
  // 0. Open final file
  RETURN_NOT_OK(openDataFile());
  // 1. Open all spilled files, update totalBytesEvicted.
  std::vector<int32_t> spillInfoOffsets(spills_.size(), 0);
  std::vector<std::shared_ptr<arrow::io::MemoryMappedFile>> spilledFiles;
  for (const auto& spill : spills_) {
    ARROW_ASSIGN_OR_RAISE(auto is, arrow::io::MemoryMappedFile::Open(spill.spilledFile, arrow::io::FileMode::READ));
    ARROW_ASSIGN_OR_RAISE(auto spilledSize, is->GetSize());
    totalBytesEvicted += spilledSize;
    spilledFiles.push_back(std::move(is));
  }
  // 2. Iterator over pid
  for (auto pid = 0; pid < numPartitions; ++pid) {
    bool firstWrite = true;
    // 3. Record start offset.
    ARROW_ASSIGN_OR_RAISE(auto startInFinalFile, dataFileOs_->Tell());
    // 4. Iterator over all spilled files
    for (auto i = 0; i < spills_.size(); ++i) {
      auto partitionSpillInfo = spills_[i].partitionSpillInfos[spillInfoOffsets[i]];
      // 5. read if partition exists in the spilled file and write to the final file
      if (partitionSpillInfo.partitionId == pid) { // A hit
        if (firstWrite) {
          // Write schema payload for this partition
          if (writeSchema) {
            RETURN_NOT_OK(writeSchemaPayload(dataFileOs_.get()));
          }
          firstWrite = false;
        }
        ARROW_ASSIGN_OR_RAISE(auto raw, spilledFiles[i]->ReadAt(partitionSpillInfo.start, partitionSpillInfo.length));
        RETURN_NOT_OK(dataFileOs_->Write(raw));
        // Goto next partition in this spillInfo
        spillInfoOffsets[i]++;
      }
    }
    // 6. Write cached batches
    auto cachedPayloadSize = shuffleWriter_->partitionCachedRecordbatchSize()[pid];
    if (cachedPayloadSize > 0) {
      if (firstWrite) {
        // Write schema payload for this partition
        if (writeSchema) {
          RETURN_NOT_OK(writeSchemaPayload(dataFileOs_.get()));
        }
        firstWrite = false;
      }
      RETURN_NOT_OK(flushCachedPayloads(dataFileOs_.get(), shuffleWriter_->partitionCachedRecordbatch()[pid]));
      // clearCache();
      shuffleWriter_->partitionCachedRecordbatch()[pid].clear();
      shuffleWriter_->setPartitionCachedRecordbatchSize(pid, 0);
    }
    // 7. Write the last payload.
    ARROW_ASSIGN_OR_RAISE(auto rb, shuffleWriter_->createArrowRecordBatchFromBuffer(pid, true));
    if (rb) {
      if (firstWrite) {
        // Write schema payload for this partition
        if (writeSchema) {
          RETURN_NOT_OK(writeSchemaPayload(dataFileOs_.get()));
        }
        firstWrite = false;
      }
      // Record rawPartitionLength and flush the last payload.
      ARROW_ASSIGN_OR_RAISE(auto lastPayload, shuffleWriter_->createArrowIpcPayload(*rb, false));
      shuffleWriter_->setRawPartitionLength(
          pid, shuffleWriter_->rawPartitionLengths()[pid] + lastPayload->raw_body_length);
      int32_t metadataLength = 0; // unused
      RETURN_NOT_OK(flushCachedPayload(dataFileOs_.get(), lastPayload, &metadataLength));
    }
    // 8. Write EOS if any payload written.
    if (!firstWrite) {
      RETURN_NOT_OK(writeEos(dataFileOs_.get()));
    }
    ARROW_ASSIGN_OR_RAISE(auto endInFinalFile, dataFileOs_->Tell());

    shuffleWriter_->setPartitionLengths(pid, endInFinalFile - startInFinalFile);
  }

  // 9. close spilled file streams and delete the file
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  for (auto i = 0; i < spills_.size(); ++i) {
    // Check if all spilled data are merged.
    if (spillInfoOffsets[i] != spills_[i].partitionSpillInfos.size()) {
      return arrow::Status::Invalid("Merging from spilled file NO." + std::to_string(i) + " is not exhausted.");
    }
    RETURN_NOT_OK(spilledFiles[i]->Close());
    RETURN_NOT_OK(fs->DeleteFile(spills_[i].spilledFile));
  }

  ARROW_ASSIGN_OR_RAISE(totalBytesWritten, dataFileOs_->Tell());

  TIME_NANO_END(totalWriteTime)

  shuffleWriter_->setTotalWriteTime(totalWriteTime);
  shuffleWriter_->setTotalBytesEvicted(totalBytesEvicted);
  shuffleWriter_->setTotalBytesWritten(totalBytesWritten);

  // 10. Close Final file, Clear buffered resources.
  RETURN_NOT_OK(clearResource());

  return arrow::Status::OK();
}

arrow::Status PreferCachePartitionWriter::clearResource() {
  RETURN_NOT_OK(LocalPartitionWriterBase::clearResource());
  spills_.clear();
  return arrow::Status::OK();
}

LocalPartitionWriterCreator::LocalPartitionWriterCreator(bool preferEvict)
    : PartitionWriterCreator(), preferEvict_(preferEvict) {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> LocalPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  std::shared_ptr<ShuffleWriter::PartitionWriter> res;
  if (preferEvict_) {
    res = std::make_shared<PreferEvictPartitionWriter>(shuffleWriter);
  } else {
    res = std::make_shared<PreferCachePartitionWriter>(shuffleWriter);
  }
  RETURN_NOT_OK(res->init());
  return res;
}
} // namespace gluten
