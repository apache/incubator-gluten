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

class LocalPartitionWriter::LocalPartitionWriterInstance {
 public:
  LocalPartitionWriterInstance(
      LocalPartitionWriter* partitionWriter,
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
    const auto& dataFileOs = partitionWriter_->data_file_os_;
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
    RETURN_NOT_OK(partitionWriter_->data_file_os_->Write(buffer));

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

  LocalPartitionWriter* partitionWriter_;
  ShuffleWriter* shuffleWriter_;
  uint32_t partitionId_;
  std::string spilledFile_;
  std::shared_ptr<arrow::io::FileOutputStream> spilledFileOs_;

  bool spilledFileOpened_ = false;
};

arrow::Status LocalPartitionWriter::init() {
  partition_writer_instance_.resize(shuffleWriter_->numPartitions());
  ARROW_ASSIGN_OR_RAISE(configured_dirs_, getConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (shuffleWriter_->options().data_file.length() == 0) {
    std::string dataFileTemp;
    ARROW_ASSIGN_OR_RAISE(shuffleWriter_->options().data_file, createTempShuffleFile(configured_dirs_[0]));
  }
  return arrow::Status::OK();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(configured_dirs_[dir_selection_], sub_dir_selection_[dir_selection_]);
  sub_dir_selection_[dir_selection_] =
      (sub_dir_selection_[dir_selection_] + 1) % shuffleWriter_->options().num_sub_dirs;
  dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
  return spilledFileDir;
}

arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> LocalPartitionWriter::getSchemaPayload(
    std::shared_ptr<arrow::Schema> schema) {
  if (schema_payload_ != nullptr) {
    return schema_payload_;
  }
  schema_payload_ = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::ipc::DictionaryFieldMapper dictFileMapper; // unused
  RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(
      *schema, shuffleWriter_->options().ipc_write_options, dictFileMapper, schema_payload_.get()));
  return schema_payload_;
}

arrow::Status LocalPartitionWriter::evictPartition(int32_t partitionId) {
  if (partition_writer_instance_[partitionId] == nullptr) {
    partition_writer_instance_[partitionId] =
        std::make_shared<LocalPartitionWriterInstance>(this, shuffleWriter_, partitionId);
  }
  int64_t tempTotalEvictTime = 0;
  TIME_NANO_OR_RAISE(tempTotalEvictTime, partition_writer_instance_[partitionId]->spill());
  shuffleWriter_->setTotalEvictTime(tempTotalEvictTime);

  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(shuffleWriter_->options().data_file, true));
  if (shuffleWriter_->options().buffered_write) {
    ARROW_ASSIGN_OR_RAISE(
        data_file_os_,
        arrow::io::BufferedOutputStream::Create(16384, shuffleWriter_->options().memory_pool.get(), fout));
  } else {
    data_file_os_ = fout;
  }

  // stop PartitionWriter and collect metrics
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    RETURN_NOT_OK(shuffleWriter_->createRecordBatchFromBuffer(pid, true));
    if (shuffleWriter_->partitionCachedRecordbatchSize()[pid] > 0) {
      if (partition_writer_instance_[pid] == nullptr) {
        partition_writer_instance_[pid] = std::make_shared<LocalPartitionWriterInstance>(this, shuffleWriter_, pid);
      }
    }
    if (partition_writer_instance_[pid] != nullptr) {
      const auto& writer = partition_writer_instance_[pid];
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
  if (shuffleWriter_->combineBuffer() != nullptr) {
    shuffleWriter_->combineBuffer().reset();
  }
  this->schema_payload_.reset();
  shuffleWriter_->partitionBuffer().clear();

  // close data file output Stream
  RETURN_NOT_OK(data_file_os_->Close());
  return arrow::Status::OK();
}

LocalPartitionWriterCreator::LocalPartitionWriterCreator() : PartitionWriterCreator() {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> LocalPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  std::shared_ptr<LocalPartitionWriter> res(new LocalPartitionWriter(shuffleWriter));
  RETURN_NOT_OK(res->init());
  return res;
}
} // namespace gluten
