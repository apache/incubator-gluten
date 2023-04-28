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
      LocalPartitionWriter* partition_writer,
      ShuffleWriter* shuffle_writer,
      uint32_t partition_id)
      : partition_writer_(partition_writer), shuffle_writer_(shuffle_writer), partition_id_(partition_id) {}

  arrow::Status Spill() {
#ifndef SKIPWRITE
    RETURN_NOT_OK(EnsureOpened());
#endif
    RETURN_NOT_OK(WriteRecordBatchPayload(spilled_file_os_.get()));
    ClearCache();
    return arrow::Status::OK();
  }

  arrow::Status WriteCachedRecordBatchAndClose() {
    const auto& data_file_os = partition_writer_->data_file_os_;
    ARROW_ASSIGN_OR_RAISE(auto before_write, data_file_os->Tell());

    if (shuffle_writer_->Options().write_schema) {
      RETURN_NOT_OK(WriteSchemaPayload(data_file_os.get()));
    }

    if (spilled_file_opened_) {
      RETURN_NOT_OK(spilled_file_os_->Close());
      RETURN_NOT_OK(MergeSpilled());
    } else {
      if (shuffle_writer_->PartitionCachedRecordbatchSize()[partition_id_] == 0) {
        return arrow::Status::Invalid("Partition writer got empty partition");
      }
    }

    RETURN_NOT_OK(WriteRecordBatchPayload(data_file_os.get()));
    RETURN_NOT_OK(WriteEOS(data_file_os.get()));
    ClearCache();

    ARROW_ASSIGN_OR_RAISE(auto after_write, data_file_os->Tell());
    partition_length = after_write - before_write;

    return arrow::Status::OK();
  }

  // metrics
  int64_t bytes_spilled = 0;
  int64_t partition_length = 0;
  int64_t compress_time = 0;

 private:
  arrow::Status EnsureOpened() {
    if (!spilled_file_opened_) {
      ARROW_ASSIGN_OR_RAISE(spilled_file_, CreateTempShuffleFile(partition_writer_->NextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(spilled_file_os_, arrow::io::FileOutputStream::Open(spilled_file_, true));
      spilled_file_opened_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeSpilled() {
    ARROW_ASSIGN_OR_RAISE(
        auto spilled_file_is_, arrow::io::MemoryMappedFile::Open(spilled_file_, arrow::io::FileMode::READ));
    // copy spilled data blocks
    ARROW_ASSIGN_OR_RAISE(auto nbytes, spilled_file_is_->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, spilled_file_is_->Read(nbytes));
    RETURN_NOT_OK(partition_writer_->data_file_os_->Write(buffer));

    // close spilled file streams and delete the file
    RETURN_NOT_OK(spilled_file_is_->Close());
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    RETURN_NOT_OK(fs->DeleteFile(spilled_file_));
    bytes_spilled += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status WriteSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, partition_writer_->GetSchemaPayload(shuffle_writer_->Schema()));
    int32_t metadata_length = 0; // unused
    RETURN_NOT_OK(
        arrow::ipc::WriteIpcPayload(*payload, shuffle_writer_->Options().ipc_write_options, os, &metadata_length));
    return arrow::Status::OK();
  }

  arrow::Status WriteRecordBatchPayload(arrow::io::OutputStream* os) {
    int32_t metadata_length = 0; // unused
#ifndef SKIPWRITE
    for (auto& payload : shuffle_writer_->PartitionCachedRecordbatch()[partition_id_]) {
      RETURN_NOT_OK(
          arrow::ipc::WriteIpcPayload(*payload, shuffle_writer_->Options().ipc_write_options, os, &metadata_length));
      payload = nullptr;
    }
#endif
    return arrow::Status::OK();
  }

  arrow::Status WriteEOS(arrow::io::OutputStream* os) {
    // write EOS
    constexpr int32_t kZeroLength = 0;
    RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(int32_t)));
    RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(int32_t)));
    return arrow::Status::OK();
  }

  void ClearCache() {
    shuffle_writer_->PartitionCachedRecordbatch()[partition_id_].clear();
    shuffle_writer_->SetPartitionCachedRecordbatchSize(partition_id_, 0);
  }

  LocalPartitionWriter* partition_writer_;
  ShuffleWriter* shuffle_writer_;
  uint32_t partition_id_;
  std::string spilled_file_;
  std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;

  bool spilled_file_opened_ = false;
};

arrow::Result<std::shared_ptr<LocalPartitionWriter>> LocalPartitionWriter::Create(
    ShuffleWriter* shuffle_writer,
    int32_t num_partitions) {
  std::shared_ptr<LocalPartitionWriter> res(new LocalPartitionWriter(shuffle_writer, num_partitions));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status LocalPartitionWriter::Init() {
  partition_writer_instance_.resize(num_partitions_);
  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (shuffle_writer_->Options().data_file.length() == 0) {
    std::string data_file_temp;
    ARROW_ASSIGN_OR_RAISE(shuffle_writer_->Options().data_file, CreateTempShuffleFile(configured_dirs_[0]));
  }
  return arrow::Status::OK();
}

std::string LocalPartitionWriter::NextSpilledFileDir() {
  auto spilled_file_dir =
      GetSpilledShuffleFileDir(configured_dirs_[dir_selection_], sub_dir_selection_[dir_selection_]);
  sub_dir_selection_[dir_selection_] =
      (sub_dir_selection_[dir_selection_] + 1) % shuffle_writer_->Options().num_sub_dirs;
  dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
  return spilled_file_dir;
}

arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> LocalPartitionWriter::GetSchemaPayload(
    std::shared_ptr<arrow::Schema> schema) {
  if (schema_payload_ != nullptr) {
    return schema_payload_;
  }
  schema_payload_ = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::ipc::DictionaryFieldMapper dict_file_mapper; // unused
  RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(
      *schema, shuffle_writer_->Options().ipc_write_options, dict_file_mapper, schema_payload_.get()));
  return schema_payload_;
}

arrow::Status LocalPartitionWriter::EvictPartition(int32_t partition_id) {
  if (partition_writer_instance_[partition_id] == nullptr) {
    partition_writer_instance_[partition_id] =
        std::make_shared<LocalPartitionWriterInstance>(this, shuffle_writer_, partition_id);
  }
  int64_t temp_total_evict_time = 0;
  TIME_NANO_OR_RAISE(temp_total_evict_time, partition_writer_instance_[partition_id]->Spill());
  shuffle_writer_->SetTotalEvictTime(temp_total_evict_time);

  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::Stop() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(shuffle_writer_->Options().data_file, true));
  if (shuffle_writer_->Options().buffered_write) {
    ARROW_ASSIGN_OR_RAISE(
        data_file_os_,
        arrow::io::BufferedOutputStream::Create(16384, shuffle_writer_->Options().memory_pool.get(), fout));
  } else {
    data_file_os_ = fout;
  }

  // stop PartitionWriter and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(shuffle_writer_->CreateRecordBatchFromBuffer(pid, true));
    if (shuffle_writer_->PartitionCachedRecordbatchSize()[pid] > 0) {
      if (partition_writer_instance_[pid] == nullptr) {
        partition_writer_instance_[pid] = std::make_shared<LocalPartitionWriterInstance>(this, shuffle_writer_, pid);
      }
    }
    if (partition_writer_instance_[pid] != nullptr) {
      const auto& writer = partition_writer_instance_[pid];
      int64_t temp_total_write_time = 0;
      TIME_NANO_OR_RAISE(temp_total_write_time, writer->WriteCachedRecordBatchAndClose());
      shuffle_writer_->SetTotalWriteTime(temp_total_write_time);
      shuffle_writer_->SetPartitionLengths(pid, writer->partition_length);
      shuffle_writer_->SetTotalBytesWritten(shuffle_writer_->TotalBytesWritten() + writer->partition_length);
      shuffle_writer_->SetTotalBytesEvicted(shuffle_writer_->TotalBytesEvicted() + writer->bytes_spilled);
    } else {
      shuffle_writer_->SetPartitionLengths(pid, 0);
    }
  }
  if (shuffle_writer_->CombineBuffer() != nullptr) {
    shuffle_writer_->CombineBuffer().reset();
  }
  this->schema_payload_.reset();
  shuffle_writer_->PartitionBuffer().clear();

  // close data file output Stream
  RETURN_NOT_OK(data_file_os_->Close());
  return arrow::Status::OK();
}

} // namespace gluten
