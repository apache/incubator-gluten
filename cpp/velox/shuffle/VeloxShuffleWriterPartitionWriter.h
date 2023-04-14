#pragma once

namespace gluten {

class VeloxShuffleWriter::PartitionWriter {
 public:
  PartitionWriter(VeloxShuffleWriter* shuffle_writer, uint32_t partition_id)
      : shuffle_writer_(shuffle_writer), partition_id_(partition_id) {}

  arrow::Status Spill() {
#ifndef SKIPWRITE
    RETURN_NOT_OK(EnsureOpened());
#endif
    RETURN_NOT_OK(WriteRecordBatchPayload(spilled_file_os_.get()));
    ClearCache();
    return arrow::Status::OK();
  }

  arrow::Status WriteCachedRecordBatchAndClose() {
    const auto& data_file_os = shuffle_writer_->data_file_os_;
    ARROW_ASSIGN_OR_RAISE(auto before_write, data_file_os->Tell());

    if (shuffle_writer_->options_.write_schema) {
      RETURN_NOT_OK(WriteSchemaPayload(data_file_os.get()));
    }

    if (spilled_file_opened_) {
      RETURN_NOT_OK(spilled_file_os_->Close());
      RETURN_NOT_OK(MergeSpilled());
    } else {
      if (shuffle_writer_->partition_cached_recordbatch_size_[partition_id_] == 0) {
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
      ARROW_ASSIGN_OR_RAISE(spilled_file_, CreateTempShuffleFile(shuffle_writer_->NextSpilledFileDir()));
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
    RETURN_NOT_OK(shuffle_writer_->data_file_os_->Write(buffer));

    // close spilled file streams and delete the file
    RETURN_NOT_OK(spilled_file_is_->Close());
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    RETURN_NOT_OK(fs->DeleteFile(spilled_file_));
    bytes_spilled += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status WriteSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, shuffle_writer_->GetSchemaPayload());
    int32_t metadata_length = 0; // unused
    RETURN_NOT_OK(
        arrow::ipc::WriteIpcPayload(*payload, shuffle_writer_->options_.ipc_write_options, os, &metadata_length));
    return arrow::Status::OK();
  }

  arrow::Status WriteRecordBatchPayload(arrow::io::OutputStream* os) {
    int32_t metadata_length = 0; // unused
#ifndef SKIPWRITE
    for (auto& payload : shuffle_writer_->partition_cached_recordbatch_[partition_id_]) {
      RETURN_NOT_OK(
          arrow::ipc::WriteIpcPayload(*payload, shuffle_writer_->options_.ipc_write_options, os, &metadata_length));
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
    shuffle_writer_->partition_cached_recordbatch_[partition_id_].clear();
    shuffle_writer_->partition_cached_recordbatch_size_[partition_id_] = 0;
  }

  VeloxShuffleWriter* shuffle_writer_;
  uint32_t partition_id_;
  std::string spilled_file_;
  std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;

  bool spilled_file_opened_ = false;
}; // class PartitionWriter

} // namespace gluten
