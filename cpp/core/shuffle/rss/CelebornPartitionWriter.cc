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

#include "CelebornPartitionWriter.h"

namespace gluten {

arrow::Result<std::shared_ptr<CelebornPartitionWriter>> CelebornPartitionWriter::Create(
    ShuffleWriter* shuffle_writer,
    int32_t num_partitions) {
  std::shared_ptr<CelebornPartitionWriter> res(new CelebornPartitionWriter(shuffle_writer, num_partitions));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status CelebornPartitionWriter::Init() {
  celeborn_client_ = std::move(shuffle_writer_->Options().celeborn_client);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::EvictPartition(int32_t partition_id) {
  int64_t temp_total_time = 0;
  TIME_NANO_OR_RAISE(temp_total_time, WriteArrowToOutputStream(partition_id));
  shuffle_writer_->SetTotalWriteTime(shuffle_writer_->TotalWriteTime() + temp_total_time);
  TIME_NANO_OR_RAISE(temp_total_time, PushPartition(partition_id));
  shuffle_writer_->SetTotalEvictTime(shuffle_writer_->TotalEvictTime() + temp_total_time);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::PushPartition(int32_t partition_id) {
  auto buffer = celeborn_buffer_os_->Finish();
  int32_t size = buffer->get()->size();
  char* dst = reinterpret_cast<char*>(buffer->get()->mutable_data());
  celeborn_client_->PushPartitonData(partition_id, dst, size);
  shuffle_writer_->PartitionCachedRecordbatch()[partition_id].clear();
  shuffle_writer_->SetPartitionCachedRecordbatchSize(partition_id, 0);
  shuffle_writer_->SetPartitionLengths(partition_id, shuffle_writer_->PartitionLengths()[partition_id] + size);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::Stop() {
  // push data and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(shuffle_writer_->CreateRecordBatchFromBuffer(pid, true));
    if (shuffle_writer_->PartitionCachedRecordbatchSize()[pid] > 0) {
      RETURN_NOT_OK(EvictPartition(pid));
    }
    shuffle_writer_->SetTotalBytesWritten(
        shuffle_writer_->TotalBytesWritten() + shuffle_writer_->PartitionLengths()[pid]);
  }
  if (shuffle_writer_->CombineBuffer() != nullptr) {
    shuffle_writer_->CombineBuffer().reset();
  }
  shuffle_writer_->PartitionBuffer().clear();
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::WriteArrowToOutputStream(int32_t partition_id) {
  ARROW_ASSIGN_OR_RAISE(
      celeborn_buffer_os_,
      arrow::io::BufferOutputStream::Create(
          shuffle_writer_->Options().buffer_size, shuffle_writer_->Options().memory_pool.get()));
  int32_t metadata_length = 0; // unused
#ifndef SKIPWRITE
  for (auto& payload : shuffle_writer_->PartitionCachedRecordbatch()[partition_id]) {
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *payload, shuffle_writer_->Options().ipc_write_options, celeborn_buffer_os_.get(), &metadata_length));
    payload = nullptr;
  }
#endif
  return arrow::Status::OK();
}

} // namespace gluten
