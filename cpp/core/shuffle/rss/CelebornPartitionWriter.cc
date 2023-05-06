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

arrow::Result<std::shared_ptr<CelebornPartitionWriter>> CelebornPartitionWriter::create(
    ShuffleWriter* shuffleWriter,
    int32_t numPartitions) {
  std::shared_ptr<CelebornPartitionWriter> res(new CelebornPartitionWriter(shuffleWriter, numPartitions));
  RETURN_NOT_OK(res->init());
  return res;
}

arrow::Status CelebornPartitionWriter::init() {
  celebornClient_ = std::move(shuffle_writer_->options().celeborn_client);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::evictPartition(int32_t partitionId) {
  int64_t tempTotalTime = 0;
  TIME_NANO_OR_RAISE(tempTotalTime, writeArrowToOutputStream(partitionId));
  shuffle_writer_->setTotalWriteTime(shuffle_writer_->totalWriteTime() + tempTotalTime);
  TIME_NANO_OR_RAISE(tempTotalTime, pushPartition(partitionId));
  shuffle_writer_->setTotalEvictTime(shuffle_writer_->totalEvictTime() + tempTotalTime);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::pushPartition(int32_t partitionId) {
  auto buffer = celebornBufferOs_->Finish();
  int32_t size = buffer->get()->size();
  char* dst = reinterpret_cast<char*>(buffer->get()->mutable_data());
  celebornClient_->pushPartitonData(partitionId, dst, size);
  shuffle_writer_->partitionCachedRecordbatch()[partitionId].clear();
  shuffle_writer_->setPartitionCachedRecordbatchSize(partitionId, 0);
  shuffle_writer_->setPartitionLengths(partitionId, shuffle_writer_->partitionLengths()[partitionId] + size);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::stop() {
  // push data and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(shuffle_writer_->createRecordBatchFromBuffer(pid, true));
    if (shuffle_writer_->partitionCachedRecordbatchSize()[pid] > 0) {
      RETURN_NOT_OK(evictPartition(pid));
    }
    shuffle_writer_->setTotalBytesWritten(
        shuffle_writer_->totalBytesWritten() + shuffle_writer_->partitionLengths()[pid]);
  }
  if (shuffle_writer_->combineBuffer() != nullptr) {
    shuffle_writer_->combineBuffer().reset();
  }
  shuffle_writer_->partitionBuffer().clear();
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::writeArrowToOutputStream(int32_t partitionId) {
  ARROW_ASSIGN_OR_RAISE(
      celebornBufferOs_,
      arrow::io::BufferOutputStream::Create(
          shuffle_writer_->options().buffer_size, shuffle_writer_->options().memory_pool.get()));
  int32_t metadataLength = 0; // unused
#ifndef SKIPWRITE
  for (auto& payload : shuffle_writer_->partitionCachedRecordbatch()[partitionId]) {
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *payload, shuffle_writer_->options().ipc_write_options, celebornBufferOs_.get(), &metadataLength));
    payload = nullptr;
  }
#endif
  return arrow::Status::OK();
}

} // namespace gluten
