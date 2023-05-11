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

arrow::Status CelebornPartitionWriter::init() {
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::evictPartition(int32_t partitionId) {
  int64_t tempTotalTime = 0;
  TIME_NANO_OR_RAISE(tempTotalTime, writeArrowToOutputStream(partitionId));
  shuffleWriter_->setTotalWriteTime(shuffleWriter_->totalWriteTime() + tempTotalTime);
  TIME_NANO_OR_RAISE(tempTotalTime, pushPartition(partitionId));
  shuffleWriter_->setTotalEvictTime(shuffleWriter_->totalEvictTime() + tempTotalTime);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::pushPartition(int32_t partitionId) {
  auto buffer = celebornBufferOs_->Finish();
  int32_t size = buffer->get()->size();
  char* dst = reinterpret_cast<char*>(buffer->get()->mutable_data());
  celebornClient_->pushPartitonData(partitionId, dst, size);
  shuffleWriter_->partitionCachedRecordbatch()[partitionId].clear();
  shuffleWriter_->setPartitionCachedRecordbatchSize(partitionId, 0);
  shuffleWriter_->setPartitionLengths(partitionId, shuffleWriter_->partitionLengths()[partitionId] + size);
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::stop() {
  // push data and collect metrics
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    RETURN_NOT_OK(shuffleWriter_->createRecordBatchFromBuffer(pid, true));
    if (shuffleWriter_->partitionCachedRecordbatchSize()[pid] > 0) {
      RETURN_NOT_OK(evictPartition(pid));
    }
    shuffleWriter_->setTotalBytesWritten(shuffleWriter_->totalBytesWritten() + shuffleWriter_->partitionLengths()[pid]);
  }
  if (shuffleWriter_->combineBuffer() != nullptr) {
    shuffleWriter_->combineBuffer().reset();
  }
  shuffleWriter_->partitionBuffer().clear();
  return arrow::Status::OK();
};

arrow::Status CelebornPartitionWriter::writeArrowToOutputStream(int32_t partitionId) {
  ARROW_ASSIGN_OR_RAISE(
      celebornBufferOs_,
      arrow::io::BufferOutputStream::Create(
          shuffleWriter_->options().buffer_size, shuffleWriter_->options().memory_pool.get()));
  int32_t metadataLength = 0; // unused
#ifndef SKIPWRITE
  for (auto& payload : shuffleWriter_->partitionCachedRecordbatch()[partitionId]) {
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *payload, shuffleWriter_->options().ipc_write_options, celebornBufferOs_.get(), &metadataLength));
    payload = nullptr;
  }
#endif
  return arrow::Status::OK();
}

CelebornPartitionWriterCreator::CelebornPartitionWriterCreator(std::shared_ptr<CelebornClient> client)
    : PartitionWriterCreator(), client_(client) {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> CelebornPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  std::shared_ptr<CelebornPartitionWriter> res(new CelebornPartitionWriter(shuffleWriter, client_));
  RETURN_NOT_OK(res->init());
  return res;
}

} // namespace gluten
