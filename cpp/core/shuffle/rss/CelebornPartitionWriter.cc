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

arrow::Status CelebornPartitionWriter::pushPartition(int32_t partitionId, char* data, int64_t size) {
  int32_t celebornBytesSize = celebornClient_->pushPartitionData(partitionId, data, size);
  shuffleWriter_->setPartitionLengths(partitionId, shuffleWriter_->partitionLengths()[partitionId] + celebornBytesSize);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::stop() {
  // push data and collect metrics
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    ARROW_ASSIGN_OR_RAISE(auto payload, shuffleWriter_->createPayloadFromBuffer(pid, false));
    if (payload) {
      RETURN_NOT_OK(processPayload(pid, std::move(payload)));
    }
    shuffleWriter_->setTotalBytesWritten(shuffleWriter_->totalBytesWritten() + shuffleWriter_->partitionLengths()[pid]);
  }
  celebornClient_->stop();
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::processPayload(
    uint32_t partitionId,
    std::unique_ptr<arrow::ipc::IpcPayload> payload) {
  // Copy payload to arrow buffered os.
  int64_t writeTime = 0;
  TIME_NANO_START(writeTime)
  ARROW_ASSIGN_OR_RAISE(
      auto celebornBufferOs,
      arrow::io::BufferOutputStream::Create(
          shuffleWriter_->options().buffer_size, shuffleWriter_->options().memory_pool));
  int32_t metadataLength = 0; // unused
#ifndef SKIPWRITE
  RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
      *payload, shuffleWriter_->options().ipc_write_options, celebornBufferOs.get(), &metadataLength));
#endif
  payload = nullptr; // Invalidate payload immediately.
  TIME_NANO_END(writeTime)
  shuffleWriter_->setTotalWriteTime(shuffleWriter_->totalWriteTime() + writeTime);

  // Push.
  int64_t evictTime = 0;
  ARROW_ASSIGN_OR_RAISE(auto buffer, celebornBufferOs->Finish());
  TIME_NANO_OR_RAISE(
      evictTime,
      pushPartition(partitionId, reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())), buffer->size()));
  shuffleWriter_->setTotalEvictTime(shuffleWriter_->totalEvictTime() + evictTime);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::spill() {
  // No-op because there's no cached data to spill.
  return arrow::Status::OK();
}

CelebornPartitionWriterCreator::CelebornPartitionWriterCreator(std::shared_ptr<RssClient> client)
    : PartitionWriterCreator(), client_(client) {}

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> CelebornPartitionWriterCreator::make(
    ShuffleWriter* shuffleWriter) {
  std::shared_ptr<CelebornPartitionWriter> res(new CelebornPartitionWriter(shuffleWriter, client_));
  RETURN_NOT_OK(res->init());
  return res;
}

} // namespace gluten
