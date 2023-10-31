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

class CelebornEvictHandle final : public EvictHandle {
 public:
  CelebornEvictHandle(
      int64_t bufferSize,
      const arrow::ipc::IpcWriteOptions& options,
      arrow::MemoryPool* pool,
      RssClient* client,
      std::vector<int32_t>& bytesEvicted)
      : bufferSize_(bufferSize), options_(options), pool_(pool), client_(client), bytesEvicted_(bytesEvicted) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override {
    // Copy payload to arrow buffered os.
    ARROW_ASSIGN_OR_RAISE(auto celebornBufferOs, arrow::io::BufferOutputStream::Create(bufferSize_, pool_));
    int32_t metadataLength = 0; // unused
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(*payload, options_, celebornBufferOs.get(), &metadataLength));
    payload = nullptr; // Invalidate payload immediately.

    // Push.
    ARROW_ASSIGN_OR_RAISE(auto buffer, celebornBufferOs->Finish());
    bytesEvicted_[partitionId] += client_->pushPartitionData(
        partitionId, reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())), buffer->size());
    return arrow::Status::OK();
  }

  arrow::Status finish() override {
    return arrow::Status::OK();
  }

 private:
  int64_t bufferSize_;
  arrow::ipc::IpcWriteOptions options_;
  arrow::MemoryPool* pool_;
  RssClient* client_;

  std::vector<int32_t>& bytesEvicted_;
};

arrow::Status CelebornPartitionWriter::init() {
  const auto& options = shuffleWriter_->options();
  bytesEvicted_.resize(shuffleWriter_->numPartitions(), 0);
  evictHandle_ = std::make_shared<CelebornEvictHandle>(
      options.buffer_size, options.ipc_write_options, options.memory_pool, celebornClient_.get(), bytesEvicted_);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::stop() {
  // push data and collect metrics
  for (auto pid = 0; pid < shuffleWriter_->numPartitions(); ++pid) {
    ARROW_ASSIGN_OR_RAISE(auto payload, shuffleWriter_->createPayloadFromBuffer(pid, false));
    if (payload) {
      RETURN_NOT_OK(evictHandle_->evict(pid, std::move(payload)));
    }
    shuffleWriter_->setPartitionLengths(pid, bytesEvicted_[pid]);
    shuffleWriter_->setTotalBytesWritten(shuffleWriter_->totalBytesWritten() + bytesEvicted_[pid]);
  }
  celebornClient_->stop();
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::requestNextEvict(bool flush) {
  return arrow::Status::OK();
}

EvictHandle* CelebornPartitionWriter::getEvictHandle() {
  return evictHandle_.get();
}

arrow::Status CelebornPartitionWriter::finishEvict() {
  return evictHandle_->finish();
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
