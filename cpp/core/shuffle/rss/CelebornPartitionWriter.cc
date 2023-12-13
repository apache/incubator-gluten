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
#include "shuffle/BlockPayload.h"
#include "shuffle/Utils.h"
#include "utils/Timer.h"

namespace gluten {

class CelebornEvictHandle final : public Evictor {
 public:
  CelebornEvictHandle(ShuffleWriterOptions* options, RssClient* client, std::vector<int64_t>& bytesEvicted)
      : Evictor(options), client_(client), bytesEvicted_(bytesEvicted) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<Payload> payload) override {
    // Copy payload to arrow buffered os.
    ARROW_ASSIGN_OR_RAISE(
        auto celebornBufferOs, arrow::io::BufferOutputStream::Create(options_->buffer_size, options_->memory_pool));
    RETURN_NOT_OK(payload->serialize(celebornBufferOs.get()));
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
  RssClient* client_;

  std::vector<int64_t>& bytesEvicted_;
};

void CelebornPartitionWriter::init() {
  bytesEvicted_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
  evictor_ = std::make_shared<CelebornEvictHandle>(options_, celebornClient_.get(), bytesEvicted_);
}

arrow::Status CelebornPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  // Push data and collect metrics.
  auto totalBytesEvicted = std::accumulate(bytesEvicted_.begin(), bytesEvicted_.end(), 0LL);
  celebornClient_->stop();
  // Populate metrics.
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += evictor_->getEvictTime();
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted;
  metrics->totalBytesWritten += totalBytesEvicted;
  metrics->partitionLengths = std::move(bytesEvicted_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::finishEvict() {
  return evictor_->finish();
}

arrow::Status CelebornPartitionWriter::evict(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    bool reuseBuffers,
    Evictor::Type evictType) {
  rawPartitionLengths_[partitionId] += getBufferSize(buffers);
  ScopedTimer timer(evictTime_);
  auto payloadType = (codec_ && numRows >= options_->compression_threshold) ? BlockPayload::Type::kCompressed
                                                                            : BlockPayload::Type::kUncompressed;
  ARROW_ASSIGN_OR_RAISE(
      auto payload,
      BlockPayload::fromBuffers(
          payloadType, numRows, std::move(buffers), payloadPool_.get(), codec_ ? codec_.get() : nullptr, false));
  RETURN_NOT_OK(evictor_->evict(partitionId, std::move(payload)));
  return arrow::Status::OK();
}
} // namespace gluten
