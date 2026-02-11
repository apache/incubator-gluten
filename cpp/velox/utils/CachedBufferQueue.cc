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

#include "utils/CachedBufferQueue.h"
#include "memory/GpuBufferColumnarBatch.h"

namespace gluten {

void CachedBufferQueue::put(std::shared_ptr<GpuBufferColumnarBatch> batch) {
  std::unique_lock<std::mutex> lock(m_);
  const auto batchSize = batch->numBytes();

  VELOX_CHECK_LE(batchSize, capacity_, "Batch size exceeds queue capacity");

  notFull_.wait(lock, [&]() { return totalSize_ + batchSize <= capacity_; });

  queue_.push(std::move(batch));
  totalSize_ += batchSize;

  notEmpty_.notify_one();
}

std::shared_ptr<GpuBufferColumnarBatch> CachedBufferQueue::get() {
  std::unique_lock<std::mutex> lock(m_);
  notEmpty_.wait(lock, [&]() { return noMoreBatches_ || !queue_.empty(); });

  if (queue_.empty()) {
    return nullptr;
  }
  auto batch = std::move(queue_.front());
  LOG(WARNING) << "Trying to get from cached buffer queue. Queue length: " << queue_.size()
               << ", total size in queue: " << totalSize_ << ", current batch size: " << batch->numBytes() << std::endl;

  queue_.pop();
  totalSize_ -= batch->numBytes();

  notFull_.notify_one();
  return batch;
}

void CachedBufferQueue::noMoreBatches() {
  std::unique_lock<std::mutex> lock(m_);
  noMoreBatches_ = true;
  notFull_.notify_all();
  notEmpty_.notify_all();
}

int64_t CachedBufferQueue::size() const {
  return totalSize_;
}

bool CachedBufferQueue::empty() const {
  return queue_.empty();
}

} // namespace gluten
