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

#include "shuffle/ShuffleMemoryPool.h"

namespace gluten {
gluten::ShuffleMemoryPool::ShuffleMemoryPool(arrow::MemoryPool* pool) : pool_(pool) {}

arrow::Status ShuffleMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  auto before = pool_->bytes_allocated();
  auto status = pool_->Allocate(size, alignment, out);
  if (status.ok()) {
    bytesAllocated_ += (pool_->bytes_allocated() - before);
    if (bytesAllocated_ > peakBytesAllocated_) {
      peakBytesAllocated_ = bytesAllocated_;
    }
  }
  return status;
}

arrow::Status ShuffleMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) {
  auto before = pool_->bytes_allocated();
  auto status = pool_->Reallocate(old_size, new_size, alignment, ptr);
  if (status.ok()) {
    bytesAllocated_ += (pool_->bytes_allocated() - before);
    if (bytesAllocated_ > peakBytesAllocated_) {
      peakBytesAllocated_ = bytesAllocated_;
    }
  }
  return status;
}

void ShuffleMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  auto before = pool_->bytes_allocated();
  pool_->Free(buffer, size, alignment);
  bytesAllocated_ += (pool_->bytes_allocated() - before);
}

int64_t ShuffleMemoryPool::bytes_allocated() const {
  return bytesAllocated_;
}

int64_t ShuffleMemoryPool::max_memory() const {
  return peakBytesAllocated_;
}

std::string ShuffleMemoryPool::backend_name() const {
  return pool_->backend_name();
}

int64_t ShuffleMemoryPool::total_bytes_allocated() const {
  return pool_->total_bytes_allocated();
}

int64_t ShuffleMemoryPool::num_allocations() const {
  throw pool_->num_allocations();
}
} // namespace gluten
