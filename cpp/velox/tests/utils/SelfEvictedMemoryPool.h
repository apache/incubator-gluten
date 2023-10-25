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

#pragma once

#include <arrow/memory_pool.h>
#include "memory/Evictable.h"

namespace gluten {

class SelfEvictedMemoryPool : public arrow::MemoryPool {
 public:
  explicit SelfEvictedMemoryPool(arrow::MemoryPool* pool) : pool_(pool) {}

  void setCapacity(int64_t capacity) {
    capacity_ = capacity;
  }

  int64_t capacity() const {
    return capacity_;
  }

  void setEvictable(Evictable* evictable) {
    evictable_ = evictable;
  }

  arrow::MemoryPool* delegated() {
    return pool_;
  }

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    RETURN_NOT_OK(CheckEvict(size));
    return pool_->Allocate(size, alignment, out);
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
    if (new_size > old_size) {
      RETURN_NOT_OK(CheckEvict(new_size - old_size));
    }
    return pool_->Reallocate(old_size, new_size, alignment, ptr);
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    return pool_->Free(buffer, size, alignment);
  }

  int64_t bytes_allocated() const override {
    return pool_->bytes_allocated();
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return pool_->backend_name();
  }

  int64_t total_bytes_allocated() const override {
    return pool_->total_bytes_allocated();
  }

  int64_t num_allocations() const override {
    throw pool_->num_allocations();
  }

 private:
  arrow::Status CheckEvict(int64_t size) {
    if (size > capacity_ - pool_->bytes_allocated()) {
      // Self evict.
      int64_t actual;
      RETURN_NOT_OK(evictable_->evictFixedSize(size, &actual));
      if (size > capacity_ - pool_->bytes_allocated()) {
        return arrow::Status::OutOfMemory(
            "Failed to allocate after evict. Capacity: ",
            capacity_,
            ", Requested: ",
            size,
            ", Evicted: ",
            actual,
            ", Allocated: ",
            pool_->bytes_allocated());
      }
    }
    return arrow::Status::OK();
  }

  arrow::MemoryPool* pool_;
  Evictable* evictable_;
  uint64_t capacity_{std::numeric_limits<uint64_t>::max()};
};

} // namespace gluten
