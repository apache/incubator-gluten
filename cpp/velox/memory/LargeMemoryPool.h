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

namespace gluten {

class LargeMemoryPool : public arrow::MemoryPool {
 public:
  constexpr static uint64_t kHugePageSize = 1 << 21;
  constexpr static uint64_t kLargeBufferSize = 4 << 21;

  explicit LargeMemoryPool(MemoryPool* pool) : delegated_(pool) {}

  ~LargeMemoryPool();

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  std::string backend_name() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

 protected:
  virtual arrow::Status doAlloc(int64_t size, int64_t alignment, uint8_t** out);

  virtual void doFree(uint8_t* buffer, int64_t size);

  struct BufferAllocated {
    uint8_t* startAddr;
    uint8_t* lastAllocAddr;
    uint64_t size;
    uint64_t allocated;
    uint64_t freed;
  };

  std::vector<BufferAllocated> buffers_;

  MemoryPool* delegated_;
};

// MMapMemoryPool can't be tracked by Spark. Currently only used for test purpose.
class MMapMemoryPool : public LargeMemoryPool {
 public:
  explicit MMapMemoryPool() : LargeMemoryPool(arrow::default_memory_pool()) {}

  ~MMapMemoryPool() override;

 protected:
  arrow::Status doAlloc(int64_t size, int64_t alignment, uint8_t** out) override;

  void doFree(uint8_t* buffer, int64_t size) override;
};

} // namespace gluten
