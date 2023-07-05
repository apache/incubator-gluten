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

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>

#include "arrow/memory_pool.h"

namespace gluten {

class MemoryAllocator {
 public:
  virtual ~MemoryAllocator() = default;

  virtual bool allocate(int64_t size, void** out) = 0;
  virtual bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) = 0;
  virtual bool allocateAligned(uint64_t alignment, int64_t size, void** out) = 0;

  virtual bool reallocate(void* p, int64_t size, int64_t newSize, void** out) = 0;
  virtual bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) = 0;

  virtual bool free(void* p, int64_t size) = 0;

  virtual bool reserveBytes(int64_t size) = 0;
  virtual bool unreserveBytes(int64_t size) = 0;

  virtual int64_t getBytes() const = 0;
};

class AllocationListener {
 public:
  virtual ~AllocationListener() = default;

  // Value of diff can be either positive or negative
  virtual void allocationChanged(int64_t diff) = 0;

 protected:
  AllocationListener() = default;
};

class ListenableMemoryAllocator final : public MemoryAllocator {
 public:
  explicit ListenableMemoryAllocator(MemoryAllocator* delegated, std::shared_ptr<AllocationListener> listener)
      : delegated_(delegated), listener_(std::move(listener)) {}

 public:
  bool allocate(int64_t size, void** out) override;

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override;

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override;

  bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) override;

  bool free(void* p, int64_t size) override;

  bool reserveBytes(int64_t size) override;

  bool unreserveBytes(int64_t size) override;

  int64_t getBytes() const override;

 private:
  MemoryAllocator* delegated_;
  std::shared_ptr<AllocationListener> listener_;
  std::atomic_int64_t bytes_{0};
};

class StdMemoryAllocator final : public MemoryAllocator {
 public:
  bool allocate(int64_t size, void** out) override;

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override;

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override;

  bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) override;

  bool free(void* p, int64_t size) override;

  bool reserveBytes(int64_t size) override;

  bool unreserveBytes(int64_t size) override;

  int64_t getBytes() const override;

 private:
  std::atomic_int64_t bytes_{0};
};

std::shared_ptr<MemoryAllocator> defaultMemoryAllocator();

} // namespace gluten
