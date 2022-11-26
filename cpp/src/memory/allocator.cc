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

#include "allocator.h"
#include <iostream>
#include "hbw_allocator.h"

bool gluten::memory::ListenableMemoryAllocator::Allocate(int64_t size, void** out) {
  listener_->AllocationChanged(size);
  bool succeed = delegated_->Allocate(size, out);
  if (!succeed) {
    listener_->AllocationChanged(-size);
  }
  if (succeed) {
    bytes_ += size;
  }
  return succeed;
}

bool gluten::memory::ListenableMemoryAllocator::AllocateZeroFilled(
    int64_t nmemb,
    int64_t size,
    void** out) {
  listener_->AllocationChanged(size * nmemb);
  bool succeed = delegated_->AllocateZeroFilled(nmemb, size, out);
  if (!succeed) {
    listener_->AllocationChanged(-size * nmemb);
  }
  if (succeed) {
    bytes_ += size * nmemb;
  }
  return succeed;
}

bool gluten::memory::ListenableMemoryAllocator::AllocateAligned(
    uint16_t alignment,
    int64_t size,
    void** out) {
  listener_->AllocationChanged(size);
  bool succeed = delegated_->AllocateAligned(alignment, size, out);
  if (!succeed) {
    listener_->AllocationChanged(-size);
  }
  if (succeed) {
    bytes_ += size;
  }
  return succeed;
}

bool gluten::memory::ListenableMemoryAllocator::Reallocate(
    void* p,
    int64_t size,
    int64_t new_size,
    void** out) {
  int64_t diff = new_size - size;
  listener_->AllocationChanged(diff);
  bool succeed = delegated_->Reallocate(p, size, new_size, out);
  if (!succeed) {
    listener_->AllocationChanged(-diff);
  }
  if (succeed) {
    bytes_ += diff;
  }
  return succeed;
}

bool gluten::memory::ListenableMemoryAllocator::ReallocateAligned(
    void* p,
    uint16_t alignment,
    int64_t size,
    int64_t new_size,
    void** out) {
  int64_t diff = new_size - size;
  listener_->AllocationChanged(diff);
  bool succeed = delegated_->ReallocateAligned(p, alignment, size, new_size, out);
  if (!succeed) {
    listener_->AllocationChanged(-diff);
  }
  if (succeed) {
    bytes_ += diff;
  }
  return succeed;
}

bool gluten::memory::ListenableMemoryAllocator::Free(void* p, int64_t size) {
  listener_->AllocationChanged(-size);
  bool succeed = delegated_->Free(p, size);
  if (!succeed) {
    listener_->AllocationChanged(size);
  }
  if (succeed) {
    bytes_ -= size;
  }
  return succeed;
}

int64_t gluten::memory::ListenableMemoryAllocator::GetBytes() {
  return bytes_;
}

bool gluten::memory::StdMemoryAllocator::Allocate(int64_t size, void** out) {
  *out = std::malloc(size);
  bytes_ += size;
  return true;
}

bool gluten::memory::StdMemoryAllocator::AllocateZeroFilled(
    int64_t nmemb,
    int64_t size,
    void** out) {
  *out = std::calloc(nmemb, size);
  bytes_ += size;
  return true;
}

bool gluten::memory::StdMemoryAllocator::AllocateAligned(
    uint16_t alignment,
    int64_t size,
    void** out) {
  *out = aligned_alloc(alignment, size);
  bytes_ += size;
  return true;
}

bool gluten::memory::StdMemoryAllocator::Reallocate(
    void* p,
    int64_t size,
    int64_t new_size,
    void** out) {
  *out = std::realloc(p, new_size);
  bytes_ += (new_size - size);
  return true;
}

bool gluten::memory::StdMemoryAllocator::ReallocateAligned(
    void* p,
    uint16_t alignment,
    int64_t size,
    int64_t new_size,
    void** out) {
  if (new_size <= 0) {
    return false;
  }
  void* reallocated_p = std::realloc(p, new_size);
  if (!reallocated_p) {
    return false;
  }
  memcpy(reallocated_p, p, std::min(size, new_size));
  bytes_ += (new_size - size);
  return true;
}

bool gluten::memory::StdMemoryAllocator::Free(void* p, int64_t size) {
  std::free(p);
  bytes_ -= size;
  return true;
}

int64_t gluten::memory::StdMemoryAllocator::GetBytes() {
  return bytes_;
}

arrow::Status gluten::memory::WrappedArrowMemoryPool::Allocate(int64_t size, uint8_t** out) {
  if (!allocator_->Allocate(size, reinterpret_cast<void**>(out))) {
    return arrow::Status::Invalid(
        "WrappedMemoryPool: Error allocating " + std::to_string(size) + " bytes");
  }
  return arrow::Status::OK();
}

arrow::Status gluten::memory::WrappedArrowMemoryPool::Reallocate(
    int64_t old_size,
    int64_t new_size,
    uint8_t** ptr) {
  if (!allocator_->Reallocate(*ptr, old_size, new_size, reinterpret_cast<void**>(ptr))) {
    return arrow::Status::Invalid(
        "WrappedMemoryPool: Error reallocating " + std::to_string(new_size) + " bytes");
  }
  return arrow::Status::OK();
}

void gluten::memory::WrappedArrowMemoryPool::Free(uint8_t* buffer, int64_t size) {
  allocator_->Free(buffer, size);
}

int64_t gluten::memory::WrappedArrowMemoryPool::bytes_allocated() const {
  // fixme use self accountant
  return allocator_->GetBytes();
}

std::string gluten::memory::WrappedArrowMemoryPool::backend_name() const {
  return "gluten allocator";
}

std::shared_ptr<gluten::memory::MemoryAllocator> gluten::memory::DefaultMemoryAllocator() {
#if defined(GLUTEN_ENABLE_HBM)
  static std::shared_ptr<MemoryAllocator> alloc = std::make_shared<HbwMemoryAllocator>();
#else
  static std::shared_ptr<MemoryAllocator> alloc = std::make_shared<StdMemoryAllocator>();
#endif
  return alloc;
}
