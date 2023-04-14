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

#include "MemoryAllocator.h"
#include "HbwAllocator.h"

namespace gluten {

bool ListenableMemoryAllocator::Allocate(int64_t size, void** out) {
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

bool ListenableMemoryAllocator::AllocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
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

bool ListenableMemoryAllocator::AllocateAligned(uint16_t alignment, int64_t size, void** out) {
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

bool ListenableMemoryAllocator::Reallocate(void* p, int64_t size, int64_t new_size, void** out) {
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

bool ListenableMemoryAllocator::ReallocateAligned(
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

bool ListenableMemoryAllocator::Free(void* p, int64_t size) {
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

bool ListenableMemoryAllocator::ReserveBytes(int64_t size) {
  listener_->AllocationChanged(size);
  return true;
}

bool ListenableMemoryAllocator::UnreserveBytes(int64_t size) {
  listener_->AllocationChanged(-size);
  return true;
}

int64_t ListenableMemoryAllocator::GetBytes() const {
  return bytes_;
}

bool StdMemoryAllocator::Allocate(int64_t size, void** out) {
  *out = std::malloc(size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::AllocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  *out = std::calloc(nmemb, size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::AllocateAligned(uint16_t alignment, int64_t size, void** out) {
  *out = aligned_alloc(alignment, size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::Reallocate(void* p, int64_t size, int64_t new_size, void** out) {
  *out = std::realloc(p, new_size);
  bytes_ += (new_size - size);
  return true;
}

bool StdMemoryAllocator::ReallocateAligned(void* p, uint16_t alignment, int64_t size, int64_t new_size, void** out) {
  if (new_size <= 0) {
    return false;
  }
  void* reallocated_p = std::malloc(new_size);
  if (!reallocated_p) {
    return false;
  }
  memcpy(reallocated_p, p, std::min(size, new_size));
  std::free(p);
  *out = reallocated_p;
  bytes_ += (new_size - size);
  return true;
}

bool StdMemoryAllocator::Free(void* p, int64_t size) {
  std::free(p);
  bytes_ -= size;
  return true;
}

bool StdMemoryAllocator::ReserveBytes(int64_t size) {
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::UnreserveBytes(int64_t size) {
  bytes_ -= size;
  return true;
}

int64_t StdMemoryAllocator::GetBytes() const {
  return bytes_;
}

std::shared_ptr<MemoryAllocator> DefaultMemoryAllocator() {
#if defined(GLUTEN_ENABLE_HBM)
  static std::shared_ptr<MemoryAllocator> alloc = HbwMemoryAllocator::NewInstance();
#else
  static std::shared_ptr<MemoryAllocator> alloc = std::make_shared<StdMemoryAllocator>();
#endif
  return alloc;
}

} // namespace gluten
