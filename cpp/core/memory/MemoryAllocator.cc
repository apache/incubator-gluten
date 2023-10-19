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
#include "utils/macros.h"

namespace gluten {

bool ListenableMemoryAllocator::allocate(int64_t size, void** out) {
  listener_->allocationChanged(size);
  bool succeed = delegated_->allocate(size, out);
  if (!succeed) {
    listener_->allocationChanged(-size);
  }
  if (succeed) {
    bytes_ += size;
  }
  return succeed;
}

bool ListenableMemoryAllocator::allocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  listener_->allocationChanged(size * nmemb);
  bool succeed = delegated_->allocateZeroFilled(nmemb, size, out);
  if (!succeed) {
    listener_->allocationChanged(-size * nmemb);
  }
  if (succeed) {
    bytes_ += size * nmemb;
  }
  return succeed;
}

bool ListenableMemoryAllocator::allocateAligned(uint64_t alignment, int64_t size, void** out) {
  listener_->allocationChanged(size);
  bool succeed = delegated_->allocateAligned(alignment, size, out);
  if (!succeed) {
    listener_->allocationChanged(-size);
  }
  if (succeed) {
    bytes_ += size;
  }
  return succeed;
}

bool ListenableMemoryAllocator::reallocate(void* p, int64_t size, int64_t newSize, void** out) {
  int64_t diff = newSize - size;
  listener_->allocationChanged(diff);
  bool succeed = delegated_->reallocate(p, size, newSize, out);
  if (!succeed) {
    listener_->allocationChanged(-diff);
  }
  if (succeed) {
    bytes_ += diff;
  }
  return succeed;
}

bool ListenableMemoryAllocator::reallocateAligned(
    void* p,
    uint64_t alignment,
    int64_t size,
    int64_t newSize,
    void** out) {
  int64_t diff = newSize - size;
  listener_->allocationChanged(diff);
  bool succeed = delegated_->reallocateAligned(p, alignment, size, newSize, out);
  if (!succeed) {
    listener_->allocationChanged(-diff);
  }
  if (succeed) {
    bytes_ += diff;
  }
  return succeed;
}

bool ListenableMemoryAllocator::free(void* p, int64_t size) {
  listener_->allocationChanged(-size);
  bool succeed = delegated_->free(p, size);
  if (!succeed) {
    listener_->allocationChanged(size);
  }
  if (succeed) {
    bytes_ -= size;
  }
  return succeed;
}

int64_t ListenableMemoryAllocator::getBytes() const {
  return bytes_;
}

bool StdMemoryAllocator::allocate(int64_t size, void** out) {
  *out = std::malloc(size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::allocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  *out = std::calloc(nmemb, size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::allocateAligned(uint64_t alignment, int64_t size, void** out) {
  *out = aligned_alloc(alignment, size);
  bytes_ += size;
  return true;
}

bool StdMemoryAllocator::reallocate(void* p, int64_t size, int64_t newSize, void** out) {
  *out = std::realloc(p, newSize);
  bytes_ += (newSize - size);
  return true;
}

bool StdMemoryAllocator::reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) {
  if (newSize <= 0) {
    return false;
  }
  if (newSize <= size) {
    auto aligned = ROUND_TO_LINE(newSize, alignment);
    if (aligned <= size) {
      // shrink-to-fit
      return reallocate(p, size, aligned, out);
    }
  }
  void* reallocatedP = std::aligned_alloc(alignment, newSize);
  if (!reallocatedP) {
    return false;
  }
  memcpy(reallocatedP, p, std::min(size, newSize));
  std::free(p);
  *out = reallocatedP;
  bytes_ += (newSize - size);
  return true;
}

bool StdMemoryAllocator::free(void* p, int64_t size) {
  std::free(p);
  bytes_ -= size;
  return true;
}

int64_t StdMemoryAllocator::getBytes() const {
  return bytes_;
}

std::shared_ptr<MemoryAllocator> defaultMemoryAllocator() {
#if defined(GLUTEN_ENABLE_HBM)
  static std::shared_ptr<MemoryAllocator> alloc = HbwMemoryAllocator::newInstance();
#else
  static std::shared_ptr<MemoryAllocator> alloc = std::make_shared<StdMemoryAllocator>();
#endif
  return alloc;
}

} // namespace gluten
