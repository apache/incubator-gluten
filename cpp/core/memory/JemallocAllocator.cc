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

#include "JemallocAllocator.h"
#include "MemoryAllocator.h"
#include <jemalloc/jemalloc.h>
#include "utils/macros.h"

namespace gluten {

std::shared_ptr<MemoryAllocator> JemallocMemoryAllocator::newInstance() {
  return std::make_shared<JemallocMemoryAllocator>();
}

bool JemallocMemoryAllocator::allocate(int64_t size, void** out) {
  *out = je_gluten_malloc(size);
  if (*out == nullptr) {
    return false;
  }
  bytes_ += size;
  return true;
}

bool JemallocMemoryAllocator::allocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  *out = je_gluten_calloc(nmemb, size);
  if (*out == nullptr) {
    return false;
  }
  bytes_ += size;
  return true;
}

bool JemallocMemoryAllocator::allocateAligned(uint64_t alignment, int64_t size, void** out) {
  *out = je_gluten_mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(static_cast<size_t>(alignment)));
  if (*out == nullptr) {
    return false;
  }
  bytes_ += size;
  return true;
}

bool JemallocMemoryAllocator::reallocate(void* p, int64_t size, int64_t newSize, void** out) {
  *out = je_gluten_realloc(p, newSize);
  if (*out == nullptr) {
    return false;
  }
  bytes_ += (newSize - size);
  return true;
}

bool JemallocMemoryAllocator::reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) {
  if (newSize <= 0) {
    return false;
  }
  *out = je_gluten_rallocx(*out, static_cast<size_t>(newSize), MALLOCX_ALIGN(static_cast<size_t>(alignment)));
  if (*out == nullptr) {
    return false;
  }
  bytes_ += (newSize - size);
  return true;
}

bool JemallocMemoryAllocator::free(void* p, int64_t size, int64_t alignment) {
  je_gluten_sdallocx(p, static_cast<size_t>(size), MALLOCX_ALIGN(static_cast<size_t>(alignment)));
  bytes_ -= size;
  return true;
}

int64_t JemallocMemoryAllocator::getBytes() const {
  return bytes_;
}

int64_t JemallocMemoryAllocator::peakBytes() const {
  return 0;
}

} // namespace gluten
