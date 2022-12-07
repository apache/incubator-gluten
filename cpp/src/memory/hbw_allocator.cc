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

#include "hbw_allocator.h"

#include <hbwmalloc.h>
#include "allocator.h"

namespace gluten {

bool HbwMemoryAllocator::Allocate(int64_t size, void** out) {
  *out = hbw_malloc(size);
  bytes_ += size;
  return true;
}

bool HbwMemoryAllocator::AllocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  *out = hbw_calloc(nmemb, size);
  bytes_ += size;
  return true;
}

bool HbwMemoryAllocator::AllocateAligned(uint16_t alignment, int64_t size, void** out) {
  if (hbw_posix_memalign(out, alignment, size) != 0) {
    return false;
  }
  bytes_ += size;
  return true;
}

bool HbwMemoryAllocator::Reallocate(void* p, int64_t size, int64_t new_size, void** out) {
  *out = hbw_realloc(p, new_size);
  bytes_ += (new_size - size);
  return true;
}

bool HbwMemoryAllocator::ReallocateAligned(void* p, uint16_t alignment, int64_t size, int64_t new_size, void** out) {
  if (new_size <= 0) {
    return false;
  }
  void* reallocated_p = nullptr;
  if (hbw_posix_memalign(&reallocated_p, alignment, new_size) != 0) {
    return false;
  }
  memcpy(reallocated_p, p, std::min(size, new_size));
  hbw_free(p);
  *out = reallocated_p;
  bytes_ += (new_size - size);
  return true;
}

bool HbwMemoryAllocator::Free(void* p, int64_t size) {
  hbw_free(p);
  bytes_ -= size;
  return true;
}

int64_t HbwMemoryAllocator::GetBytes() const {
  return bytes_;
}

} // namespace gluten
