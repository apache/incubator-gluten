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

#include "allocator.h"

namespace gluten {
namespace memory {

class HbwMemoryAllocator : public MemoryAllocator {
 public:
  bool Allocate(int64_t size, void** out) override;

  bool AllocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool AllocateAligned(uint16_t alignment, int64_t size, void** out) override;

  bool Reallocate(void* p, int64_t size, int64_t new_size, void** out) override;

  bool ReallocateAligned(void* p, uint16_t alignment, int64_t size, int64_t new_size, void** out)
      override;

  bool Free(void* p, int64_t size) override;

  int64_t GetBytes() override;

 private:
  std::atomic_int64_t bytes_{0};
};

} // namespace memory
} // namespace gluten
