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

#include "MemoryAllocator.h"

namespace gluten {

class HbwMemoryAllocator final : public MemoryAllocator {
 public:
  static std::shared_ptr<MemoryAllocator> newInstance();

  bool allocate(int64_t size, void** out) override;

  bool allocateZeroFilled(int64_t nmemb, int64_t size, void** out) override;

  bool allocateAligned(uint64_t alignment, int64_t size, void** out) override;

  bool reallocate(void* p, int64_t size, int64_t newSize, void** out) override;

  bool reallocateAligned(void* p, uint64_t alignment, int64_t size, int64_t newSize, void** out) override;

  bool free(void* p, int64_t size) override;

  int64_t getBytes() const override;

  int64_t peakBytes() const override;

 private:
  std::atomic_int64_t bytes_{0};
};

} // namespace gluten
