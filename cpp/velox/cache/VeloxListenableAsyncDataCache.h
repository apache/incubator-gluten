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

#include "memory/AllocationListener.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"

namespace gluten {
class ListenableAsyncDataCache final : public facebook::velox::cache::AsyncDataCache {
 public:
  ListenableAsyncDataCache(
      facebook::velox::memory::MemoryAllocator* allocator,
      AllocationListener* listener,
      std::unique_ptr<facebook::velox::cache::SsdCache> ssdCache = nullptr)
      : facebook::velox::cache::AsyncDataCache(allocator, std::move(ssdCache)) {
        listener_ = listener;
      }

  static std::shared_ptr<ListenableAsyncDataCache> create(
      facebook::velox::memory::MemoryAllocator* allocator,
      gluten::AllocationListener* listener,
      std::unique_ptr<facebook::velox::cache::SsdCache> ssdCache = nullptr);
  uint64_t shrink(uint64_t targetBytes) override;
  facebook::velox::cache::CachePin
  findOrCreate(facebook::velox::cache::RawFileCacheKey key, uint64_t size, folly::SemiFuture<bool>* waitFuture = nullptr) override;

 private:
  AllocationListener* listener_;
  std::recursive_mutex mutex_;
};

} // namespace gluten
