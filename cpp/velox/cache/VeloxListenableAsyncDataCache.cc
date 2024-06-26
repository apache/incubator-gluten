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

#include "VeloxListenableAsyncDataCache.h"
#include <iostream>

namespace gluten {

std::shared_ptr<ListenableAsyncDataCache> ListenableAsyncDataCache::create(
    facebook::velox::memory::MemoryAllocator* allocator,
    gluten::AllocationListener* listener,
    std::unique_ptr<facebook::velox::cache::SsdCache> ssdCache) {
  auto cache = std::make_shared<ListenableAsyncDataCache>(allocator, listener, std::move(ssdCache));
  allocator->registerCache(cache);
  return cache;
}

uint64_t ListenableAsyncDataCache::shrink(uint64_t targetBytes) {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  uint64_t shrinkedSize = facebook::velox::cache::AsyncDataCache::shrink(targetBytes);
  listener_->allocationChanged(-shrinkedSize);
  return shrinkedSize;
}
facebook::velox::cache::CachePin ListenableAsyncDataCache::findOrCreate(
    facebook::velox::cache::RawFileCacheKey key,
    uint64_t size,
    folly::SemiFuture<bool>* waitFuture) {
  bool exists = facebook::velox::cache::AsyncDataCache::exists(key);
  if (exists) {
    return facebook::velox::cache::AsyncDataCache::findOrCreate(key, size, waitFuture);
  } else {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    listener_->allocationChanged(size);
    facebook::velox::cache::CacheStats preStats = facebook::velox::cache::AsyncDataCache::refreshStats();
    uint64_t preCacheSize = preStats.tinySize + preStats.largeSize + preStats.tinyPadding + preStats.largePadding;
    auto pin = facebook::velox::cache::AsyncDataCache::findOrCreate(key, size, waitFuture);
    facebook::velox::cache::CacheStats posStats = facebook::velox::cache::AsyncDataCache::refreshStats();
    uint64_t posCacheSize = posStats.tinySize + posStats.largeSize + posStats.tinyPadding + posStats.largePadding;
    listener_->allocationChanged(posCacheSize - preCacheSize - size);
    return pin;
  }
}

} // namespace gluten
