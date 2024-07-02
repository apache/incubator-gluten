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
#include <limits>
#include <mutex>
#include <unordered_map>
#include "utils/exception.h"

namespace gluten {
using ResourceHandle = uint32_t;
static_assert(std::numeric_limits<ResourceHandle>::min() == 0);

template <typename T, typename F>
T safeCast(F f) {
  GLUTEN_CHECK(sizeof(T) <= sizeof(F), "Vain safe casting");
  F min = 0;
  F max = static_cast<F>(std::numeric_limits<T>::max());
  GLUTEN_CHECK(f >= min, "Safe casting a negative number");
  GLUTEN_CHECK(f <= max, "Number overflow");
  return static_cast<T>(f);
}

/**
 * An utility class that map resource handle to its shared pointers.
 * Not thread-safe.
 * @tparam TResource class of the object to hold.
 */
template <typename TResource>
class ResourceMap {
 public:
  ResourceMap() : resourceId_(kInitResourceId) {}

  ResourceHandle insert(TResource holder) {
    ResourceHandle result = safeCast<ResourceHandle>(resourceId_++);
    const std::lock_guard<std::mutex> lock(mtx_);
    map_.insert(std::pair<ResourceHandle, TResource>(result, holder));
    return result;
  }

  void erase(ResourceHandle moduleId) {
    const std::lock_guard<std::mutex> lock(mtx_);
    GLUTEN_CHECK(map_.erase(moduleId) == 1, "Module not found in resource map: " + std::to_string(moduleId));
  }

  TResource lookup(ResourceHandle moduleId) {
    const std::lock_guard<std::mutex> lock(mtx_);
    auto it = map_.find(moduleId);
    GLUTEN_CHECK(it != map_.end(), "Module not found in resource map: " + std::to_string(moduleId));
    return it->second;
  }

  void clear() {
    const std::lock_guard<std::mutex> lock(mtx_);
    map_.clear();
  }

  size_t size() {
    const std::lock_guard<std::mutex> lock(mtx_);
    return map_.size();
  }

  size_t nextId() {
    return resourceId_;
  }

 private:
  // Initialize the resource id starting value to a number greater than zero
  // to allow for easier debugging of uninitialized java variables.
  static constexpr size_t kInitResourceId = 4;

  std::atomic<size_t> resourceId_{0};

  // map from resource ids returned to Java and resource pointers
  std::unordered_map<ResourceHandle, TResource> map_;
  std::mutex mtx_;
};

} // namespace gluten
