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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace gluten {

using ResourceHandle = int64_t;
constexpr static ResourceHandle kInvalidResourceHandle = -1;

/**
 * An utility class that map module id to module pointers.
 * @tparam Holder class of the object to hold.
 */
template <typename Holder>
class ConcurrentMap {
 public:
  ConcurrentMap() : moduleId_(kInitModuleId) {}

  ResourceHandle insert(Holder holder) {
    std::lock_guard<std::mutex> lock(mtx_);
    ResourceHandle result = moduleId_++;
    map_.insert(std::pair<ResourceHandle, Holder>(result, holder));
    return result;
  }

  void erase(ResourceHandle moduleId) {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.erase(moduleId);
  }

  Holder lookup(ResourceHandle moduleId) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = map_.find(moduleId);
    if (it != map_.end()) {
      return it->second;
    }
    return nullptr;
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.clear();
  }

  size_t size() {
    std::lock_guard<std::mutex> lock(mtx_);
    return map_.size();
  }

 private:
  // Initialize the module id starting value to a number greater than zero
  // to allow for easier debugging of uninitialized java variables.
  static constexpr int kInitModuleId = 4;

  ResourceHandle moduleId_;
  std::mutex mtx_;

  // map from module ids returned to Java and module pointers
  std::unordered_map<ResourceHandle, Holder> map_;
};

} // namespace gluten
