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

#include "folly/container/F14Map.h"

namespace gluten {

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
    ResourceHandle result = resourceId_++;
    map_.insert(std::pair<ResourceHandle, TResource>(result, holder));
    return result;
  }

  void erase(ResourceHandle moduleId) {
    map_.erase(moduleId);
  }

  TResource lookup(ResourceHandle moduleId) {
    auto it = map_.find(moduleId);
    if (it != map_.end()) {
      return it->second;
    }
    return nullptr;
  }

  void clear() {
    map_.clear();
  }

  size_t size() {
    return map_.size();
  }

 private:
  // Initialize the resource id starting value to a number greater than zero
  // to allow for easier debugging of uninitialized java variables.
  static constexpr int kInitResourceId = 4;

  ResourceHandle resourceId_;

  // map from resource ids returned to Java and resource pointers
  folly::F14FastMap<ResourceHandle, TResource> map_;
};

} // namespace gluten
