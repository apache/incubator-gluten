/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 */

#pragma once

#include <jni.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace gluten {

/**
 * An utility class that map module id to module pointers.
 * @tparam Holder class of the object to hold.
 */
template <typename Holder>
class ConcurrentMap {
 public:
  ConcurrentMap() : moduleId_(kInitModuleId) {}

  jlong insert(Holder holder) {
    std::lock_guard<std::mutex> lock(mtx_);
    jlong result = moduleId_++;
    map_.insert(std::pair<jlong, Holder>(result, holder));
    return result;
  }

  void erase(jlong moduleId) {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.erase(moduleId);
  }

  Holder lookup(jlong moduleId) {
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

  int64_t moduleId_;
  std::mutex mtx_;

  // map from module ids returned to Java and module pointers
  std::unordered_map<jlong, Holder> map_;
};

} // namespace gluten
