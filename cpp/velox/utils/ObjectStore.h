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

#include "utils/ResourceMap.h"
#include <set>

namespace gluten {

class JniObjectStore {
 public:
  static std::unique_ptr<JniObjectStore> create()  {
    return std::unique_ptr<JniObjectStore>(new JniObjectStore());
  }

  ResourceHandle save(std::shared_ptr<void> obj) {
    const std::lock_guard<std::mutex> lock(mtx_);
    ResourceHandle handle = store_.insert(obj);
    aliveObjectHandles_.insert(handle);
    return handle;
  }

  template <typename T>
  std::shared_ptr<T> retrieve(ResourceHandle handle) {
    const std::lock_guard<std::mutex> lock(mtx_);
    std::shared_ptr<void> object = store_.lookup(handle);
    // todo
  }

  void release(ResourceHandle handle) {
    const std::lock_guard<std::mutex> lock(mtx_);
  }

 private:
  JniObjectStore() {};
  ResourceMap<std::shared_ptr<void>> store_;
  std::set<ResourceHandle> aliveObjectHandles_;
  std::mutex mtx_;
};
}