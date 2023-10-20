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

#include <set>
#include "utils/ResourceMap.h"
#include "utils/exception.h"

namespace gluten {

// A store for caching shared-ptrs and enlarging lifecycles of the ptrs to match lifecycle of the store itself by
// default, and also serving release calls to release a ptr in advance. This is typically used in JNI scenario to bind
// a shared-ptr's lifecycle to a Java-side object or some kind of resource manager.
class ObjectStore {
 public:
  static std::unique_ptr<ObjectStore> create() {
    return std::unique_ptr<ObjectStore>(new ObjectStore());
  }

  virtual ~ObjectStore();

  ResourceHandle save(std::shared_ptr<void> obj);

  template <typename T>
  std::shared_ptr<T> retrieve(ResourceHandle handle) {
    const std::lock_guard<std::mutex> lock(mtx_);
    std::shared_ptr<void> object = store_.lookup(handle);
    // Programming carefully. This will lead to ub if wrong typename T was passed in.
    auto casted = std::static_pointer_cast<T>(object);
    return casted;
  }

  void release(ResourceHandle handle);

 private:
  ObjectStore(){};
  ResourceMap<std::shared_ptr<void>> store_;
  std::set<ResourceHandle> aliveObjectHandles_;
  std::mutex mtx_;
};
} // namespace gluten
