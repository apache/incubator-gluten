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

#include <map>
#include "utils/ResourceMap.h"

namespace gluten {

// ObjectHandle is a signed int64 consisting of:
// 1. 1 - 32 bits is a signed int32 as the object store's ID;
// 2. 1st bit is always zero to be compatible with jlong;
// 3. 33 - 64 bits is an unsigned int32 as the object's ID;
//
// When the object is tended to be retrieved with its ObjectHandle,
// the program first finds its resident object store, then looks up
// for the object in the store.
using StoreHandle = int32_t;
using ObjectHandle = int64_t;
constexpr static ObjectHandle kInvalidObjectHandle = -1;

template <typename T>
struct SafeSizeOf {
  static constexpr size_t value = sizeof(T);
};

template <>
struct SafeSizeOf<void> {
  static constexpr size_t value = 0;
};

// A store for caching shared-ptrs and enlarging lifecycles of the ptrs to match lifecycle of the store itself by
// default, and also serving release calls to release a ptr in advance. This is typically used in JNI scenario to bind
// a shared-ptr's lifecycle to a Java-side object or some kind of resource manager.
class ObjectStore {
 public:
  static std::unique_ptr<ObjectStore> create();

  static void release(ObjectHandle handle) {
    ResourceHandle storeId = safeCast<ResourceHandle>(handle >> (sizeof(ResourceHandle) * 8));
    ResourceHandle resourceId = safeCast<ResourceHandle>(handle & std::numeric_limits<ResourceHandle>::max());
    auto store = stores().lookup(storeId);
    store->releaseInternal(resourceId);
  }

  template <typename T>
  static std::shared_ptr<T> retrieve(ObjectHandle handle) {
    ResourceHandle storeId = safeCast<ResourceHandle>(handle >> (sizeof(ResourceHandle) * 8));
    ResourceHandle resourceId = safeCast<ResourceHandle>(handle & std::numeric_limits<ResourceHandle>::max());
    auto store = stores().lookup(storeId);
    return store->retrieveInternal<T>(resourceId);
  }

  virtual ~ObjectStore();

  StoreHandle id() {
    return storeId_;
  }

  template <typename T>
  ObjectHandle save(std::shared_ptr<T> obj) {
    const std::lock_guard<std::mutex> lock(mtx_);
    const std::string_view typeName = typeid(T).name();
    const size_t size = SafeSizeOf<T>::value;
    ResourceHandle handle = store_.insert(std::move(obj));
    aliveObjects_.emplace(handle, ObjectDebugInfo{typeName, size});
    return toObjHandle(handle);
  }

 private:
  static ResourceMap<ObjectStore*>& stores();

  struct ObjectDebugInfo {
    const std::string_view typeName;
    const size_t size;
  };

  ObjectHandle toObjHandle(ResourceHandle rh) {
    ObjectHandle prefix = static_cast<ObjectHandle>(storeId_) << (sizeof(ResourceHandle) * 8);
    ObjectHandle objHandle = prefix + rh;
    return objHandle;
  }

  template <typename T>
  std::shared_ptr<T> retrieveInternal(ResourceHandle handle) {
    const std::lock_guard<std::mutex> lock(mtx_);
    std::shared_ptr<void> object = store_.lookup(handle);
    // Programming carefully. This will lead to ub if wrong typename T was passed in.
    auto casted = std::static_pointer_cast<T>(object);
    return casted;
  }

  void releaseInternal(ResourceHandle handle);

  ObjectStore(StoreHandle storeId) : storeId_(storeId){};
  StoreHandle storeId_;
  ResourceMap<std::shared_ptr<void>> store_;
  // Preserves handles of objects in the store in order, with additional attributes associated with them.
  std::map<ResourceHandle, ObjectDebugInfo> aliveObjects_{};
  std::mutex mtx_;
};
} // namespace gluten
