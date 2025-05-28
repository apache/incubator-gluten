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

#include "ObjectStore.h"
#include <glog/logging.h>
#include <iostream>

//static
std::unique_ptr<gluten::ObjectStore> gluten::ObjectStore::create() {
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  StoreHandle nextId = stores().nextId();
  auto store = std::unique_ptr<gluten::ObjectStore>(new gluten::ObjectStore(nextId));
  StoreHandle storeId = safeCast<StoreHandle>(stores().insert(store.get()));
  GLUTEN_CHECK(storeId == nextId, "Store ID mismatched, this should not happen");
  return store;
}

// static
gluten::ResourceMap<gluten::ObjectStore*>& gluten::ObjectStore::stores() {
  static gluten::ResourceMap<gluten::ObjectStore*> stores;
  return stores;
}

gluten::ObjectStore::~ObjectStore() {
  // destructing in reversed order (the last added object destructed first)
  const std::lock_guard<std::mutex> lock(mtx_);
  for (auto itr = aliveObjects_.rbegin(); itr != aliveObjects_.rend(); itr++) {
    const ResourceHandle handle = (*itr).first;
    const auto& info = (*itr).second;
    const std::string_view typeName = info.typeName;
    const size_t size = info.size;
    VLOG(2) << "Unclosed object ["
            << "Store ID: " << storeId_ << ", Resource handle ID: " << handle << ", TypeName: " << typeName
            << ", Size: " << size
            << "] is found when object store is closing. Gluten will"
               " destroy it automatically but it's recommended to manually close"
               " the object through the Java closing API after use,"
               " to minimize peak memory pressure of the application.";
    store_.erase(handle);
  }
  stores().erase(storeId_);
}

void gluten::ObjectStore::releaseInternal(gluten::ResourceHandle handle) {
  const std::lock_guard<std::mutex> lock(mtx_);
  store_.erase(handle);
  aliveObjects_.erase(handle);
}
