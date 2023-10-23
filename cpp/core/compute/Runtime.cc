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

#include "Runtime.h"
#include "utils/Print.h"

namespace gluten {

namespace {
class FactoryRegistry {
 public:
  void registerFactory(const std::string& kind, Runtime::Factory factory) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) == map_.end(), "Runtime factory already registered for " + kind);
    map_[kind] = std::move(factory);
  }

  Runtime::Factory& getFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) != map_.end(), "Runtime factory not registered for " + kind);
    return map_[kind];
  }

  bool unregisterFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    GLUTEN_CHECK(map_.find(kind) != map_.end(), "Runtime factory not registered for " + kind);
    return map_.erase(kind);
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, Runtime::Factory> map_;
};

FactoryRegistry& runtimeFactories() {
  static FactoryRegistry registry;
  return registry;
}
} // namespace

void Runtime::registerFactory(const std::string& kind, Runtime::Factory factory) {
  runtimeFactories().registerFactory(kind, std::move(factory));
}

Runtime* Runtime::create(const std::string& kind, const std::unordered_map<std::string, std::string>& sessionConf) {
  auto& factory = runtimeFactories().getFactory(kind);
  return factory(sessionConf);
}

void Runtime::release(Runtime* runtime) {
  delete runtime;
}

} // namespace gluten
