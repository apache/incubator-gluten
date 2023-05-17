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

#include "Backend.h"

namespace gluten {

static std::mutex mutex;
static std::function<std::shared_ptr<Backend>()> backendFactory;

void setBackendFactory(std::function<std::shared_ptr<Backend>()> factory) {
  std::lock_guard<std::mutex> lockGuard(mutex);
  if (backendFactory != nullptr) {
    throw std::runtime_error("Execution backend already set.");
  }
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Set backend factory." << std::endl;
#endif
  backendFactory = std::move(factory);
}

std::shared_ptr<Backend> createBackend() {
  {
    std::lock_guard<std::mutex> lockGuard(mutex);
    if (backendFactory == nullptr) {
      throw std::runtime_error(
          "Execution backend not set. This may due to the backend library not loaded, or SetBackendFactory() is not called in nativeInitNative() JNI call.");
    }
  }
  return backendFactory();
}

} // namespace gluten
