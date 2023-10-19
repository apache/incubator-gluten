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

#include "arrow/memory_pool.h"
#include "memory.pb.h"

namespace gluten {

class MemoryManager {
 public:
  MemoryManager() = default;

  virtual ~MemoryManager() = default;

  virtual arrow::MemoryPool* getArrowMemoryPool() = 0;

  virtual const MemoryUsageStats collectMemoryUsageStats() const = 0;

  virtual const int64_t shrink(int64_t size) = 0;
};

} // namespace gluten
