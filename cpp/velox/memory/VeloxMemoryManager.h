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

#include "memory/MemoryAllocator.h"
#include "memory/MemoryManager.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"

namespace gluten {

class VeloxMemoryManager final : public MemoryManager {
 public:
  explicit VeloxMemoryManager(
      std::string name,
      std::shared_ptr<MemoryAllocator> allocator,
      std::shared_ptr<AllocationListener> listener);

  ~VeloxMemoryManager() {
    // Release order is important, from leaf to root.
    veloxLeafPool_ = nullptr;
    veloxPool_ = nullptr;
    veloxMemoryManager_ = nullptr;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> getAggregateMemoryPool() {
    return veloxPool_;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> getLeafMemoryPool() {
    return veloxLeafPool_;
  }

  virtual MemoryAllocator* getMemoryAllocator() override {
    return glutenAlloc_.get();
  }

 private:
  std::string name_;
  std::unique_ptr<facebook::velox::memory::MemoryManager> veloxMemoryManager_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxLeafPool_;
  std::shared_ptr<MemoryAllocator> glutenAlloc_;
  std::shared_ptr<AllocationListener> listener_;
};

/// This pool is not tracked by Spark, should only used in test or validation.
/// TODO: Remove this pool with tracked pool.
std::shared_ptr<facebook::velox::memory::MemoryPool> defaultLeafVeloxMemoryPool();
} // namespace gluten
