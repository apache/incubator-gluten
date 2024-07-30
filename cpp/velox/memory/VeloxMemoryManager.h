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

#include "memory/AllocationListener.h"
#include "memory/MemoryAllocator.h"
#include "memory/MemoryManager.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"

namespace gluten {

// Make sure the class is thread safe
class VeloxMemoryManager final : public MemoryManager {
 public:
  VeloxMemoryManager(std::unique_ptr<AllocationListener> listener);

  ~VeloxMemoryManager() override;
  VeloxMemoryManager(const VeloxMemoryManager&) = delete;
  VeloxMemoryManager(VeloxMemoryManager&&) = delete;
  VeloxMemoryManager& operator=(const VeloxMemoryManager&) = delete;
  VeloxMemoryManager& operator=(VeloxMemoryManager&&) = delete;

  std::shared_ptr<facebook::velox::memory::MemoryPool> getAggregateMemoryPool() const {
    return veloxAggregatePool_;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> getLeafMemoryPool() const {
    return veloxLeafPool_;
  }

  facebook::velox::memory::MemoryManager* getMemoryManager() const {
    return veloxMemoryManager_.get();
  }

  arrow::MemoryPool* getArrowMemoryPool() override {
    return arrowPool_.get();
  }

  const MemoryUsageStats collectMemoryUsageStats() const override;

  const int64_t shrink(int64_t size) override;

  void hold() override;

  /// Test only
  MemoryAllocator* allocator() const {
    return listenableAlloc_.get();
  }

  AllocationListener* getListener() const {
    return listener_.get();
  }

 private:
  bool tryDestructSafe();

#ifdef GLUTEN_ENABLE_HBM
  std::unique_ptr<VeloxMemoryAllocator> wrappedAlloc_;
#endif

  // This is a listenable allocator used for arrow.
  std::unique_ptr<MemoryAllocator> listenableAlloc_;
  std::unique_ptr<AllocationListener> listener_;
  std::unique_ptr<AllocationListener> blockListener_;
  std::unique_ptr<arrow::MemoryPool> arrowPool_;

  std::unique_ptr<facebook::velox::memory::MemoryManager> veloxMemoryManager_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxAggregatePool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxLeafPool_;
  std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>> heldVeloxPools_;
};

/// Not tracked by Spark and should only used in test or validation.
inline std::shared_ptr<gluten::VeloxMemoryManager> getDefaultMemoryManager() {
  static auto memoryManager = std::make_shared<gluten::VeloxMemoryManager>(gluten::AllocationListener::noop());
  return memoryManager;
}

inline std::shared_ptr<facebook::velox::memory::MemoryPool> defaultLeafVeloxMemoryPool() {
  return getDefaultMemoryManager()->getLeafMemoryPool();
}

} // namespace gluten
