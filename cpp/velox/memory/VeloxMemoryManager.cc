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

#include "VeloxMemoryManager.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/MemoryPool.h"

#include <execinfo.h>
#include "memory/ArrowMemoryPool.h"
#include "utils/exception.h"

namespace gluten {

using namespace facebook;

// So far HbmMemoryAllocator would not work correctly since the underlying
//   gluten allocator is only used to do allocation-reporting to Spark in mmap case
// This allocator only hook `allocateBytes` and `freeBytes`, we can not ensure this behavior is safe enough,
// so, only use this allocator when build with GLUTEN_ENABLE_HBM.
class VeloxMemoryAllocator final : public velox::memory::MallocAllocator {
 public:
  VeloxMemoryAllocator(gluten::MemoryAllocator* glutenAlloc)
      : MallocAllocator(velox::memory::kMaxMemory), glutenAlloc_(glutenAlloc) {}

 protected:
  void* allocateBytesWithoutRetry(uint64_t bytes, uint16_t alignment) override {
    void* out;
    VELOX_CHECK(glutenAlloc_->allocateAligned(alignment, bytes, &out), "Issue allocating bytes");
    return out;
  }

 public:
  void freeBytes(void* p, uint64_t size) noexcept override {
    VELOX_CHECK(glutenAlloc_->free(p, size));
  }

 private:
  gluten::MemoryAllocator* glutenAlloc_;
};

/// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config), listener_(listener) {}

  std::string kind() const override {
    return kind_;
  }

  void reserveMemory(velox::memory::MemoryPool* pool, uint64_t) override {
    growPool(pool, memoryPoolInitCapacity_);
  }

  uint64_t releaseMemory(velox::memory::MemoryPool* pool, uint64_t bytes) override {
    uint64_t freeBytes = pool->shrink(bytes);
    listener_->allocationChanged(-freeBytes);
    if (bytes == 0 && pool->capacity() != 0) {
      // So far only MemoryManager::dropPool() calls with 0 bytes. Let's assert the pool
      //   gives all capacity back to Spark
      //
      // We are likely in destructor, do not throw. INFO log is fine since we have leak checks from Spark's memory
      //   manager
      LOG(INFO) << "Memory pool " << pool->name() << " not completely shrunken when Memory::dropPool() is called";
    }
    return freeBytes;
  }

  uint64_t shrinkMemory(const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& pools, uint64_t targetBytes)
      override {
    GLUTEN_CHECK(false, "Not implemented");
  }

  bool growMemory(
      velox::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& candidatePools,
      uint64_t targetBytes) override {
    GLUTEN_CHECK(candidatePools.size() == 1, "ListenableArbitrator should only be used within a single root pool");
    auto candidate = candidatePools.back();
    GLUTEN_CHECK(pool->root() == candidate.get(), "Illegal state in ListenableArbitrator");
    growPool(pool, targetBytes);
    return true;
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format("ARBITRATOR[{}] CAPACITY {} {}", kind_, velox::succinctBytes(capacity_), stats().toString());
  }

 private:
  void growPool(velox::memory::MemoryPool* pool, uint64_t bytes) {
    listener_->allocationChanged(bytes);
    pool->grow(bytes);
  }

  gluten::AllocationListener* listener_;
  inline static std::string kind_ = "GLUTEN";
};

class ArbitratorFactoryRegister {
 public:
  explicit ArbitratorFactoryRegister(gluten::AllocationListener* listener) : listener_(listener) {
    static std::atomic_uint32_t id{0UL};
    kind_ = "GLUTEN_ARBITRATOR_FACTORY_" + std::to_string(id++);
    velox::memory::MemoryArbitrator::registerFactory(
        kind_,
        [this](
            const velox::memory::MemoryArbitrator::Config& config) -> std::unique_ptr<velox::memory::MemoryArbitrator> {
          return std::make_unique<ListenableArbitrator>(config, listener_);
        });
  }

  virtual ~ArbitratorFactoryRegister() {
    velox::memory::MemoryArbitrator::unregisterFactory(kind_);
  }

  const std::string& getKind() const {
    return kind_;
  }

 private:
  std::string kind_;
  gluten::AllocationListener* listener_;
};

VeloxMemoryManager::VeloxMemoryManager(
    const std::string& name,
    std::shared_ptr<MemoryAllocator> allocator,
    std::unique_ptr<AllocationListener> listener)
    : MemoryManager(), name_(name), listener_(std::move(listener)) {
  glutenAlloc_ = std::make_unique<ListenableMemoryAllocator>(allocator.get(), listener_.get());
  arrowPool_ = std::make_shared<ArrowMemoryPool>(glutenAlloc_.get());

  auto veloxAlloc = velox::memory::MemoryAllocator::getInstance();

#ifdef GLUTEN_ENABLE_HBM
  wrappedAlloc_ = std::make_unique<VeloxMemoryAllocator>(allocator.get(), veloxAlloc);
  veloxAlloc = wrappedAlloc_.get();
#endif

  ArbitratorFactoryRegister afr(listener_.get());
  velox::memory::MemoryManagerOptions mmOptions{
      velox::memory::MemoryAllocator::kMaxAlignment,
      velox::memory::kMaxMemory,
      velox::memory::kMaxMemory,
      true, // leak check
      false, // debug
#ifdef GLUTEN_ENABLE_HBM
      wrappedAlloc.get(),
#else
      veloxAlloc,
#endif
      afr.getKind(),
      0,
      32 << 20,
      true};
  veloxMemoryManager_ = std::make_unique<velox::memory::MemoryManager>(mmOptions);

  veloxAggregatePool_ = veloxMemoryManager_->addRootPool(
      name_ + "_root",
      velox::memory::kMaxMemory, // the 3rd capacity
      facebook::velox::memory::MemoryReclaimer::create());

  veloxLeafPool_ = veloxAggregatePool_->addLeafChild(name_ + "_default_leaf");
}

namespace {
MemoryUsageStats collectMemoryUsageStatsInternal(const velox::memory::MemoryPool* pool) {
  MemoryUsageStats stats;
  stats.set_current(pool->currentBytes());
  stats.set_peak(pool->peakBytes());
  // walk down root and all children
  pool->visitChildren([&](velox::memory::MemoryPool* pool) -> bool {
    stats.mutable_children()->emplace(pool->name(), collectMemoryUsageStatsInternal(pool));
    return true;
  });
  return stats;
}

int64_t shrinkVeloxMemoryPool(velox::memory::MemoryPool* pool, int64_t size) {
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Shrink[" + poolName + "]: "};
  DLOG(INFO) << logPrefix << "Trying to shrink " << size << " bytes of data...";
  DLOG(INFO) << logPrefix << "Pool has reserved " << pool->currentBytes() << "/" << pool->root()->reservedBytes() << "/"
             << pool->root()->capacity() << "/" << pool->root()->maxCapacity() << " bytes.";
  DLOG(INFO) << logPrefix << "Shrinking...";
  int64_t shrunken = pool->shrinkManaged(pool, size);
  DLOG(INFO) << logPrefix << shrunken << " bytes released from shrinking.";
  return shrunken;
}
} // namespace

const MemoryUsageStats VeloxMemoryManager::collectMemoryUsageStats() const {
  return collectMemoryUsageStatsInternal(veloxAggregatePool_.get());
}

const int64_t VeloxMemoryManager::shrink(int64_t size) {
  return shrinkVeloxMemoryPool(veloxAggregatePool_.get(), size);
}

velox::memory::MemoryManager* getDefaultVeloxMemoryManager() {
  return &(facebook::velox::memory::defaultMemoryManager());
}

} // namespace gluten
