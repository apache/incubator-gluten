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

#include "VeloxMemoryPool.h"

#include <sstream>

using namespace facebook;

namespace gluten {
namespace {
#define VELOX_MEM_MANAGER_CAP_EXCEEDED(cap)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory manager cap of {} MB",                       \
      (cap) / 1024 / 1024);

std::shared_ptr<velox::memory::MemoryUsageTracker> createMemoryUsageTracker(
    velox::memory::MemoryPool* parent,
    velox::memory::MemoryPool::Kind kind,
    const velox::memory::MemoryPool::Options& options) {
  if (parent == nullptr) {
    return options.trackUsage ? velox::memory::MemoryUsageTracker::create(options.capacity, options.checkUsageLeak)
                              : nullptr;
  }
  if (parent->getMemoryUsageTracker() == nullptr) {
    return nullptr;
  }
  const bool isLeaf = kind == velox::memory::MemoryPool::Kind::kLeaf;
  VELOX_CHECK(isLeaf || options.threadSafe);
  return parent->getMemoryUsageTracker()->addChild(isLeaf, options.threadSafe);
}
} // namespace

//  The code is originated from /velox/common/memory/Memory.h
//  Removed memory manager.
class WrappedVeloxMemoryPool final : public velox::memory::MemoryPool {
 public:
  using DestructionCallback = std::function<void(MemoryPool*)>;

  // Should perhaps make this method private so that we only create node through
  // parent.
  WrappedVeloxMemoryPool(
      velox::memory::MemoryManager* memoryManager,
      const std::string& name,
      Kind kind,
      std::shared_ptr<velox::memory::MemoryPool> parent,
      gluten::MemoryAllocator* gluten_alloc,
      DestructionCallback destructionCb,
      const Options& options = Options{})
      : velox::memory::MemoryPool{name, kind, parent, options},
        memoryUsageTracker_(createMemoryUsageTracker(parent_.get(), kind, options)),
        memoryManager_{memoryManager},
        velox_alloc_{&memoryManager_->getAllocator()},
        gluten_alloc_{gluten_alloc},
        destructionCb_(std::move(destructionCb)),
        localMemoryUsage_{} {}

  ~WrappedVeloxMemoryPool() {
    const auto remainingBytes = memoryUsageTracker_->currentBytes();
    VELOX_CHECK_EQ(
        0,
        remainingBytes,
        "Memory pool {} should be destroyed only after all allocated memory has been freed. Remaining bytes allocated: {}, cumulative bytes allocated: {}, number of allocations: {}",
        name(),
        remainingBytes,
        memoryUsageTracker_->cumulativeBytes(),
        memoryUsageTracker_->numAllocs());
    if (destructionCb_ != nullptr) {
      destructionCb_(this);
    }
  }

  // Actual memory allocation operations. Can be delegated.
  // Access global MemoryManager to check usage of current node and enforce
  // memory cap accordingly. Since MemoryManager walks the MemoryPoolImpl
  // tree periodically, this is slightly stale and we have to reserve our own
  // overhead.
  void* FOLLY_NULLABLE allocate(int64_t size) override {
    checkMemoryAllocation();

    const auto alignedSize = sizeAlign(size);
    reserve(alignedSize);
    void* buffer;
    try {
      if (!gluten_alloc_->Allocate(alignedSize, &buffer)) {
        VELOX_FAIL(fmt::format("WrappedVeloxMemoryPool: Failed to allocate {} bytes", alignedSize))
      }
    } catch (std::exception& e) {
      release(alignedSize);
      VELOX_MEM_ALLOC_ERROR(
          fmt::format("{} failed with {} bytes from {}, cause: {}", __FUNCTION__, alignedSize, toString(), e.what()));
    }
    VELOX_CHECK_NOT_NULL(buffer)
    return buffer;
  }

  void* FOLLY_NULLABLE allocateZeroFilled(int64_t numEntries, int64_t sizeEach) override {
    checkMemoryAllocation();

    const auto alignedSize = sizeAlign(sizeEach * numEntries);
    reserve(alignedSize);
    void* buffer;
    try {
      bool succeed = gluten_alloc_->AllocateZeroFilled(alignedSize, 1, &buffer);
      if (!succeed) {
        VELOX_FAIL(fmt::format(
            "WrappedVeloxMemoryPool: Failed to allocate (zero filled) {} members, {} bytes for each", alignedSize, 1))
      }
    } catch (std::exception& e) {
      release(alignedSize);
      VELOX_MEM_ALLOC_ERROR(fmt::format(
          "{} failed with {} entries and {} bytes each from {}, cause: {}",
          __FUNCTION__,
          numEntries,
          sizeEach,
          toString(),
          e.what()));
    }
    VELOX_CHECK_NOT_NULL(buffer)
    return buffer;
  }

  // No-op for attempts to shrink buffer.
  void* FOLLY_NULLABLE reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) override {
    checkMemoryAllocation();

    auto alignedSize = sizeAlign(size);
    auto alignedNewSize = sizeAlign(newSize);
    reserve(alignedNewSize);
    void* newP;
    try {
      bool succeed = gluten_alloc_->Allocate(alignedNewSize, &newP);
      VELOX_CHECK(succeed)
    } catch (std::exception& e) {
      free(p, alignedSize);
      release(alignedNewSize);
      VELOX_MEM_ALLOC_ERROR(fmt::format(
          "WrappedVeloxMemoryPool {} failed with {} new bytes and {} old bytes from {}, cause: {}",
          __FUNCTION__,
          newSize,
          size,
          toString(),
          e.what()));
    }
    VELOX_CHECK_NOT_NULL(newP);
    if (p == nullptr) {
      return newP;
    }
    ::memcpy(newP, p, std::min(size, newSize));
    free(p, alignedSize);
    return newP;
  }

  void free(void* FOLLY_NULLABLE p, int64_t size) override {
    checkMemoryAllocation();

    auto alignedSize = sizeAlign(size);
    if (!gluten_alloc_->Free(p, alignedSize)) {
      VELOX_FAIL(fmt::format("WrappedVeloxMemoryPool: Failed to free {} bytes", alignedSize))
    }
    release(alignedSize);
  }

  // FIXME: This function should call glutenAllocator_.
  void allocateNonContiguous(
      velox::memory::MachinePageCount numPages,
      velox::memory::Allocation& out,
      velox::memory::MachinePageCount minSizeClass) override {
    checkMemoryAllocation();
    VELOX_CHECK_GT(numPages, 0);

    try {
      bool succeed = velox_alloc_->allocateNonContiguous(
          numPages,
          out,
          [this](int64_t allocBytes, bool preAlloc) {
            bool succeed =
                preAlloc ? gluten_alloc_->ReserveBytes(allocBytes) : gluten_alloc_->UnreserveBytes(allocBytes);
            VELOX_CHECK(succeed)
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          256);
      VELOX_CHECK(succeed)
    } catch (std::exception& e) {
      VELOX_CHECK(out.empty());
      VELOX_MEM_ALLOC_ERROR(
          fmt::format("{} failed with {} pages from {}, cause: {}", __FUNCTION__, numPages, toString(), e.what()));
    }
    VELOX_CHECK(!out.empty());
    VELOX_CHECK_NULL(out.pool());
    out.setPool(this);
  }

  void freeNonContiguous(velox::memory::Allocation& allocation) override {
    checkMemoryAllocation();

    const int64_t freedBytes = velox_alloc_->freeNonContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    release(freedBytes);
    VELOX_CHECK(gluten_alloc_->UnreserveBytes(freedBytes));
  }

  velox::memory::MachinePageCount largestSizeClass() const override {
    return velox_alloc_->largestSizeClass();
  }

  const std::vector<velox::memory::MachinePageCount>& sizeClasses() const override {
    return velox_alloc_->sizeClasses();
  }

  void allocateContiguous(velox::memory::MachinePageCount numPages, velox::memory::ContiguousAllocation& out) override {
    checkMemoryAllocation();
    VELOX_CHECK_GT(numPages, 0);

    try {
      bool succeed =
          velox_alloc_->allocateContiguous(numPages, nullptr, out, [this](int64_t allocBytes, bool preAlloc) {
            bool succeed =
                preAlloc ? gluten_alloc_->ReserveBytes(allocBytes) : gluten_alloc_->UnreserveBytes(allocBytes);
            VELOX_CHECK(succeed)
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          });
      VELOX_CHECK(succeed)
    } catch (std::exception& e) {
      VELOX_CHECK(out.empty());
      VELOX_MEM_ALLOC_ERROR(
          fmt::format("{} failed with {} pages from {}, cause: {}", __FUNCTION__, numPages, toString(), e.what()));
    }
    VELOX_CHECK(!out.empty());
    VELOX_CHECK_NULL(out.pool());
    out.setPool(this);
  }

  void freeContiguous(velox::memory::ContiguousAllocation& allocation) override {
    checkMemoryAllocation();

    const int64_t bytesToFree = allocation.size();
    velox_alloc_->freeContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    release(bytesToFree);
    VELOX_CHECK(gluten_alloc_->UnreserveBytes(bytesToFree))
  }
  //////////////////// Memory Management methods /////////////////////
  // Library checks for low memory mode on a push model. The respective root,
  // component level or global, would compute for memory pressure.
  // This is the signaling mechanism the customer application can use to make
  // all subcomponents start trimming memory usage.
  // virtual bool shouldTrim() const {
  //   return trimming_;
  // }
  // // Set by MemoryManager in periodic refresh threads. Stores the trim
  // target
  // // state potentially for a more granular/simplified global control.
  // virtual void startTrimming(int64_t target) {
  //   trimming_ = true;
  //   trimTarget_ = target;
  // }
  // // Resets the trim flag and trim target.
  // virtual void stopTrimming() {
  //   trimming_ = false;
  //   trimTarget_ = std::numeric_limits<int64_t>::max();
  // }

  // TODO: Consider putting these in base class also.
  int64_t getCurrentBytes() const override {
    return getAggregateBytes();
  }

  int64_t getMaxBytes() const override {
    return std::max(getSubtreeMaxBytes(), localMemoryUsage_.getMaxBytes());
  }

  const std::shared_ptr<velox::memory::MemoryUsageTracker>& getMemoryUsageTracker() const override {
    return memoryUsageTracker_;
  }

  int64_t updateSubtreeMemoryUsage(int64_t size) override {
    int64_t aggregateBytes;
    updateSubtreeMemoryUsage([&aggregateBytes, size](velox::memory::MemoryUsage& subtreeUsage) {
      aggregateBytes = subtreeUsage.getCurrentBytes() + size;
      subtreeUsage.setCurrentBytes(aggregateBytes);
    });
    return aggregateBytes;
  }

  uint16_t getAlignment() const override {
    return alignment_;
  }

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      MemoryPool::Kind kind,
      bool /*unused*/,
      std::shared_ptr<facebook::velox::memory::MemoryReclaimer> /*unused*/) override {
    return std::make_shared<WrappedVeloxMemoryPool>(
        memoryManager_, name, kind, parent, gluten_alloc_, nullptr, Options{.alignment = alignment_});
  }

  // Gets the memory allocation stats of the MemoryPoolImpl attached to the
  // current MemoryPoolImpl. Not to be confused with total memory usage of the
  // subtree.
  const velox::memory::MemoryUsage& getLocalMemoryUsage() const {
    return localMemoryUsage_;
  }

  // Get the total memory consumption of the subtree, self + all recursive
  // children.
  int64_t getAggregateBytes() const {
    int64_t aggregateBytes = localMemoryUsage_.getCurrentBytes();
    accessSubtreeMemoryUsage([&aggregateBytes](const velox::memory::MemoryUsage& subtreeUsage) {
      aggregateBytes += subtreeUsage.getCurrentBytes();
    });
    return aggregateBytes;
  }

  int64_t getSubtreeMaxBytes() const {
    int64_t maxBytes;
    accessSubtreeMemoryUsage(
        [&maxBytes](const velox::memory::MemoryUsage& subtreeUsage) { maxBytes = subtreeUsage.getMaxBytes(); });
    return maxBytes;
  }

  // TODO: consider returning bool instead.
  void reserve(int64_t size) override {
    checkMemoryAllocation();

    memoryUsageTracker_->update(size);
    localMemoryUsage_.incrementCurrentBytes(size);

    bool success = memoryManager_->reserve(size);
    if (UNLIKELY(!success)) {
      // NOTE: If we can make the reserve and release a single transaction we
      // would have more accurate aggregates in intermediate states. However, this
      // is low-pri because we can only have inflated aggregates, and be on the
      // more conservative side.
      release(size);
      VELOX_MEM_MANAGER_CAP_EXCEEDED(memoryManager_->getMemoryQuota());
    }
  }

  void release(int64_t size) override {
    checkMemoryAllocation();

    memoryManager_->release(size);
    localMemoryUsage_.incrementCurrentBytes(-size);
    memoryUsageTracker_->update(-size);
  }

  std::string toString() const override {
    return fmt::format(
        "Memory Pool[{} {} {}]",
        name_,
        kindString(kind_),
        velox::memory::MemoryAllocator::kindString(velox_alloc_->kind()));
  }

  uint64_t freeBytes() const override {
    VELOX_NYI("{} unsupported", __FUNCTION__);
  }

  uint64_t shrink(uint64_t targetBytes = 0) override {
    VELOX_NYI("{} unsupported", __FUNCTION__);
  }

  uint64_t grow(uint64_t bytes) override {
    VELOX_NYI("{} unsupported", __FUNCTION__);
  }

 private:
  VELOX_FRIEND_TEST(MemoryPoolTest, Ctor);

  int64_t sizeAlign(int64_t size) {
    const auto remainder = size % alignment_;
    return (remainder == 0) ? size : (size + alignment_ - remainder);
  }

  void accessSubtreeMemoryUsage(std::function<void(const velox::memory::MemoryUsage&)> visitor) const {
    folly::SharedMutex::ReadHolder readLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }

  void updateSubtreeMemoryUsage(std::function<void(velox::memory::MemoryUsage&)> visitor) {
    folly::SharedMutex::WriteHolder writeLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }

  const std::shared_ptr<velox::memory::MemoryUsageTracker> memoryUsageTracker_;
  velox::memory::MemoryManager* const memoryManager_;
  velox::memory::MemoryAllocator* const velox_alloc_;
  gluten::MemoryAllocator* gluten_alloc_;
  const DestructionCallback destructionCb_;

  // Memory allocated attributed to the memory node.
  velox::memory::MemoryUsage localMemoryUsage_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  velox::memory::MemoryUsage subtreeMemoryUsage_;
};

std::shared_ptr<velox::memory::MemoryPool> AsWrappedVeloxAggregateMemoryPool(
    gluten::MemoryAllocator* allocator,
    const facebook::velox::memory::MemoryPool::Options& options) {
  return std::make_shared<WrappedVeloxMemoryPool>(
      &velox::memory::MemoryManager::getInstance(),
      "wrappedRoot",
      velox::memory::MemoryPool::Kind::kAggregate,
      nullptr,
      allocator,
      nullptr,
      options);
}

std::shared_ptr<velox::memory::MemoryPool> GetDefaultWrappedVeloxAggregateMemoryPool() {
  static std::shared_ptr<WrappedVeloxMemoryPool> default_pool_root = std::make_shared<WrappedVeloxMemoryPool>(
      &velox::memory::MemoryManager::getInstance(),
      "root",
      velox::memory::MemoryPool::Kind::kAggregate,
      nullptr,
      DefaultMemoryAllocator().get(),
      nullptr);
  return default_pool_root;
}

std::shared_ptr<velox::memory::MemoryPool> GetDefaultVeloxLeafMemoryPool() {
  static std::shared_ptr<velox::memory::MemoryPool> default_pool =
      GetDefaultWrappedVeloxAggregateMemoryPool()->addLeafChild("default_pool");
  return default_pool;
}
} // namespace gluten
