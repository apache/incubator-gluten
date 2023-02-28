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

using namespace facebook;

namespace gluten {
namespace {
#define VELOX_MEM_MANUAL_CAP()                          \
  _VELOX_THROW(                                         \
      velox::VeloxRuntimeError,                         \
      velox::error_source::kErrorSourceRuntime.c_str(), \
      velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                           \
      "Memory allocation manually capped");
} // namespace

class VeloxMemoryAllocatorVariant {
 public:
  VeloxMemoryAllocatorVariant(gluten::MemoryAllocator* gluten_alloc) : gluten_alloc_(gluten_alloc) {}

  static std::shared_ptr<VeloxMemoryAllocatorVariant> createDefaultAllocator() {
    static std::shared_ptr<VeloxMemoryAllocatorVariant> velox_alloc =
        std::make_shared<VeloxMemoryAllocatorVariant>(DefaultMemoryAllocator().get());
    return velox_alloc;
  }

  void* alloc(int64_t size) {
    void* out;
    if (!gluten_alloc_->Allocate(size, &out)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to allocate " + std::to_string(size) + " bytes")
    }
    return out;
  }

  void* allocZeroFilled(int64_t numMembers, int64_t sizeEach) {
    void* out;
    if (!gluten_alloc_->AllocateZeroFilled(numMembers, sizeEach, &out)) {
      VELOX_FAIL(
          "VeloxMemoryAllocatorVariant: Failed to allocate (zero filled) " + std::to_string(numMembers) + " members, " +
          std::to_string(sizeEach) + " bytes for each")
    }
    return out;
  }

  void* allocAligned(uint16_t alignment, int64_t size) {
    void* out;
    if (!gluten_alloc_->AllocateAligned(alignment, size, &out)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to allocate (aligned) " + std::to_string(size) + " bytes")
    }
    return out;
  }

  void* realloc(void* p, int64_t size, int64_t newSize) {
    void* out;
    if (!gluten_alloc_->Reallocate(p, size, newSize, &out)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to reallocate " + std::to_string(newSize) + " bytes")
    }
    return out;
  }

  void* reallocAligned(void* p, uint16_t alignment, int64_t size, int64_t newSize) {
    void* out;
    if (!gluten_alloc_->ReallocateAligned(p, alignment, size, newSize, &out)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to reallocate (aligned) " + std::to_string(newSize) + " bytes")
    }
    return out;
  }

  void free(void* p, int64_t size) {
    if (!gluten_alloc_->Free(p, size)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to free " + std::to_string(size) + " bytes")
    }
  }

 private:
  gluten::MemoryAllocator* gluten_alloc_;
};

//  The code is originated from /velox/common/memory/Memory.h
//  Removed memory manager.
class WrappedVeloxMemoryPool final : public velox::memory::MemoryPool {
 public:
  // Should perhaps make this method private so that we only create node through
  // parent.
  WrappedVeloxMemoryPool(
      velox::memory::MemoryManager& memoryManager,
      const std::string& name,
      std::shared_ptr<velox::memory::MemoryPool> parent,
      std::shared_ptr<VeloxMemoryAllocatorVariant> glutenAllocator,
      const Options& options = Options{})
      : velox::memory::MemoryPool{name, parent, options},
        cap_{options.capacity},
        memoryManager_{memoryManager},
        localMemoryUsage_{},
        allocator_{memoryManager_.getAllocator()},
        glutenAllocator_{glutenAllocator} {
    VELOX_USER_CHECK_GT(cap_, 0);
  }

  ~WrappedVeloxMemoryPool() {
    if (const auto& tracker = getMemoryUsageTracker()) {
      // TODO: change to check reserved bytes which including the unused
      // reservation.
      auto remainingBytes = tracker->currentBytes();
      VELOX_CHECK_EQ(
          0,
          remainingBytes,
          "Memory pool should be destroyed only after all allocated memory has been freed. Remaining bytes allocated: {}, cumulative bytes allocated: {}, number of allocations: {}",
          remainingBytes,
          tracker->cumulativeBytes(),
          tracker->numAllocs());
    }
  }

  // Actual memory allocation operations. Can be delegated.
  // Access global MemoryManager to check usage of current node and enforce
  // memory cap accordingly. Since MemoryManager walks the MemoryPoolImpl
  // tree periodically, this is slightly stale and we have to reserve our own
  // overhead.
  void* FOLLY_NULLABLE allocate(int64_t size) {
    if (this->isMemoryCapped()) {
      VELOX_MEM_MANUAL_CAP();
    }
    const auto alignedSize = sizeAlign(size);
    reserve(alignedSize);
    return glutenAllocator_->alloc(alignedSize);
  }

  void* FOLLY_NULLABLE allocateZeroFilled(int64_t numMembers, int64_t sizeEach) {
    if (this->isMemoryCapped()) {
      VELOX_MEM_MANUAL_CAP();
    }
    const auto alignedSize = sizeAlign(sizeEach * numMembers);
    reserve(alignedSize);
    return glutenAllocator_->allocZeroFilled(alignedSize, 1);
  }

  // No-op for attempts to shrink buffer.
  void* FOLLY_NULLABLE reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    auto alignedSize = sizeAlign(size);
    auto alignedNewSize = sizeAlign(newSize);
    int64_t difference = alignedNewSize - alignedSize;
    if (UNLIKELY(difference <= 0)) {
      // Track and pretend the shrink took place for accounting purposes.
      release(-difference);
      return p;
    }

    reserve(difference);
    void* newP = reallocAligned(p, alignedSize, alignedNewSize);
    if (UNLIKELY(!newP)) {
      free(p, alignedSize);
      VELOX_MEM_CAP_EXCEEDED(cap_);
    }

    return newP;
  }

  void free(void* FOLLY_NULLABLE p, int64_t size) {
    auto alignedSize = sizeAlign(size);
    glutenAllocator_->free(p, alignedSize);
    release(alignedSize);
  }

  // FIXME: This function should call glutenAllocator_.
  void allocateNonContiguous(
      velox::memory::MachinePageCount numPages,
      velox::memory::Allocation& out,
      velox::memory::MachinePageCount minSizeClass) {
    VELOX_CHECK_GT(numPages, 0);

    if (!allocator_.allocateNonContiguous(
            numPages,
            out,
            [this](int64_t allocBytes, bool preAllocate) {
              if (memoryUsageTracker_ != nullptr) {
                memoryUsageTracker_->update(preAllocate ? allocBytes : -allocBytes);
              }
            },
            minSizeClass)) {
      VELOX_CHECK(out.empty());
      VELOX_MEM_ALLOC_ERROR(fmt::format("{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
    }
    VELOX_CHECK(!out.empty());
    VELOX_CHECK_NULL(out.pool());
    out.setPool(this);
  }

  void freeNonContiguous(velox::memory::Allocation& allocation) {
    const int64_t freedBytes = allocator_.freeNonContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    if (memoryUsageTracker_ != nullptr) {
      memoryUsageTracker_->update(-freedBytes);
    }
  }

  velox::memory::MachinePageCount largestSizeClass() const {
    return allocator_.largestSizeClass();
  }

  const std::vector<velox::memory::MachinePageCount>& sizeClasses() const {
    return allocator_.sizeClasses();
  }

  void allocateContiguous(velox::memory::MachinePageCount numPages, velox::memory::ContiguousAllocation& out) {
    VELOX_CHECK_GT(numPages, 0);

    if (!allocator_.allocateContiguous(numPages, nullptr, out, [this](int64_t allocBytes, bool preAlloc) {
          if (memoryUsageTracker_) {
            memoryUsageTracker_->update(preAlloc ? allocBytes : -allocBytes);
          }
        })) {
      VELOX_CHECK(out.empty());
      VELOX_MEM_ALLOC_ERROR(fmt::format("{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
    }
    VELOX_CHECK(!out.empty());
    VELOX_CHECK_NULL(out.pool());
    out.setPool(this);
  }

  void freeContiguous(velox::memory::ContiguousAllocation& allocation) {
    const int64_t bytesToFree = allocation.size();
    allocator_.freeContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    if (memoryUsageTracker_ != nullptr) {
      memoryUsageTracker_->update(-bytesToFree);
    }
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
  int64_t getCurrentBytes() const {
    return getAggregateBytes();
  }

  int64_t getMaxBytes() const {
    return std::max(getSubtreeMaxBytes(), localMemoryUsage_.getMaxBytes());
  }

  void setMemoryUsageTracker(const std::shared_ptr<velox::memory::MemoryUsageTracker>& tracker) {
    const auto currentBytes = getCurrentBytes();
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(-currentBytes);
    }
    memoryUsageTracker_ = tracker;
    memoryUsageTracker_->update(currentBytes);
  }

  const std::shared_ptr<velox::memory::MemoryUsageTracker>& getMemoryUsageTracker() const {
    return memoryUsageTracker_;
  }

  void setSubtreeMemoryUsage(int64_t size) {
    updateSubtreeMemoryUsage([size](velox::memory::MemoryUsage& subtreeUsage) { subtreeUsage.setCurrentBytes(size); });
  }

  int64_t updateSubtreeMemoryUsage(int64_t size) {
    int64_t aggregateBytes;
    updateSubtreeMemoryUsage([&aggregateBytes, size](velox::memory::MemoryUsage& subtreeUsage) {
      aggregateBytes = subtreeUsage.getCurrentBytes() + size;
      subtreeUsage.setCurrentBytes(aggregateBytes);
    });
    return aggregateBytes;
  }

  // Get the cap for the memory node and its subtree.
  int64_t cap() const {
    return cap_;
  }

  uint16_t getAlignment() const {
    return alignment_;
  }

  bool isMemoryCapped() const {
    return capped_.load();
  }

  std::shared_ptr<velox::memory::MemoryPool> genChild(
      std::shared_ptr<velox::memory::MemoryPool> parent,
      const std::string& name) {
    return std::make_shared<WrappedVeloxMemoryPool>(
        memoryManager_, name, parent, glutenAllocator_, Options{.alignment = alignment_});
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
  void reserve(int64_t size) {
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(size);
    }
    localMemoryUsage_.incrementCurrentBytes(size);

    bool success = memoryManager_.reserve(size);
    bool manualCap = isMemoryCapped();
    int64_t aggregateBytes = getAggregateBytes();
    if (UNLIKELY(!success || manualCap || aggregateBytes > cap_)) {
      // NOTE: If we can make the reserve and release a single transaction we
      // would have more accurate aggregates in intermediate states. However,
      // this is low-pri because we can only have inflated aggregates, and be on
      // the more conservative side.
      release(size);
      if (manualCap) {
        VELOX_MEM_MANUAL_CAP();
      }
      VELOX_MEM_CAP_EXCEEDED(cap_);
    }
  }

  void release(int64_t size) {
    memoryManager_.release(size);
    localMemoryUsage_.incrementCurrentBytes(-size);
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(-size);
    }
  }

  std::string toString() const {
    return fmt::format("Memory Pool[{} {}]", name_, velox::memory::MemoryAllocator::kindString(allocator_.kind()));
  }

 private:
  VELOX_FRIEND_TEST(MemoryPoolTest, Ctor);

  int64_t sizeAlign(int64_t size) {
    const auto remainder = size % alignment_;
    return (remainder == 0) ? size : (size + alignment_ - remainder);
  }

  void* FOLLY_NULLABLE reallocAligned(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    return glutenAllocator_->realloc(p, size, newSize);
  }

  void accessSubtreeMemoryUsage(std::function<void(const velox::memory::MemoryUsage&)> visitor) const {
    folly::SharedMutex::ReadHolder readLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }

  void updateSubtreeMemoryUsage(std::function<void(velox::memory::MemoryUsage&)> visitor) {
    folly::SharedMutex::WriteHolder writeLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }

  int64_t cap_;
  velox::memory::MemoryManager& memoryManager_;

  // Memory allocated attributed to the memory node.
  velox::memory::MemoryUsage localMemoryUsage_;
  std::shared_ptr<velox::memory::MemoryUsageTracker> memoryUsageTracker_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  velox::memory::MemoryUsage subtreeMemoryUsage_;
  std::atomic_bool capped_{false};
  velox::memory::MemoryAllocator& allocator_;
  std::shared_ptr<VeloxMemoryAllocatorVariant> glutenAllocator_;
};

std::shared_ptr<velox::memory::MemoryPool> AsWrappedVeloxMemoryPool(gluten::MemoryAllocator* allocator) {
  return std::make_shared<WrappedVeloxMemoryPool>(
      velox::memory::MemoryManager::getInstance(),
      "wrapped",
      nullptr,
      std::make_shared<VeloxMemoryAllocatorVariant>(allocator));
}

velox::memory::MemoryPool* GetDefaultWrappedVeloxMemoryPool() {
  static WrappedVeloxMemoryPool default_pool(
      velox::memory::MemoryManager::getInstance(),
      "root",
      nullptr,
      VeloxMemoryAllocatorVariant::createDefaultAllocator());
  return &default_pool;
}

} // namespace gluten
