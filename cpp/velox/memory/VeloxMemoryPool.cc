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
#define VELOX_MEM_MANAGER_CAP_EXCEEDED(cap)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory manager cap of {} MB",                       \
      (cap) / 1024 / 1024);
} // namespace

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
      gluten::MemoryAllocator* gluten_alloc,
      const Options& options = Options{})
      : velox::memory::MemoryPool{name, parent, options},
        memoryManager_{memoryManager},
        localMemoryUsage_{},
        velox_alloc_{memoryManager_.getAllocator()},
        gluten_alloc_{gluten_alloc} {}

  ~WrappedVeloxMemoryPool() override {
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
  void* FOLLY_NULLABLE allocate(int64_t size) override {
    const auto alignedSize = sizeAlign(size);
    reserve(alignedSize);
    void* buffer;
    if (!gluten_alloc_->Allocate(alignedSize, &buffer)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to allocate " + std::to_string(alignedSize) + " bytes")
    }
    if (FOLLY_UNLIKELY(buffer == nullptr)) {
      release(alignedSize);
      VELOX_MEM_ALLOC_ERROR(fmt::format("{} failed with {} bytes from {}", __FUNCTION__, alignedSize, toString()));
    }
    return buffer;
  }

  void* FOLLY_NULLABLE allocateZeroFilled(int64_t numEntries, int64_t sizeEach) override {
    const auto alignedSize = sizeAlign(sizeEach * numEntries);
    reserve(alignedSize);
    void* buffer;
    if (!gluten_alloc_->AllocateZeroFilled(alignedSize, 1, &buffer)) {
      VELOX_FAIL(
          "VeloxMemoryAllocatorVariant: Failed to allocate (zero filled) " + std::to_string(alignedSize) +
          " members, " + std::to_string(1) + " bytes for each")
    }
    if (FOLLY_UNLIKELY(buffer == nullptr)) {
      release(alignedSize);
      VELOX_MEM_ALLOC_ERROR(fmt::format(
          "{} failed with {} entries and {} bytes each from {}", __FUNCTION__, numEntries, sizeEach, toString()));
    }
    return buffer;
  }

  // No-op for attempts to shrink buffer.
  void* FOLLY_NULLABLE reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) override {
    auto alignedSize = sizeAlign(size);
    auto alignedNewSize = sizeAlign(newSize);
    int64_t difference = alignedNewSize - alignedSize;
    reserve(difference);
    void* newP;
    if (!gluten_alloc_->Reallocate(p, alignedSize, alignedNewSize, &newP)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to reallocate " + std::to_string(alignedNewSize) + " bytes")
    }
    if (FOLLY_UNLIKELY(newP == nullptr)) {
      free(p, alignedSize);
      release(alignedNewSize);
      VELOX_MEM_ALLOC_ERROR(
          fmt::format("{} failed with {} new bytes and {} old bytes from {}", __FUNCTION__, newSize, size, toString()));
    }
    return newP;
  }

  void free(void* FOLLY_NULLABLE p, int64_t size) override {
    auto alignedSize = sizeAlign(size);
    if (!gluten_alloc_->Free(p, alignedSize)) {
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to free " + std::to_string(alignedSize) + " bytes")
    }
    release(alignedSize);
  }

  // FIXME: This function should call glutenAllocator_.
  void allocateNonContiguous(
      velox::memory::MachinePageCount numPages,
      velox::memory::Allocation& out,
      velox::memory::MachinePageCount minSizeClass) override {
    VELOX_CHECK_GT(numPages, 0);

    if (!velox_alloc_.allocateNonContiguous(
            numPages,
            out,
            [this](int64_t allocBytes, bool preAllocate) {
              bool succeed =
                  preAllocate ? gluten_alloc_->ReserveBytes(allocBytes) : gluten_alloc_->UnreserveBytes(allocBytes);
              VELOX_CHECK(succeed)
              if (memoryUsageTracker_ != nullptr) {
                memoryUsageTracker_->update(preAllocate ? allocBytes : -allocBytes);
              }
            },
            256)) {
      VELOX_CHECK(out.empty());
      VELOX_MEM_ALLOC_ERROR(fmt::format("{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
    }
    VELOX_CHECK(!out.empty());
    VELOX_CHECK_NULL(out.pool());
    out.setPool(this);
  }

  void freeNonContiguous(velox::memory::Allocation& allocation) override {
    const int64_t freedBytes = velox_alloc_.freeNonContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    if (memoryUsageTracker_ != nullptr) {
      memoryUsageTracker_->update(-freedBytes);
    }
    VELOX_CHECK(gluten_alloc_->UnreserveBytes(freedBytes))
  }

  velox::memory::MachinePageCount largestSizeClass() const override {
    return velox_alloc_.largestSizeClass();
  }

  const std::vector<velox::memory::MachinePageCount>& sizeClasses() const override {
    return velox_alloc_.sizeClasses();
  }

  void allocateContiguous(velox::memory::MachinePageCount numPages, velox::memory::ContiguousAllocation& out) override {
    VELOX_CHECK_GT(numPages, 0);
    if (!velox_alloc_.allocateContiguous(numPages, nullptr, out, [this](int64_t allocBytes, bool preAlloc) {
          bool succeed = preAlloc ? gluten_alloc_->ReserveBytes(allocBytes) : gluten_alloc_->UnreserveBytes(allocBytes);
          VELOX_CHECK(succeed)
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

  void freeContiguous(velox::memory::ContiguousAllocation& allocation) override {
    const int64_t bytesToFree = allocation.size();
    velox_alloc_.freeContiguous(allocation);
    VELOX_CHECK(allocation.empty());
    if (memoryUsageTracker_ != nullptr) {
      memoryUsageTracker_->update(-bytesToFree);
    }
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

  void setMemoryUsageTracker(const std::shared_ptr<velox::memory::MemoryUsageTracker>& tracker) override {
    const auto currentBytes = getCurrentBytes();
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(-currentBytes);
    }
    memoryUsageTracker_ = tracker;
    memoryUsageTracker_->update(currentBytes);
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
  std::shared_ptr<velox::memory::MemoryPool> genChild(
      std::shared_ptr<velox::memory::MemoryPool> parent,
      const std::string& name) override {
    return std::make_shared<WrappedVeloxMemoryPool>(
        memoryManager_, name, parent, gluten_alloc_, Options{.alignment = alignment_});
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
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(size);
    }
    localMemoryUsage_.incrementCurrentBytes(size);

    bool success = memoryManager_.reserve(size);
    if (UNLIKELY(!success)) {
      // NOTE: If we can make the reserve and release a single transaction we
      // would have more accurate aggregates in intermediate states. However, this
      // is low-pri because we can only have inflated aggregates, and be on the
      // more conservative side.
      release(size);
      VELOX_MEM_MANAGER_CAP_EXCEEDED(memoryManager_.getMemoryQuota());
    }
  }

  void release(int64_t size) override {
    memoryManager_.release(size);
    localMemoryUsage_.incrementCurrentBytes(-size);
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(-size);
    }
  }

  std::string toString() const override {
    return fmt::format("Memory Pool[{} {}]", name_, velox::memory::MemoryAllocator::kindString(velox_alloc_.kind()));
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

  int64_t cap_;
  velox::memory::MemoryManager& memoryManager_;

  // Memory allocated attributed to the memory node.
  velox::memory::MemoryUsage localMemoryUsage_;
  std::shared_ptr<velox::memory::MemoryUsageTracker> memoryUsageTracker_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  velox::memory::MemoryUsage subtreeMemoryUsage_;
  velox::memory::MemoryAllocator& velox_alloc_;
  gluten::MemoryAllocator* gluten_alloc_;
};

std::shared_ptr<velox::memory::MemoryPool> AsWrappedVeloxMemoryPool(gluten::MemoryAllocator* allocator) {
  return std::make_shared<WrappedVeloxMemoryPool>(
      velox::memory::MemoryManager::getInstance(), "wrapped", nullptr, allocator);
}

velox::memory::MemoryPool* GetDefaultWrappedVeloxMemoryPool() {
  static WrappedVeloxMemoryPool default_pool(
      velox::memory::MemoryManager::getInstance(), "root", nullptr, DefaultMemoryAllocator().get());
  return &default_pool;
}

} // namespace gluten
