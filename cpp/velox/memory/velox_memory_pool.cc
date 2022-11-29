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

#include "velox_memory_pool.h"

namespace gluten {

class VeloxMemoryAllocatorVariant {
 public:
  VeloxMemoryAllocatorVariant(MemoryAllocator* gluten_alloc) : gluten_alloc_(gluten_alloc) {}

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
      VELOX_FAIL("VeloxMemoryAllocatorVariant: Failed to allocate (zero filled) " +
          std::to_string(numMembers) + " members, " + std::to_string(sizeEach) + " bytes for each")
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
  MemoryAllocator* gluten_alloc_;
};

// The code is originated from /velox/common/memory/Memory.h
// Removed memory manager.
template <typename Allocator = VeloxMemoryAllocatorVariant, uint16_t ALIGNMENT = kNoAlignment>
class WrappedVeloxMemoryPool : public facebook::velox::memory::MemoryPool {
 public:
  // Should perhaps make this method private so that we only create node through
  // parent.
  explicit WrappedVeloxMemoryPool(
      const std::string& name,
      std::shared_ptr<MemoryPool> parent,
      std::shared_ptr<Allocator> allocator,
      int64_t cap = kMaxMemory)
      : MemoryPool{name, parent}, localMemoryUsage_{}, cap_{cap}, allocator_{allocator} {
    VELOX_USER_CHECK_GT(cap, 0);
  };

  // Actual memory allocation operations. Can be delegated.
  // Access global MemoryManager to check usage of current node and enforce
  // memory cap accordingly. Since MemoryManager walks the MemoryPoolImpl
  // tree periodically, this is slightly stale and we have to reserve our own
  // overhead.
  void* FOLLY_NULLABLE allocate(int64_t size) {
    if (this->isMemoryCapped()) {
      VELOX_MEM_MANUAL_CAP();
    }
    auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
    reserve(alignedSize);
    return allocAligned<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, alignedSize);
  }

  void* FOLLY_NULLABLE allocateZeroFilled(int64_t numMembers, int64_t sizeEach) {
    VELOX_USER_CHECK_EQ(sizeEach, 1);
    auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, numMembers);
    if (this->isMemoryCapped()) {
      VELOX_MEM_MANUAL_CAP();
    }
    reserve(alignedSize * sizeEach);
    return allocator_->allocZeroFilled(alignedSize, sizeEach);
  }

  // No-op for attempts to shrink buffer.
  void* FOLLY_NULLABLE reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
    auto alignedNewSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, newSize);
    int64_t difference = alignedNewSize - alignedSize;
    if (UNLIKELY(difference <= 0)) {
      // Track and pretend the shrink took place for accounting purposes.
      release(-difference);
      return p;
    }

    reserve(difference);
    void* newP = reallocAligned<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, p, alignedSize, alignedNewSize);
    if (UNLIKELY(!newP)) {
      free(p, alignedSize);
      VELOX_MEM_CAP_EXCEEDED(cap_);
    }

    return newP;
  }

  void free(void* FOLLY_NULLABLE p, int64_t size) {
    auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
    allocator_->free(p, alignedSize);
    release(alignedSize);
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

  void setMemoryUsageTracker(const std::shared_ptr<facebook::velox::memory::MemoryUsageTracker>& tracker) {
    memoryUsageTracker_ = tracker;
  }

  const std::shared_ptr<facebook::velox::memory::MemoryUsageTracker>& getMemoryUsageTracker() const {
    return memoryUsageTracker_;
  }

  void setSubtreeMemoryUsage(int64_t size) {
    updateSubtreeMemoryUsage([size](facebook::velox::memory::MemoryUsage& subtreeUsage) {
          subtreeUsage.setCurrentBytes(size);
        });
  }

  int64_t updateSubtreeMemoryUsage(int64_t size) {
    int64_t aggregateBytes;
    updateSubtreeMemoryUsage([&aggregateBytes, size](facebook::velox::memory::MemoryUsage& subtreeUsage) {
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
    return ALIGNMENT;
  }

  void capMemoryAllocation() {
    capped_.store(true);
    for (const auto& child : children_) {
      child->capMemoryAllocation();
    }
  }

  void uncapMemoryAllocation() {
    // This means if we try to post-order traverse the tree like we do
    // in MemoryManager, only parent has the right to lift the cap.
    // This suffices because parent will then recursively lift the cap on the
    // entire tree.
    if (getAggregateBytes() > cap()) {
      return;
    }
    if (parent_ != nullptr && parent_->isMemoryCapped()) {
      return;
    }
    capped_.store(false);
    visitChildren([](MemoryPool* child) { child->uncapMemoryAllocation(); });
  }

  bool isMemoryCapped() const {
    return capped_.load();
  }

  std::shared_ptr<MemoryPool> genChild(std::shared_ptr<MemoryPool> parent, const std::string& name, int64_t cap) {
    return std::make_shared<WrappedVeloxMemoryPool<Allocator, ALIGNMENT>>(name, parent, allocator_, cap);
  }

  // Gets the memory allocation stats of the MemoryPoolImpl attached to the
  // current MemoryPoolImpl. Not to be confused with total memory usage of the
  // subtree.
  const facebook::velox::memory::MemoryUsage& getLocalMemoryUsage() const {
    return localMemoryUsage_;
  }

  // Get the total memory consumption of the subtree, self + all recursive
  // children.
  int64_t getAggregateBytes() const {
    int64_t aggregateBytes = localMemoryUsage_.getCurrentBytes();
    accessSubtreeMemoryUsage([&aggregateBytes](const facebook::velox::memory::MemoryUsage& subtreeUsage) {
      aggregateBytes += subtreeUsage.getCurrentBytes();
    });
    return aggregateBytes;
  }
  int64_t getSubtreeMaxBytes() const {
    int64_t maxBytes;
    accessSubtreeMemoryUsage([&maxBytes](const facebook::velox::memory::MemoryUsage& subtreeUsage) {
      maxBytes = subtreeUsage.getMaxBytes();
    });
    return maxBytes;
  }

 private:
  VELOX_FRIEND_TEST(MemoryPoolTest, Ctor);

  template <uint16_t A>
  struct ALIGNER {};

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  int64_t sizeAlign(ALIGNER<A> /* unused */, int64_t size) {
    auto remainder = size % ALIGNMENT;
    return (remainder == 0) ? size : (size + ALIGNMENT - remainder);
  }

  template <uint16_t A>
  int64_t sizeAlign(ALIGNER<kNoAlignment> /* unused */, int64_t size) {
    return size;
  }

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  void* FOLLY_NULLABLE allocAligned(ALIGNER<A> /* unused */, int64_t size) {
    return allocator_.allocAligned(A, size);
  }

  template <uint16_t A>
  void* FOLLY_NULLABLE allocAligned(ALIGNER<kNoAlignment> /* unused */, int64_t size) {
    return allocator_->alloc(size);
  }

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  void* FOLLY_NULLABLE reallocAligned(ALIGNER<A> /* unused */, void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    return allocator_.reallocAligned(p, A, size, newSize);
  }

  template <uint16_t A>
  void* FOLLY_NULLABLE
  reallocAligned(ALIGNER<kNoAlignment> /* unused */, void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    return allocator_->realloc(p, size, newSize);
  }

  void accessSubtreeMemoryUsage(std::function<void(const facebook::velox::memory::MemoryUsage&)> visitor) const {
    folly::SharedMutex::ReadHolder readLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }
  void updateSubtreeMemoryUsage(std::function<void(facebook::velox::memory::MemoryUsage&)> visitor) {
    folly::SharedMutex::WriteHolder writeLock{subtreeUsageMutex_};
    visitor(subtreeMemoryUsage_);
  }

  // TODO: consider returning bool instead.
  void reserve(int64_t size) {
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(size);
    }
    localMemoryUsage_.incrementCurrentBytes(size);
    bool manualCap = isMemoryCapped();
    int64_t aggregateBytes = getAggregateBytes();
    if (UNLIKELY(manualCap || aggregateBytes > cap_)) {
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
    localMemoryUsage_.incrementCurrentBytes(-size);
    if (memoryUsageTracker_) {
      memoryUsageTracker_->update(-size);
    }
  }

  // Memory allocated attributed to the memory node.
  facebook::velox::memory::MemoryUsage localMemoryUsage_;
  std::shared_ptr<facebook::velox::memory::MemoryUsageTracker> memoryUsageTracker_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  facebook::velox::memory::MemoryUsage subtreeMemoryUsage_;
  int64_t cap_;
  std::atomic_bool capped_{false};

  std::shared_ptr<Allocator> allocator_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool> AsWrappedVeloxMemoryPool(MemoryAllocator* allocator) {
  return std::make_shared<WrappedVeloxMemoryPool<VeloxMemoryAllocatorVariant>>(
      "wrapped", nullptr, std::make_shared<VeloxMemoryAllocatorVariant>(allocator));
}

std::shared_ptr<facebook::velox::memory::MemoryPool>
GetDefaultWrappedVeloxMemoryPool() {
  static auto default_pool = std::make_shared<WrappedVeloxMemoryPool<VeloxMemoryAllocatorVariant>>(
          "root", nullptr, VeloxMemoryAllocatorVariant::createDefaultAllocator());
  return default_pool;
}

} // namespace gluten
