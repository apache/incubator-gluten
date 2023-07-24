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
#include "compute/Backend.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxInitializer.h"
#include "utils/TaskContext.h"
#include "velox/common/memory/MemoryAllocator.h"

#include <glog/logging.h>

using namespace facebook;

namespace gluten {

// So far HbmMemoryAllocator would not work correctly since the underlying
//   gluten allocator is only used to do allocation-reporting to Spark in mmap case
class VeloxMemoryAllocator final : public velox::memory::MemoryAllocator {
 public:
  VeloxMemoryAllocator(gluten::MemoryAllocator* glutenAlloc, velox::memory::MemoryAllocator* veloxAlloc)
      : glutenAlloc_(glutenAlloc), veloxAlloc_(veloxAlloc) {}

  Kind kind() const override {
    return veloxAlloc_->kind();
  }

  bool allocateNonContiguous(
      velox::memory::MachinePageCount numPages,
      velox::memory::Allocation& out,
      ReservationCallback reservationCB,
      velox::memory::MachinePageCount minSizeClass) override {
    return veloxAlloc_->allocateNonContiguous(numPages, out, reservationCB, minSizeClass);
  }

  int64_t freeNonContiguous(velox::memory::Allocation& allocation) override {
    int64_t freedBytes = veloxAlloc_->freeNonContiguous(allocation);
    return freedBytes;
  }

  bool allocateContiguous(
      velox::memory::MachinePageCount numPages,
      velox::memory::Allocation* collateral,
      velox::memory::ContiguousAllocation& allocation,
      ReservationCallback reservationCB,
      velox::memory::MachinePageCount maxPages) override {
    return veloxAlloc_->allocateContiguous(numPages, collateral, allocation, reservationCB, maxPages);
  }

  void freeContiguous(velox::memory::ContiguousAllocation& allocation) override {
    veloxAlloc_->freeContiguous(allocation);
  }

  bool growContiguous(
      velox::memory::MachinePageCount increment,
      velox::memory::ContiguousAllocation& allocation,
      ReservationCallback reservationCB) override {
    return veloxAlloc_->growContiguous(increment, allocation, reservationCB);
  }

  void* allocateBytes(uint64_t bytes, uint16_t alignment) override {
    void* out;
    VELOX_CHECK(glutenAlloc_->allocateAligned(alignment, bytes, &out))
    return out;
  }

  void freeBytes(void* p, uint64_t size) noexcept override {
    VELOX_CHECK(glutenAlloc_->free(p, size));
  }

  bool checkConsistency() const override {
    return veloxAlloc_->checkConsistency();
  }

  size_t totalUsedBytes() const override {
    return veloxAlloc_->totalUsedBytes();
  }

  size_t capacity() const override {
    return veloxAlloc_->capacity();
  }

  velox::memory::MachinePageCount numAllocated() const override {
    return veloxAlloc_->numAllocated();
  }

  velox::memory::MachinePageCount numMapped() const override {
    return veloxAlloc_->numMapped();
  }

  std::string toString() const override {
    return veloxAlloc_->toString();
  }

 private:
  gluten::MemoryAllocator* glutenAlloc_;
  velox::memory::MemoryAllocator* veloxAlloc_;
};

// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config), listener_(listener) {}

  void reserveMemory(velox::memory::MemoryPool* pool, uint64_t) override {
    uint64_t bytes = initMemoryPoolCapacity_;
    listener_->allocationChanged(bytes);
    pool->grow(bytes);
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
      LOG(INFO) << "Memory pool " << pool->name() << " not completely shrunk when Memory::dropPool() is called";
    }
    return freeBytes;
  }

  bool growMemory(
      velox::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& candidatePools,
      uint64_t targetBytes) override {
    GLUTEN_CHECK(candidatePools.size() == 1, "ListenableArbitrator should only be used within a single root pool");
    auto candidate = candidatePools.back();
    GLUTEN_CHECK(pool->root() == candidate.get(), "Illegal state in ListenableArbitrator");
    reserveMemory(pool, targetBytes);
    return true;
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format(
        "ARBITRATOR[{}] CAPACITY {} {}", kindString(kind_), velox::succinctBytes(capacity_), stats().toString());
  }

 private:
  void abort(velox::memory::MemoryPool* pool) {
    try {
      pool->abort();
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to abort memory pool " << pool->toString();
    }
    // NOTE: no matter memory pool abort throws or not, it should have been marked
    // as aborted to prevent any new memory arbitration triggered from the aborted
    // memory pool.
    VELOX_CHECK(pool->aborted());
  }

  gluten::AllocationListener* listener_;
};

velox::memory::MemoryManager* getDefaultVeloxMemoryManager() {
  return &(facebook::velox::memory::defaultMemoryManager());
}

static std::shared_ptr<velox::memory::MemoryPool> rootVeloxMemoryPool() {
  auto* mm = getDefaultVeloxMemoryManager();
  static auto root = mm->addRootPool(
      "gluten_root", mm->capacity(), facebook::velox::memory::MemoryReclaimer::create()); // the 3rd capacity
  return root;
}

std::shared_ptr<velox::memory::MemoryPool> defaultLeafVeloxMemoryPool() {
  // this pool is not tracked by Spark
  static auto leaf =
      rootVeloxMemoryPool()->addLeafChild("default_leaf", true, facebook::velox::memory::MemoryReclaimer::create());
  return leaf;
}

std::shared_ptr<velox::memory::MemoryPool> asAggregateVeloxMemoryPool(gluten::MemoryAllocator* allocator) {
  // this pool is tracked by Spark
  static std::atomic_uint32_t id = 0;
  velox::memory::MemoryAllocator* veloxAlloc = velox::memory::MemoryAllocator::getInstance();
  gluten::MemoryAllocator* glutenAlloc;
  gluten::AllocationListener* listener;
  if (dynamic_cast<gluten::ListenableMemoryAllocator*>(allocator)) {
    // unwrap allocator and listener
    auto listenable = dynamic_cast<gluten::ListenableMemoryAllocator*>(allocator);
    glutenAlloc = listenable->delegatedAllocator();
    listener = listenable->listener();
  } else {
    // use the allocator directly
    glutenAlloc = allocator;
    listener = AllocationListener::noop();
  }
  auto wrappedAlloc = std::make_shared<VeloxMemoryAllocator>(glutenAlloc, veloxAlloc);
  bindToTask(wrappedAlloc); // keep alive util task ends
  velox::memory::MemoryArbitrator::Config arbitratorConfig{
      velox::memory::MemoryArbitrator::Kind::kNoOp, // do not use shared arbitrator as it will mess up the thread
                                                    // contexts (one Spark task reclaims memory from another)
      velox::memory::kMaxMemory, // the 2nd capacity
      128 << 20,
      32 << 20,
      true};
  velox::memory::MemoryManagerOptions mmOptions{
      velox::memory::MemoryAllocator::kMaxAlignment,
      velox::memory::kMaxMemory, // the 1st capacity, Velox requires for a couple of different capacity numbers
      true,
      false,
      wrappedAlloc.get(), // the allocator is tracked by Spark
      [=]() { return std::make_unique<ListenableArbitrator>(arbitratorConfig, listener); },
  };
  std::shared_ptr<velox::memory::MemoryManager> mm = std::make_shared<velox::memory::MemoryManager>(mmOptions);
  bindToTask(mm);
  auto pool = mm->addRootPool(
      "wrapped_root_" + std::to_string(id++),
      velox::memory::kMaxMemory, // the 3rd capacity
      facebook::velox::memory::MemoryReclaimer::create());
  return pool;
}
} // namespace gluten
