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
  VeloxMemoryAllocator(gluten::MemoryAllocator* glutenAlloc, velox::memory::MemoryAllocator* veloxAlloc, bool track)
      : glutenAlloc_(glutenAlloc), veloxAlloc_(veloxAlloc), track_(track) {}

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
      ReservationCallback reservationCB) override {
    return veloxAlloc_->allocateContiguous(numPages, collateral, allocation, reservationCB);
  }

  void freeContiguous(velox::memory::ContiguousAllocation& allocation) override {
    veloxAlloc_->freeContiguous(allocation);
  }

  void* allocateBytes(uint64_t bytes, uint16_t alignment) override {
    return veloxAlloc_->allocateBytes(bytes, alignment);
  }

  void freeBytes(void* p, uint64_t size) noexcept override {
    return veloxAlloc_->freeBytes(p, size);
  }

  bool checkConsistency() const override {
    return veloxAlloc_->checkConsistency();
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

  size_t capacity() const override {
    return veloxAlloc_->capacity();
  }

 private:
  gluten::MemoryAllocator* glutenAlloc_;
  velox::memory::MemoryAllocator* veloxAlloc_;
  bool track_{false};
};

velox::memory::IMemoryManager* getDefaultVeloxMemoryManager() {
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

std::shared_ptr<velox::memory::MemoryPool> asAggregateVeloxMemoryPool(gluten::MemoryAllocator* allocator, bool track) {
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
  auto wrappedAlloc = std::make_shared<VeloxMemoryAllocator>(glutenAlloc, veloxAlloc, track);
  bindToTask(wrappedAlloc); // keep alive util task ends
  velox::memory::MemoryArbitrator::Config arbitratorConfig{
      velox::memory::MemoryArbitrator::Kind::kNoOp, // do not use shared arbitrator as it will mess up the thread
                                                    // contexts (one Spark task reclaims memory from another)
      velox::memory::kMaxMemory, // the 2nd capacity
      0,
      32 << 20,
      true};
  velox::memory::IMemoryManager::Options mmOptions{
      velox::memory::MemoryAllocator::kMaxAlignment,
      velox::memory::kMaxMemory, // the 1st capacity, Velox requires for a couple of different capacity numbers
      true,
      track,
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
