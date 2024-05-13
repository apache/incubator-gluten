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
#include "velox/exec/MemoryReclaimer.h"

#include "memory/ArrowMemoryPool.h"
#include "utils/exception.h"

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);

namespace gluten {

using namespace facebook;

/// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config), listener_(listener) {}

  std::string kind() const override {
    return kind_;
  }

  uint64_t growCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    VELOX_CHECK_EQ(targetBytes, 0, "Gluten has set MemoryManagerOptions.memoryPoolInitCapacity to 0")
    return 0;
  }

  uint64_t shrinkCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    std::lock_guard<std::mutex> l(mutex_);

    return shrinkCapacityLocked(pool, targetBytes);
  }

  bool growCapacity(
      velox::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& candidatePools,
      uint64_t targetBytes) override {
    ArbitrationOperation op(pool, targetBytes, candidatePools);
    ScopedArbitration scopedArbitration(this, &op);
    //    velox::memory::ScopedMemoryArbitrationContext ctx(pool);
    VELOX_CHECK_EQ(candidatePools.size(), 1, "ListenableArbitrator should only be used within a single root pool")
    auto candidate = candidatePools.back();
    VELOX_CHECK(pool->root() == candidate.get(), "Illegal state in ListenableArbitrator");

    std::lock_guard<std::mutex> l(mutex_);

    growCapacityLocked(pool->root(), targetBytes);
    return true;
  }

  uint64_t shrinkCapacity(
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& pools,
      uint64_t targetBytes,
      bool allowSpill,
      bool allowAbort) override {
    ArbitrationOperation op(targetBytes, pools);
    ScopedArbitration scopedArbitration(this, &op);
    //    velox::memory::ScopedMemoryArbitrationContext ctx(nullptr);
    facebook::velox::exec::MemoryReclaimer::Stats status;
    VELOX_CHECK_EQ(pools.size(), 1, "Gluten only has one root pool");
    std::lock_guard<std::mutex> l(mutex_);

    auto pool = pools.at(0);
    const uint64_t oldCapacity = pool->capacity();
    pool->reclaim(targetBytes, 0, status); // ignore the output
    pool->shrink(0);
    const uint64_t newCapacity = pool->capacity();
    uint64_t total = oldCapacity - newCapacity;
    listener_->allocationChanged(-total);
    return total;
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format("ARBITRATOR[{}] CAPACITY {} {}", kind_, velox::succinctBytes(capacity_), stats().toString());
  }

 private:
  struct ArbitrationOperation {
    velox::memory::MemoryPool* const requestPool;
    velox::memory::MemoryPool* const requestRoot;
    const uint64_t targetBytes;

    ArbitrationOperation(
        uint64_t targetBytes,
        const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& candidatePools)
        : ArbitrationOperation(nullptr, targetBytes, candidatePools) {}

    ArbitrationOperation(
        velox::memory::MemoryPool* _requestor,
        uint64_t _targetBytes,
        const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& _candidatePools)
        : requestPool(_requestor),
          requestRoot(_requestor == nullptr ? nullptr : _requestor->root()),
          targetBytes(_targetBytes) {}

    void enterArbitration() {
      if (requestPool != nullptr) {
        requestPool->enterArbitration();
      }
    }
    void leaveArbitration() {
      if (requestPool != nullptr) {
        requestPool->leaveArbitration();
      }
    }
  };

  class ScopedArbitration {
   public:
    ScopedArbitration(ListenableArbitrator* arbitrator, ArbitrationOperation* op)
        : operation_(op), arbitrator_(arbitrator), arbitrationCtx_(op->requestPool) {
      VELOX_CHECK_NOT_NULL(arbitrator_);
      VELOX_CHECK_NOT_NULL(operation_);
      operation_->enterArbitration();
      if (arbitrator_->arbitrationStateCheckCb_ != nullptr && operation_->requestPool != nullptr) {
        arbitrator_->arbitrationStateCheckCb_(*operation_->requestPool);
      }
      arbitrator_->startArbitration(operation_);
    }

    ~ScopedArbitration() {
      operation_->leaveArbitration();
      arbitrator_->finishArbitration(operation_);
    }

   private:
    ArbitrationOperation* const operation_;
    ListenableArbitrator* const arbitrator_;
    const velox::memory::ScopedMemoryArbitrationContext arbitrationCtx_;
  };

  void startArbitration(ArbitrationOperation* op) {
    velox::ContinueFuture waitPromise{velox::ContinueFuture::makeEmpty()};
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (running_) {
        waitPromises_.emplace_back(
            fmt::format("Wait for arbitration {}/{}", op->requestPool->name(), op->requestRoot->name()));
        waitPromise = waitPromises_.back().getSemiFuture();
      } else {
        VELOX_CHECK(waitPromises_.empty());
        running_ = true;
      }
    }

    if (waitPromise.valid()) {
      waitPromise.wait();
    }
  }

  void finishArbitration(ArbitrationOperation* op) {
    velox::ContinuePromise resumePromise{velox::ContinuePromise::makeEmpty()};
    {
      std::lock_guard<std::mutex> l(mutex_);
      VELOX_CHECK(running_);
      if (!waitPromises_.empty()) {
        resumePromise = std::move(waitPromises_.back());
        waitPromises_.pop_back();
      } else {
        running_ = false;
      }
    }
    if (resumePromise.valid()) {
      resumePromise.setValue();
    }
  }

  void growCapacityLocked(velox::memory::MemoryPool* pool, uint64_t bytes) {
    // Since
    // https://github.com/facebookincubator/velox/pull/9557/files#diff-436e44b7374032f8f5d7eb45869602add6f955162daa2798d01cc82f8725724dL812-L820,
    // We should pass bytes as parameter "reservationBytes" when calling ::grow.
    auto freeByes = pool->freeBytes();
    if (freeByes > bytes) {
      if (pool->grow(0, bytes)) {
        return;
      }
    }
    auto reclaimedFreeBytes = pool->shrink(0);
    auto neededBytes = bytes - reclaimedFreeBytes;
    listener_->allocationChanged(neededBytes);
    auto ret = pool->grow(bytes, bytes);
    VELOX_CHECK(
        ret,
        "{} failed to grow {} bytes, current state {}",
        pool->name(),
        velox::succinctBytes(bytes),
        pool->toString())
  }

  uint64_t shrinkCapacityLocked(velox::memory::MemoryPool* pool, uint64_t bytes) {
    uint64_t freeBytes = pool->shrink(bytes);
    listener_->allocationChanged(-freeBytes);
    return freeBytes;
  }

  gluten::AllocationListener* listener_;
  //  std::recursive_mutex mutex_;
  inline static std::string kind_ = "GLUTEN";

  mutable std::mutex mutex_;
  // Indicates if there is a running arbitration request or not.
  bool running_{false};
  // The promises of the arbitration requests waiting for the serialized
  // execution.
  std::vector<velox::ContinuePromise> waitPromises_;
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
  arrowPool_ = std::make_unique<ArrowMemoryPool>(glutenAlloc_.get());

  ArbitratorFactoryRegister afr(listener_.get());
  velox::memory::MemoryManagerOptions mmOptions{
      .alignment = velox::memory::MemoryAllocator::kMaxAlignment,
      .trackDefaultUsage = true, // memory usage tracking
      .checkUsageLeak = true, // leak check
      .debugEnabled = false, // debug
      .coreOnAllocationFailureEnabled = false,
      .allocatorCapacity = velox::memory::kMaxMemory,
      .arbitratorKind = afr.getKind(),
      .memoryPoolInitCapacity = 0,
      .memoryPoolTransferCapacity = 32 << 20,
      .memoryReclaimWaitMs = 0};
  if (name_ == "WholeStageIterator") {
    mmOptions.arbitrationStateCheckCb = velox::exec::memoryArbitrationStateCheck;
  }
  veloxMemoryManager_ = std::make_unique<velox::memory::MemoryManager>(mmOptions);

  veloxAggregatePool_ = veloxMemoryManager_->addRootPool(
      name_ + "_root",
      velox::memory::kMaxMemory, // the 3rd capacity
      facebook::velox::memory::MemoryReclaimer::create());

  veloxLeafPool_ = veloxAggregatePool_->addLeafChild(name_ + "_default_leaf");
}

namespace {
MemoryUsageStats collectVeloxMemoryUsageStats(const velox::memory::MemoryPool* pool) {
  MemoryUsageStats stats;
  stats.set_current(pool->currentBytes());
  stats.set_peak(pool->peakBytes());
  // walk down root and all children
  pool->visitChildren([&](velox::memory::MemoryPool* pool) -> bool {
    stats.mutable_children()->emplace(pool->name(), collectVeloxMemoryUsageStats(pool));
    return true;
  });
  return stats;
}

MemoryUsageStats collectGlutenAllocatorMemoryUsageStats(const MemoryAllocator* allocator) {
  MemoryUsageStats stats;
  stats.set_current(allocator->getBytes());
  stats.set_peak(allocator->peakBytes());
  return stats;
}

int64_t shrinkVeloxMemoryPool(velox::memory::MemoryManager* mm, velox::memory::MemoryPool* pool, int64_t size) {
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Shrink[" + poolName + "]: "};
  VLOG(2) << logPrefix << "Trying to shrink " << size << " bytes of data...";
  VLOG(2) << logPrefix << "Pool has reserved " << pool->currentBytes() << "/" << pool->root()->reservedBytes() << "/"
          << pool->root()->capacity() << "/" << pool->root()->maxCapacity() << " bytes.";
  VLOG(2) << logPrefix << "Shrinking...";
  const uint64_t oldCapacity = pool->capacity();
  mm->arbitrator()->shrinkCapacity(pool, 0);
  const uint64_t newCapacity = pool->capacity();
  int64_t shrunken = oldCapacity - newCapacity;
  VLOG(2) << logPrefix << shrunken << " bytes released from shrinking.";
  return shrunken;
}
} // namespace

const MemoryUsageStats VeloxMemoryManager::collectMemoryUsageStats() const {
  MemoryUsageStats stats;
  stats.set_current(listener_->currentBytes());
  stats.set_peak(listener_->peakBytes());
  stats.mutable_children()->emplace(
      "gluten::MemoryAllocator", collectGlutenAllocatorMemoryUsageStats(glutenAlloc_.get()));
  stats.mutable_children()->emplace(
      veloxAggregatePool_->name(), collectVeloxMemoryUsageStats(veloxAggregatePool_.get()));
  return stats;
}

const int64_t VeloxMemoryManager::shrink(int64_t size) {
  return shrinkVeloxMemoryPool(veloxMemoryManager_.get(), veloxAggregatePool_.get(), size);
}

namespace {
void holdInternal(
    std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>& heldVeloxPools,
    const velox::memory::MemoryPool* pool) {
  pool->visitChildren([&](velox::memory::MemoryPool* child) -> bool {
    auto shared = child->shared_from_this();
    heldVeloxPools.push_back(shared);
    holdInternal(heldVeloxPools, child);
    return true;
  });
}
} // namespace

void VeloxMemoryManager::hold() {
  holdInternal(heldVeloxPools_, veloxAggregatePool_.get());
}

bool VeloxMemoryManager::tryDestructSafe() {
  // Velox memory pools considered safe to destruct when no alive allocations.
  for (const auto& pool : heldVeloxPools_) {
    if (pool && pool->currentBytes() != 0) {
      return false;
    }
  }
  if (veloxLeafPool_ && veloxLeafPool_->currentBytes() != 0) {
    return false;
  }
  if (veloxAggregatePool_ && veloxAggregatePool_->currentBytes() != 0) {
    return false;
  }
  heldVeloxPools_.clear();
  veloxLeafPool_.reset();
  veloxAggregatePool_.reset();

  // Velox memory manager considered safe to destruct when no alive pools.
  if (veloxMemoryManager_) {
    if (veloxMemoryManager_->numPools() > 1) {
      return false;
    }
    if (veloxMemoryManager_->numPools() == 1) {
      // Assert the pool is spill pool
      // See https://github.com/facebookincubator/velox/commit/e6f84e8ac9ef6721f527a2d552a13f7e79bdf72e
      int32_t spillPoolCount = 0;
      veloxMemoryManager_->testingDefaultRoot().visitChildren([&](velox::memory::MemoryPool* child) -> bool {
        if (child == veloxMemoryManager_->spillPool()) {
          spillPoolCount++;
        }
        return true;
      });
      GLUTEN_CHECK(spillPoolCount == 1, "Illegal pool count state: spillPoolCount: " + std::to_string(spillPoolCount));
    }
    if (veloxMemoryManager_->numPools() < 1) {
      GLUTEN_CHECK(false, "Unreachable code");
    }
  }
  veloxMemoryManager_.reset();

  // Applies similar rule for Arrow memory pool.
  if (arrowPool_ && arrowPool_->bytes_allocated() != 0) {
    return false;
  }
  arrowPool_.reset();

  // Successfully destructed.
  return true;
}

VeloxMemoryManager::~VeloxMemoryManager() {
  static const uint32_t kWaitTimeoutMs = FLAGS_gluten_velox_aysnc_timeout_on_task_stopping; // 30s by default
  uint32_t accumulatedWaitMs = 0UL;
  for (int32_t tryCount = 0; accumulatedWaitMs < kWaitTimeoutMs; tryCount++) {
    if (tryDestructSafe()) {
      if (tryCount > 0) {
        LOG(INFO) << "All the outstanding memory resources successfully released. ";
      }
      break;
    }
    uint32_t waitMs = 50 * static_cast<uint32_t>(pow(1.5, tryCount)); // 50ms, 75ms, 112.5ms ...
    LOG(INFO) << "There are still outstanding Velox memory allocations. Waiting for " << waitMs
              << " ms to let possible async tasks done... ";
    usleep(waitMs * 1000);
    accumulatedWaitMs += waitMs;
  }
}

} // namespace gluten
