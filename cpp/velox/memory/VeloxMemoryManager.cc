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
#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/MemoryReclaimer.h"

#include "compute/VeloxBackend.h"
#include "config/VeloxConfig.h"
#include "memory/ArrowMemoryPool.h"
#include "utils/exception.h"

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);

namespace gluten {

using namespace facebook;

namespace {

static constexpr std::string_view kMemoryPoolInitialCapacity{"memory-pool-initial-capacity"};
static constexpr uint64_t kDefaultMemoryPoolInitialCapacity{256 << 20};
static constexpr std::string_view kMemoryPoolTransferCapacity{"memory-pool-transfer-capacity"};
static constexpr uint64_t kDefaultMemoryPoolTransferCapacity{128 << 20};

template <typename T>
T getConfig(
    const std::unordered_map<std::string, std::string>& configs,
    const std::string_view& key,
    const T& defaultValue) {
  if (configs.count(std::string(key)) > 0) {
    try {
      return folly::to<T>(configs.at(std::string(key)));
    } catch (const std::exception& e) {
      VELOX_USER_FAIL("Failed while parsing SharedArbitrator configs: {}", e.what());
    }
  }
  return defaultValue;
}
} // namespace
/// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config),
        listener_(listener),
        memoryPoolInitialCapacity_(
            getConfig<uint64_t>(config.extraConfigs, kMemoryPoolInitialCapacity, kDefaultMemoryPoolInitialCapacity)),
        memoryPoolTransferCapacity_(
            getConfig<uint64_t>(config.extraConfigs, kMemoryPoolTransferCapacity, kDefaultMemoryPoolTransferCapacity)) {
  }
  std::string kind() const override {
    return kind_;
  }

  void addPool(const std::shared_ptr<velox::memory::MemoryPool>& pool) override {
    VELOX_CHECK_EQ(pool->capacity(), 0);

    std::unique_lock guard{mutex_};
    VELOX_CHECK_EQ(candidates_.count(pool.get()), 0);
    candidates_.emplace(pool.get(), pool->weak_from_this());
  }

  void removePool(velox::memory::MemoryPool* pool) override {
    VELOX_CHECK_EQ(pool->reservedBytes(), 0);
    shrinkCapacity(pool, pool->capacity());

    std::unique_lock guard{mutex_};
    const auto ret = candidates_.erase(pool);
    VELOX_CHECK_EQ(ret, 1);
  }

  bool growCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    velox::memory::ScopedMemoryArbitrationContext ctx(pool);
    velox::memory::MemoryPool* candidate;
    {
      std::unique_lock guard{mutex_};
      VELOX_CHECK_EQ(candidates_.size(), 1, "ListenableArbitrator should only be used within a single root pool")
      candidate = candidates_.begin()->first;
    }
    VELOX_CHECK(pool->root() == candidate, "Illegal state in ListenableArbitrator");

    growCapacity0(pool->root(), targetBytes);
    return true;
  }

  uint64_t shrinkCapacity(uint64_t targetBytes, bool allowSpill, bool allowAbort) override {
    velox::memory::ScopedMemoryArbitrationContext ctx((const velox::memory::MemoryPool*)nullptr);
    facebook::velox::exec::MemoryReclaimer::Stats status;
    velox::memory::MemoryPool* pool;
    {
      std::unique_lock guard{mutex_};
      VELOX_CHECK_EQ(candidates_.size(), 1, "ListenableArbitrator should only be used within a single root pool")
      pool = candidates_.begin()->first;
    }
    pool->reclaim(targetBytes, 0, status); // ignore the output
    return shrinkCapacity0(pool, 0);
  }

  uint64_t shrinkCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    return shrinkCapacity0(pool, targetBytes);
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format("ARBITRATOR[{}] CAPACITY {} {}", kind_, velox::succinctBytes(capacity_), stats().toString());
  }

 private:
  void growCapacity0(velox::memory::MemoryPool* pool, uint64_t bytes) {
    // Since
    // https://github.com/facebookincubator/velox/pull/9557/files#diff-436e44b7374032f8f5d7eb45869602add6f955162daa2798d01cc82f8725724dL812-L820,
    // We should pass bytes as parameter "reservationBytes" when calling ::grow.
    auto freeByes = pool->freeBytes();
    if (freeByes > bytes) {
      if (growPool(pool, 0, bytes)) {
        return;
      }
    }
    auto reclaimedFreeBytes = shrinkPool(pool, 0);
    auto neededBytes = velox::bits::roundUp(bytes - reclaimedFreeBytes, memoryPoolTransferCapacity_);
    listener_->allocationChanged(neededBytes);
    auto ret = growPool(pool, reclaimedFreeBytes + neededBytes, bytes);
    VELOX_CHECK(
        ret,
        "{} failed to grow {} bytes, current state {}",
        pool->name(),
        velox::succinctBytes(bytes),
        pool->toString())
  }

  uint64_t shrinkCapacity0(velox::memory::MemoryPool* pool, uint64_t bytes) {
    uint64_t freeBytes = shrinkPool(pool, bytes);
    listener_->allocationChanged(-freeBytes);
    return freeBytes;
  }

  gluten::AllocationListener* listener_;
  const uint64_t memoryPoolInitialCapacity_; // FIXME: Unused.
  const uint64_t memoryPoolTransferCapacity_;

  mutable std::mutex mutex_;
  inline static std::string kind_ = "GLUTEN";
  std::unordered_map<velox::memory::MemoryPool*, std::weak_ptr<velox::memory::MemoryPool>> candidates_;
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

VeloxMemoryManager::VeloxMemoryManager(std::unique_ptr<AllocationListener> listener)
    : MemoryManager(), listener_(std::move(listener)) {
  auto reservationBlockSize = VeloxBackend::get()->getBackendConf()->get<uint64_t>(
      kMemoryReservationBlockSize, kMemoryReservationBlockSizeDefault);
  auto memInitCapacity =
      VeloxBackend::get()->getBackendConf()->get<uint64_t>(kVeloxMemInitCapacity, kVeloxMemInitCapacityDefault);
  blockListener_ = std::make_unique<BlockAllocationListener>(listener_.get(), reservationBlockSize);
  listenableAlloc_ = std::make_unique<ListenableMemoryAllocator>(defaultMemoryAllocator().get(), blockListener_.get());
  arrowPool_ = std::make_unique<ArrowMemoryPool>(listenableAlloc_.get());

  ArbitratorFactoryRegister afr(listener_.get());
  velox::memory::MemoryManagerOptions mmOptions{
      .alignment = velox::memory::MemoryAllocator::kMaxAlignment,
      .trackDefaultUsage = true, // memory usage tracking
      .checkUsageLeak = true, // leak check
      .debugEnabled = false, // debug
      .coreOnAllocationFailureEnabled = false,
      .allocatorCapacity = velox::memory::kMaxMemory,
      .arbitratorKind = afr.getKind(),
      .memoryPoolInitCapacity = memInitCapacity,
      .memoryPoolTransferCapacity = reservationBlockSize,
      .memoryReclaimWaitMs = 0};
  veloxMemoryManager_ = std::make_unique<velox::memory::MemoryManager>(mmOptions);

  veloxAggregatePool_ = veloxMemoryManager_->addRootPool(
      "root",
      velox::memory::kMaxMemory, // the 3rd capacity
      facebook::velox::memory::MemoryReclaimer::create());

  veloxLeafPool_ = veloxAggregatePool_->addLeafChild("default_leaf");
}

namespace {
MemoryUsageStats collectVeloxMemoryUsageStats(const velox::memory::MemoryPool* pool) {
  MemoryUsageStats stats;
  stats.set_current(pool->usedBytes());
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
  VLOG(2) << logPrefix << "Pool has reserved " << pool->usedBytes() << "/" << pool->root()->reservedBytes() << "/"
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
      "gluten::MemoryAllocator", collectGlutenAllocatorMemoryUsageStats(listenableAlloc_.get()));
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
    if (pool && pool->usedBytes() != 0) {
      return false;
    }
  }
  if (veloxLeafPool_ && veloxLeafPool_->usedBytes() != 0) {
    return false;
  }
  if (veloxAggregatePool_ && veloxAggregatePool_->usedBytes() != 0) {
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
  bool destructed = false;
  for (int32_t tryCount = 0; accumulatedWaitMs < kWaitTimeoutMs; tryCount++) {
    destructed = tryDestructSafe();
    if (destructed) {
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
  if (!destructed) {
    LOG(ERROR) << "Failed to release Velox memory manager after " << accumulatedWaitMs
               << "ms as there are still outstanding memory resources. ";
  }
#ifdef ENABLE_JEMALLOC
  je_gluten_malloc_stats_print(NULL, NULL, NULL);
#endif
}

} // namespace gluten
