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

#include "benchmarks/common/BenchmarkUtils.h"
#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "memory/ListenableMemoryAllocator.h"
#include "memory/VeloxMemoryManager.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace gluten {
using namespace facebook::velox;

class MockAllocationListener : public gluten::AllocationListener {
 public:
  void allocationChanged(int64_t diff) override {
    currentBytes_ += diff;
    peakBytes_ = std::max(peakBytes_, currentBytes_);
  }
  int64_t currentBytes() override {
    return currentBytes_;
  }
  int64_t peakBytes() override {
    return peakBytes_;
  }
  uint64_t currentBytes_{0L};
  uint64_t peakBytes_{0L};
};

namespace {
static const uint64_t kMB = 1 << 20;
static const uint64_t memInitCapacity = 32 * kMB;
} // namespace

class MemoryManagerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    std::unordered_map<std::string, std::string> conf = {{kMemInitCapacity, std::to_string(memInitCapacity)}};
    initVeloxBackend(conf);
  }

  void SetUp() override {
    vmm_ = std::make_unique<VeloxMemoryManager>("test", stdAllocator_, std::make_unique<MockAllocationListener>());
    listener_ = vmm_->listener();
    allocator_ = vmm_->allocator();
  }

  std::unique_ptr<VeloxMemoryManager> vmm_;
  AllocationListener* listener_;
  MemoryAllocator* allocator_;

  std::shared_ptr<MemoryAllocator> stdAllocator_ = std::make_shared<StdMemoryAllocator>();

  struct Allocation {
    void* buffer;
    size_t size;
    memory::MemoryPool* pool;
  };
};

TEST_F(MemoryManagerTest, memoryPool) {
  ASSERT_EQ(listener_->currentBytes(), memInitCapacity)
      << "spark.gluten.sql.columnar.backend.velox.memInitCapacity is wrong.";

  auto pool = vmm_->getLeafMemoryPool();
  std::vector<Allocation> allocations;
  std::vector<uint64_t> sizes{30 * kMB, 3 * kMB};
  for (const auto& size : sizes) {
    auto buf = pool->allocate(size);
    allocations.push_back({buf, size, pool.get()});
  }
  EXPECT_EQ(listener_->currentBytes(), 40 * kMB);
  EXPECT_EQ(listener_->peakBytes(), listener_->currentBytes());

  for (auto& allocation : allocations) {
    allocation.pool->free(allocation.buffer, allocation.size);
  }

  auto currentBytes = listener_->currentBytes();
  ASSERT_EQ(vmm_->shrink(0), currentBytes);
  ASSERT_EQ(listener_->currentBytes(), 0);
}

TEST_F(MemoryManagerTest, memoryAllocator) {
  auto baseBytes = listener_->currentBytes();
  std::vector<Allocation> allocations;
  std::vector<uint64_t> sizes{7 * kMB, 3 * kMB};
  for (const auto& size : sizes) {
    Allocation allocation{.size = size};
    allocator_->allocate(size, &allocation.buffer);
    allocations.push_back(allocation);
  }

  EXPECT_EQ(allocator_->getBytes(), 16 * kMB);
  EXPECT_EQ(allocator_->peakBytes(), listener_->peakBytes() - baseBytes);

  for (auto& allocation : allocations) {
    allocator_->free(allocation.buffer, allocation.size);
  }
  ASSERT_EQ(listener_->currentBytes(), baseBytes);
  ASSERT_EQ(allocator_->getBytes(), 0);
}

} // namespace gluten
