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

#include "config/GlutenConfig.h"
#include "shuffle/VeloxHashShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "utils/TestAllocationListener.h"
#include "utils/TestUtils.h"

using namespace facebook::velox;

namespace gluten {

namespace {
void assertSpill(TestAllocationListener* listener, std::function<void()> block) {
  const auto beforeSpill = listener->reclaimedBytes();
  block();
  const auto afterSpill = listener->reclaimedBytes();

  ASSERT_GT(afterSpill, beforeSpill);
}
} // namespace

class VeloxHashShuffleWriterSpillTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    setUpTestData();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) override {
    auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();
    auto veloxPool = getDefaultMemoryManager()->getLeafMemoryPool();

    auto partitionWriter = createPartitionWriter(
        PartitionWriterType::kLocal, numPartitions, dataFile_, localDirs_, partitionWriterOptions_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxHashShuffleWriter::create(
            numPartitions, std::move(partitionWriter), shuffleWriterOptions_, veloxPool, arrowPool));

    return shuffleWriter;
  }

  int64_t splitRowVectorAndSpill(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<facebook::velox::RowVectorPtr> vectors,
      bool shrink) {
    for (auto vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }

    auto targetEvicted = shuffleWriter.cachedPayloadSize();
    if (shrink) {
      targetEvicted += shuffleWriter.partitionBufferSize();
    }
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter.reclaimFixedSize(targetEvicted, &evicted));

    return evicted;
  };
};

TEST_F(VeloxHashShuffleWriterSpillTest, memoryLeak) {
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(2);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  ASSERT_NOT_OK(shuffleWriter->stop());

  const auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();

  ASSERT_EQ(arrowPool->bytes_allocated(), 0);

  shuffleWriter.reset();
  ASSERT_EQ(arrowPool->bytes_allocated(), 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, spillFailWithOutOfMemory) {
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(2);

  listener_->updateLimit(0L);
  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->setThrowIfOOM(true);

  ASSERT_THROW([&] { auto status = splitRowVector(*shuffleWriter, inputVector1_); }(), GlutenException);

  // Should return OOM status because there's no partition buffer to spill.

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kInit) {
  shuffleWriterOptions_.bufferSize = 4;

  const int32_t numPartitions = 2;
  auto shuffleWriter = createShuffleWriter(numPartitions);

  // Test spill all partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + bufferSize, &evicted));
    ASSERT_EQ(evicted, payloadSize + bufferSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
    // All partition buffers should be evicted.
    ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
  }

  // Test spill minimum-size partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + 1, &evicted));
    ASSERT_GT(evicted, payloadSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
    ASSERT_LE(shuffleWriter->partitionBufferSize(), bufferSize);
    // Not all partition buffers was evicted.
    ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
  }

  // Test spill empty partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    // Clear buffers then the size after shrink will be 0.
    for (auto pid = 0; pid < numPartitions; ++pid) {
      ASSERT_NOT_OK(shuffleWriter->evictPartitionBuffers(pid, true));
    }

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    // Evict payload and shrink min-size buffer.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize + 1, &evicted));
    ASSERT_GT(evicted, payloadSize);
    ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
    // Evict empty partition buffers.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(bufferSize, &evicted));
    ASSERT_GT(evicted, 0);
    ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
    // Evict again. No reclaimable space.
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(1, &evicted));
    ASSERT_EQ(evicted, 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxHashShuffleWriterSpillTest, kInitSingle) {
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(2);

  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto payloadSize = shuffleWriter->partitionBufferSize() + shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize, &evicted));
    ASSERT_GE(evicted, payloadSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxHashShuffleWriterSpillTest, kSplit) {
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(2);

  listener_->setShuffleWriter(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&]() {
    // Trigger spill for the next split.
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kSplitSingle) {
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;

  auto shuffleWriter = createShuffleWriter(1);

  listener_->setShuffleWriter(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Trigger spill for the next split.
  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&]() { ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_)); });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kStop) {
  shuffleWriterOptions_.bufferSize = 4096;
  // Force compression.
  partitionWriterOptions_.compressionThreshold = 0;
  partitionWriterOptions_.mergeThreshold = 0;

  auto shuffleWriter = createShuffleWriter(2);

  for (int i = 0; i < 10; ++i) {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  }
  // Reclaim bytes to shrink partition buffer.
  int64_t reclaimed = 0;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(2000, &reclaimed));
  ASSERT_GE(reclaimed, 2000);

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  // Trigger spill during stop.
  assertSpill(listener_, [&] { ASSERT_NOT_OK(shuffleWriter->stop()); });

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, kStopComplex) {
  shuffleWriterOptions_.bufferSize = 4096;

  // Force compression.
  partitionWriterOptions_.compressionThreshold = 0;
  partitionWriterOptions_.mergeThreshold = 0;

  auto shuffleWriter = createShuffleWriter(2);

  for (int i = 0; i < 3; ++i) {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorComplex_));
  }

  // Reclaim bytes to shrink partition buffer.
  int64_t reclaimed = 0;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(2000, &reclaimed));
  ASSERT_GE(reclaimed, 2000);

  // Reclaim from PartitionWriter to free cached bytes.
  auto payloadSize = shuffleWriter->cachedPayloadSize();
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(payloadSize, &evicted));
  ASSERT_EQ(evicted, payloadSize);

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  // When evicting partitioning buffers in stop, spill will be triggered by complex types allocating extra memory.
  assertSpill(listener_, [&] { ASSERT_NOT_OK(shuffleWriter->stop()); });

  listener_->reset();
}

TEST_F(VeloxHashShuffleWriterSpillTest, evictPartitionBuffers) {
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(2);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);

  // Evict again. Because no cached payload to evict, it will try to evict all partition buffers.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->partitionBufferSize(), &evicted));
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, kUnevictableSingle) {
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;

  auto shuffleWriter = createShuffleWriter(1);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(
      shuffleWriter->partitionBufferSize() + shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GE(evicted, 0);

  // Evict again. The evicted size should be 0.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(1, &evicted));
  ASSERT_EQ(evicted, 0);
}

TEST_F(VeloxHashShuffleWriterSpillTest, resizeBinaryBufferTriggerSpill) {
  shuffleWriterOptions_.bufferReallocThreshold = 1;
  partitionWriterOptions_.compressionType = arrow::Compression::type::UNCOMPRESSED;

  auto shuffleWriter = createShuffleWriter(1);

  // Split first input vector. Large average string length.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary1_));

  listener_->setShuffleWriter(shuffleWriter.get());
  listener_->updateLimit(listener_->currentBytes());

  assertSpill(listener_, [&] {
    // Split second input vector. Large average string length.
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary2_));
  });

  ASSERT_NOT_OK(shuffleWriter->stop());

  listener_->reset();
}

} // namespace gluten