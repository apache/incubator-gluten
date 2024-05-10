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

#include <arrow/c/bridge.h>
#include <arrow/io/api.h>

#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/tests/MemoryPoolUtils.h"
#include "utils/tests/VeloxShuffleWriterTestBase.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace arrow;
using namespace arrow::ipc;

namespace gluten {

namespace {

facebook::velox::RowVectorPtr takeRows(
    const std::vector<facebook::velox::RowVectorPtr>& sources,
    const std::vector<std::vector<int32_t>>& indices) {
  facebook::velox::RowVectorPtr copy = facebook::velox::RowVector::createEmpty(sources[0]->type(), sources[0]->pool());
  for (size_t i = 0; i < sources.size(); ++i) {
    if (indices[i].empty()) {
      // Take all rows;
      copy->append(sources[i].get());
      continue;
    }
    for (int32_t idx : indices[i]) {
      if (idx >= sources[i]->size()) {
        throw GlutenException(
            "Index out of bound: " + std::to_string(idx) + ". Source RowVector " + std::to_string(i) +
            " num rows: " + std::to_string(sources[i]->size()));
      }
      copy->append(sources[i]->slice(idx, 1).get());
    }
  }
  return copy;
}

std::vector<ShuffleTestParams> createShuffleTestParams() {
  std::vector<ShuffleTestParams> params;

  std::vector<arrow::Compression::type> compressions = {
      arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD};

  std::vector<int32_t> compressionThresholds = {-1, 0, 3, 4, 10, 4096};
  std::vector<int32_t> mergeBufferSizes = {0, 3, 4, 10, 4096};

  for (const auto& compression : compressions) {
    for (const auto compressionThreshold : compressionThresholds) {
      for (const auto mergeBufferSize : mergeBufferSizes) {
        params.push_back(
            ShuffleTestParams{PartitionWriterType::kLocal, compression, compressionThreshold, mergeBufferSize});
      }
      params.push_back(ShuffleTestParams{PartitionWriterType::kRss, compression, compressionThreshold, 0});
    }
  }

  return params;
}

static const auto kShuffleWriteTestParams = createShuffleTestParams();

} // namespace

TEST_P(SinglePartitioningShuffleWriter, single) {
  // Split 1 RowVector.
  {
    ASSERT_NOT_OK(initShuffleWriterOptions());
    auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
    testShuffleWrite(*shuffleWriter, {inputVector1_});
  }
  // Split > 1 RowVector.
  {
    ASSERT_NOT_OK(initShuffleWriterOptions());
    auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
    auto resultBlock = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{}, {}, {}});
    testShuffleWrite(*shuffleWriter, {resultBlock});
  }
  // Split null RowVector.
  {
    ASSERT_NOT_OK(initShuffleWriterOptions());
    auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
    auto vector = makeRowVector({
        makeNullableFlatVector<int32_t>({std::nullopt}),
        makeNullableFlatVector<velox::StringView>({std::nullopt}),
    });
    testShuffleWrite(*shuffleWriter, {vector});
  }
  // Other types.
  {
    ASSERT_NOT_OK(initShuffleWriterOptions());
    auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
    auto vector = makeRowVector({
        makeNullableFlatVector<int32_t>({std::nullopt, 1}),
        makeNullableFlatVector<StringView>({std::nullopt, "10"}),
        makeNullableFlatVector<int64_t>({232, 34567235}, DECIMAL(12, 4)),
        makeNullableFlatVector<int128_t>({232, 34567235}, DECIMAL(20, 4)),
        makeFlatVector<int32_t>(
            2, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE()),
        makeNullableFlatVector<int32_t>({std::nullopt, 1}),
        makeRowVector({
            makeFlatVector<int32_t>({1, 3}),
            makeNullableFlatVector<velox::StringView>({std::nullopt, "de"}),
        }),
        makeNullableFlatVector<StringView>({std::nullopt, "10 I'm not inline string"}),
        makeArrayVector<int64_t>({
            {1, 2, 3, 4, 5},
            {1, 2, 3},
        }),
        makeMapVector<int32_t, StringView>({{{1, "str1000"}, {2, "str2000"}}, {{3, "str3000"}, {4, "str4000"}}}),
    });
    testShuffleWrite(*shuffleWriter, {vector});
  }
}

TEST_P(HashPartitioningShuffleWriter, hashPart1Vector) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
  auto vector = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1, 2}),
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<velox::StringView>({"nn", "re", "fr", "juiu"}),
      makeFlatVector<int64_t>({232, 34567235, 1212, 4567}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({232, 34567235, 1212, 4567}, DECIMAL(20, 4)),
      makeFlatVector<int32_t>(
          4, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE()),
      makeFlatVector<Timestamp>(
          4,
          [](vector_size_t row) {
            return Timestamp{row % 2, 0};
          },
          nullEvery(5)),
  });

  auto dataVector = makeRowVector({
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<velox::StringView>({"nn", "re", "fr", "juiu"}),
      makeFlatVector<int64_t>({232, 34567235, 1212, 4567}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({232, 34567235, 1212, 4567}, DECIMAL(20, 4)),
      makeFlatVector<int32_t>(
          4, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE()),
      makeFlatVector<Timestamp>(
          4,
          [](vector_size_t row) {
            return Timestamp{row % 2, 0};
          },
          nullEvery(5)),
  });

  auto firstBlock = makeRowVector({
      makeNullableFlatVector<int8_t>({2, std::nullopt}),
      makeFlatVector<int64_t>({2, 4}),
      makeFlatVector<velox::StringView>({"re", "juiu"}),
      makeFlatVector<int64_t>({34567235, 4567}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({34567235, 4567}, DECIMAL(20, 4)),
      makeFlatVector<int32_t>({1, 1}, DATE()),
      makeFlatVector<Timestamp>({Timestamp(1, 0), Timestamp(1, 0)}),
  });

  auto secondBlock = makeRowVector({
      makeNullableFlatVector<int8_t>({1, 3}),
      makeFlatVector<int64_t>({1, 3}),
      makeFlatVector<velox::StringView>({"nn", "fr"}),
      makeFlatVector<int64_t>({232, 1212}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({232, 1212}, DECIMAL(20, 4)),
      makeNullableFlatVector<int32_t>({std::nullopt, 0}, DATE()),
      makeNullableFlatVector<Timestamp>({std::nullopt, Timestamp(0, 0)}),
  });

  testShuffleWriteMultiBlocks(*shuffleWriter, {vector}, 2, dataVector->type(), {{firstBlock}, {secondBlock}});
}

TEST_P(HashPartitioningShuffleWriter, hashPart1VectorComplexType) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int32_t>({std::nullopt, 1}),
      makeRowVector({
          makeFlatVector<int32_t>({1, 3}),
          makeNullableFlatVector<velox::StringView>({std::nullopt, "de"}),
      }),
      makeNullableFlatVector<StringView>({std::nullopt, "10 I'm not inline string"}),
      makeArrayVector<int64_t>({
          {1, 2, 3, 4, 5},
          {1, 2, 3},
      }),
      makeMapVector<int32_t, StringView>({{{1, "str1000"}, {2, "str2000"}}, {{3, "str3000"}, {4, "str4000"}}}),
  };
  auto dataVector = makeRowVector(children);
  children.insert((children.begin()), makeFlatVector<int32_t>({1, 2}));
  auto vector = makeRowVector(children);

  auto firstBlock = makeRowVector({
      makeConstant<int32_t>(1, 1),
      makeRowVector({
          makeConstant<int32_t>(3, 1),
          makeFlatVector<velox::StringView>({"de"}),
      }),
      makeFlatVector<StringView>({"10 I'm not inline string"}),
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeMapVector<int32_t, StringView>({{{3, "str3000"}, {4, "str4000"}}}),
  });

  auto secondBlock = makeRowVector({
      makeNullConstant(TypeKind::INTEGER, 1),
      makeRowVector({
          makeConstant<int32_t>(1, 1),
          makeNullableFlatVector<velox::StringView>({std::nullopt}),
      }),
      makeNullableFlatVector<StringView>({std::nullopt}),
      makeArrayVector<int64_t>({
          {1, 2, 3, 4, 5},
      }),
      makeMapVector<int32_t, StringView>({{{1, "str1000"}, {2, "str2000"}}}),
  });

  testShuffleWriteMultiBlocks(*shuffleWriter, {vector}, 2, dataVector->type(), {{firstBlock}, {secondBlock}});
}

TEST_P(HashPartitioningShuffleWriter, hashPart3Vectors) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {0, 1}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_}, {{0, 5, 6, 7, 9, 0, 5, 6, 7, 9}});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {hashInputVector1_, hashInputVector2_, hashInputVector1_},
      2,
      inputVector1_->type(),
      {{blockPid2}, {blockPid1}});
}

TEST_P(HashPartitioningShuffleWriter, hashLargeVectors) {
  const int32_t expectedMaxBatchSize = 8;
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());
  // calculate maxBatchSize_
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, hashInputVector1_));
  VELOX_CHECK_EQ(shuffleWriter->maxBatchSize(), expectedMaxBatchSize);

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {0, 1}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_}, {{0, 5, 6, 7, 9, 0, 5, 6, 7, 9}});

  VELOX_CHECK(hashInputVector1_->size() > expectedMaxBatchSize);
  testShuffleWriteMultiBlocks(
      *shuffleWriter, {hashInputVector2_, hashInputVector1_}, 2, inputVector1_->type(), {{blockPid2}, {blockPid1}});
}

TEST_P(RangePartitioningShuffleWriter, rangePartition) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {compositeBatch1_, compositeBatch2_, compositeBatch1_},
      2,
      inputVector1_->type(),
      {{blockPid1}, {blockPid2}});
}

TEST_P(RoundRobinPartitioningShuffleWriter, roundRobin) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {inputVector1_, inputVector2_, inputVector1_},
      2,
      inputVector1_->type(),
      {{blockPid1}, {blockPid2}});
}

TEST_P(RoundRobinPartitioningShuffleWriter, preAllocForceRealloc) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferReallocThreshold = 0; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  // First spilt no null.
  auto inputNoNull = inputVectorNoNull_;

  // Second split has null. Continue filling current partition buffers.
  std::vector<VectorPtr> intHasNull = {
      makeNullableFlatVector<int8_t>({std::nullopt, 1}),
      makeNullableFlatVector<int8_t>({std::nullopt, -1}),
      makeNullableFlatVector<int32_t>({std::nullopt, 100}),
      makeNullableFlatVector<int64_t>({0, 1}),
      makeNullableFlatVector<float>({0, 0.142857}),
      makeNullableFlatVector<bool>({false, true}),
      makeNullableFlatVector<velox::StringView>({"", "alice"}),
      makeNullableFlatVector<velox::StringView>({"alice", ""}),
  };

  auto inputHasNull = makeRowVector(intHasNull);
  // Split first input no null.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Split second input, continue filling but update null.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputHasNull));

  // Split first input again.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Check when buffer is full, evict current buffers and reuse.
  auto cachedPayloadSize = shuffleWriter->cachedPayloadSize();
  auto partitionBufferBeforeEvict = shuffleWriter->partitionBufferSize();
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(cachedPayloadSize, &evicted));
  // Check only cached data being spilled.
  ASSERT_EQ(evicted, cachedPayloadSize);
  VELOX_CHECK_EQ(shuffleWriter->partitionBufferSize(), partitionBufferBeforeEvict);

  // Split more data with null. New buffer size is larger.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Split more data with null. New buffer size is smaller.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));

  // Split more data with null. New buffer size is larger and current data is preserved.
  // Evict cached data first.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  // Set a large buffer size.
  shuffleWriter->setPartitionBufferSize(100);
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  // No data got evicted so the cached size is 0.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_P(RoundRobinPartitioningShuffleWriter, preAllocForceReuse) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferReallocThreshold = 1; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  // First spilt no null.
  auto inputNoNull = inputVectorNoNull_;
  // Second split has null int, null string and non-null string,
  auto inputFixedWidthHasNull = inputVector1_;
  // Third split has null string.
  std::vector<VectorPtr> stringHasNull = {
      makeNullableFlatVector<int8_t>({0, 1}),
      makeNullableFlatVector<int8_t>({0, -1}),
      makeNullableFlatVector<int32_t>({0, 100}),
      makeNullableFlatVector<int64_t>({0, 1}),
      makeNullableFlatVector<float>({0, 0.142857}),
      makeNullableFlatVector<bool>({false, true}),
      makeNullableFlatVector<velox::StringView>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<velox::StringView>({std::nullopt, std::nullopt}),
  };
  auto inputStringHasNull = makeRowVector(stringHasNull);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputNoNull));
  // Split more data with null. Already filled + to be filled > buffer size, Buffer is resized larger.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputFixedWidthHasNull));
  // Split more data with null. Already filled + to be filled > buffer size, newSize is smaller so buffer is not
  // resized.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputStringHasNull));

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_P(RoundRobinPartitioningShuffleWriter, spillVerifyResult) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Clear buffers and evict payloads and cache.
  for (auto pid : {0, 1}) {
    ASSERT_NOT_OK(shuffleWriter->evictPartitionBuffers(pid, true));
  }

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Evict all payloads and spill.
  int64_t evicted;
  auto cachedPayloadSize = shuffleWriter->cachedPayloadSize();
  auto partitionBufferSize = shuffleWriter->partitionBufferSize();
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(cachedPayloadSize + partitionBufferSize, &evicted));

  ASSERT_EQ(evicted, cachedPayloadSize + partitionBufferSize);

  // No more cached payloads after spill.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  auto blockPid1 = takeRows({inputVector1_}, {{0, 2, 4, 6, 8, 0, 2, 4, 6, 8, 0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_}, {{1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9}});

  // Stop and verify.
  shuffleWriteReadMultiBlocks(
      *shuffleWriter,
      2,
      inputVector1_->type(),
      //      {{block1Pid1, block1Pid1, block1Pid1}, {block1Pid2, block1Pid2, block1Pid2}});
      {{blockPid1}, {blockPid2}});
}

TEST_F(VeloxShuffleWriterMemoryTest, memoryLeak) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LimitedMemoryPool>();
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(pool.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  ASSERT_NOT_OK(shuffleWriter->stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  shuffleWriter.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_F(VeloxShuffleWriterMemoryTest, spillFailWithOutOfMemory) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LimitedMemoryPool>(0);
  shuffleWriterOptions_.bufferSize = 4;

  auto shuffleWriter = createShuffleWriter(pool.get());

  auto status = splitRowVector(*shuffleWriter, inputVector1_);

  // Should return OOM status because there's no partition buffer to spill.
  ASSERT_TRUE(status.IsOutOfMemory());
}

TEST_F(VeloxShuffleWriterMemoryTest, kInit) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferSize = 4;
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

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
    for (auto pid = 0; pid < kDefaultShufflePartitions; ++pid) {
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

TEST_F(VeloxShuffleWriterMemoryTest, kInitSingle) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  shuffleWriterOptions_.bufferSize = 4;
  auto shuffleWriter = createShuffleWriter(defaultArrowMemoryPool().get());

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

TEST_F(VeloxShuffleWriterMemoryTest, kSplit) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferSize = 4;
  auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get());
  auto shuffleWriter = createShuffleWriter(&pool);

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Trigger spill for the next split.
  ASSERT_TRUE(pool.checkEvict(pool.bytes_allocated(), [&] {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  }));

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxShuffleWriterMemoryTest, kSplitSingle) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get(), false);

  auto shuffleWriter = createShuffleWriter(1, &pool);

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Trigger spill for the next split.
  ASSERT_TRUE(pool.checkEvict(
      shuffleWriter->cachedPayloadSize(), [&] { ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_)); }));

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxShuffleWriterMemoryTest, kStop) {
  for (const auto partitioning : {Partitioning::kSingle, Partitioning::kRoundRobin}) {
    ASSERT_NOT_OK(initShuffleWriterOptions());
    shuffleWriterOptions_.partitioning = partitioning;
    shuffleWriterOptions_.bufferSize = 4;
    auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get(), false);
    auto shuffleWriter = createShuffleWriter(&pool);

    pool.setEvictable(shuffleWriter.get());

    for (int i = 0; i < 10; ++i) {
      ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
      ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
      ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    }

    // Trigger spill during stop.
    // For single partitioning, spill is triggered by allocating buffered output stream.
    ASSERT_TRUE(pool.checkEvict(pool.bytes_allocated(), [&] { ASSERT_NOT_OK(shuffleWriter->stop()); }));
  }
}

TEST_F(VeloxShuffleWriterMemoryTest, evictPartitionBuffers) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferSize = 4;
  auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get());
  auto shuffleWriter = createShuffleWriter(&pool);

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
  // Set limited capacity.
  pool.setCapacity(0);
  // Evict again. Because no cached payload to evict, it will try to evict all partition buffers.
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->partitionBufferSize(), &evicted));
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
}

TEST_F(VeloxShuffleWriterMemoryTest, kUnevictableSingle) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get());
  auto shuffleWriter = createShuffleWriter(&pool);

  pool.setEvictable(shuffleWriter.get());

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

TEST_F(VeloxShuffleWriterMemoryTest, resizeBinaryBufferTriggerSpill) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferReallocThreshold = 1;
  auto pool = SelfEvictedMemoryPool(defaultArrowMemoryPool().get(), false);
  auto shuffleWriter = createShuffleWriter(&pool);

  pool.setEvictable(shuffleWriter.get());

  // Split first input vector. Large average string length.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary1_));

  // Evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->reclaimFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  // Set limited capacity.
  ASSERT_TRUE(pool.checkEvict(pool.bytes_allocated(), [&] {
    // Split second input vector. Large average string length.
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVectorLargeBinary2_));
  }));
  ASSERT_NOT_OK(shuffleWriter->stop());
}

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    SinglePartitioningShuffleWriter,
    ::testing::ValuesIn(kShuffleWriteTestParams));

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    RoundRobinPartitioningShuffleWriter,
    ::testing::ValuesIn(kShuffleWriteTestParams));

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    HashPartitioningShuffleWriter,
    ::testing::ValuesIn(kShuffleWriteTestParams));

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    RangePartitioningShuffleWriter,
    ::testing::ValuesIn(kShuffleWriteTestParams));
} // namespace gluten
