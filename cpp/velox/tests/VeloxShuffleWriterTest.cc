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
#include "shuffle/rss/CelebornPartitionWriter.h"
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

facebook::velox::RowVectorPtr takeRows(const facebook::velox::RowVectorPtr& source, const std::vector<int32_t>& idxs) {
  facebook::velox::RowVectorPtr copy = facebook::velox::RowVector::createEmpty(source->type(), source->pool());
  for (int32_t idx : idxs) {
    copy->append(source->slice(idx, 1).get());
  }
  return copy;
}

std::vector<ShuffleTestParams> createShuffleTestParams() {
  std::vector<ShuffleTestParams> params;

  std::vector<PartitionWriterType> writerTypes = {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn};

  std::vector<arrow::Compression::type> compressions = {
      arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD};

  std::vector<CompressionMode> modes = {CompressionMode::BUFFER, CompressionMode::ROWVECTOR};

  for (const auto& writerType : writerTypes) {
    for (const auto& compression : compressions) {
      for (const auto& mode : modes) {
        params.push_back(ShuffleTestParams{writerType, compression, mode});
      }
    }
  }

  return params;
}

static const auto kShuffleWriteTestParams = createShuffleTestParams();

} // namespace

TEST_P(SinglePartitioningShuffleWriter, single) {
  // Split 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter();
    testShuffleWrite(*shuffleWriter, {inputVector1_});
  }
  // Split > 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter();
    testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector2_, inputVector1_});
  }
  // Test not compress small buffer.
  {
    shuffleWriterOptions_.compression_type = arrow::Compression::LZ4_FRAME;
    shuffleWriterOptions_.compression_threshold = 1024;
    auto shuffleWriter = createShuffleWriter();
    testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector1_});
  }
  // Split null RowVector.
  {
    auto shuffleWriter = createShuffleWriter();
    auto vector = makeRowVector({
        makeNullableFlatVector<int32_t>({std::nullopt}),
        makeNullableFlatVector<velox::StringView>({std::nullopt}),
    });
    testShuffleWrite(*shuffleWriter, {vector});
  }
  // Other types.
  {
    auto shuffleWriter = createShuffleWriter();
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
  auto shuffleWriter = createShuffleWriter();
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
  auto shuffleWriter = createShuffleWriter();
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
  auto shuffleWriter = createShuffleWriter();

  auto block1Pid1 = takeRows(inputVector1_, {0, 5, 6, 7, 9});
  auto block2Pid1 = takeRows(inputVector2_, {});

  auto block1Pid2 = takeRows(inputVector1_, {1, 2, 3, 4, 8});
  auto block2Pid2 = takeRows(inputVector2_, {0, 1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {hashInputVector1_, hashInputVector2_, hashInputVector1_},
      2,
      inputVector1_->type(),
      {{block1Pid2, block2Pid2, block1Pid2}, {block1Pid1, block1Pid1}});
}

TEST_P(RangePartitioningShuffleWriter, rangePartition) {
  auto shuffleWriter = createShuffleWriter();

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block2Pid1 = takeRows(inputVector2_, {0});

  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});
  auto block2Pid2 = takeRows(inputVector2_, {1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {compositeBatch1_, compositeBatch2_, compositeBatch1_},
      2,
      inputVector1_->type(),
      {{block1Pid1, block2Pid1, block1Pid1}, {block1Pid2, block2Pid2, block1Pid2}});
}

TEST_P(RoundRobinPartitioningShuffleWriter, roundRobin) {
  auto shuffleWriter = createShuffleWriter();

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block2Pid1 = takeRows(inputVector2_, {0});

  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});
  auto block2Pid2 = takeRows(inputVector2_, {1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter,
      {inputVector1_, inputVector2_, inputVector1_},
      2,
      inputVector1_->type(),
      {{block1Pid1, block2Pid1, block1Pid1}, {block1Pid2, block2Pid2, block1Pid2}});
}

TEST_P(RoundRobinPartitioningShuffleWriter, preAllocForceRealloc) {
  shuffleWriterOptions_.buffer_realloc_threshold = 0; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter();

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
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(cachedPayloadSize, &evicted));
  // Check only cached data being spilled.
  ASSERT_EQ(evicted, cachedPayloadSize);
  VELOX_CHECK_EQ(shuffleWriter->partitionBufferSize(), partitionBufferBeforeEvict);

  // Split more data with null. New buffer size is larger.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Split more data with null. New buffer size is smaller.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));

  // Split more data with null. New buffer size is larger and current data is preserved.
  // Evict cached data first.
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  // Set a large buffer size.
  shuffleWriter->options().buffer_size = 100;
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  // No data got evicted so the cached size is 0.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_P(RoundRobinPartitioningShuffleWriter, preAllocForceReuse) {
  shuffleWriterOptions_.buffer_realloc_threshold = 100; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter();

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
  auto shuffleWriter = createShuffleWriter();

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Clear buffers and evict payloads and cache.
  for (auto pid : {0, 1}) {
    GLUTEN_ASSIGN_OR_THROW(auto payload, shuffleWriter->createPayloadFromBuffer(pid, true));
    if (payload) {
      ASSERT_NOT_OK(shuffleWriter->evictPayload(pid, std::move(payload)));
    }
  }

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Evict all payloads and spill.
  int64_t evicted;
  auto cachedPayloadSize = shuffleWriter->cachedPayloadSize();
  auto partitionBufferSize = shuffleWriter->partitionBufferSize();
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(cachedPayloadSize + partitionBufferSize, &evicted));

  ASSERT_EQ(evicted, cachedPayloadSize + partitionBufferSize);

  // No more cached payloads after spill.
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});

  // Stop and verify.
  shuffleWriteReadMultiBlocks(
      *shuffleWriter,
      2,
      inputVector1_->type(),
      {{block1Pid1, block1Pid1, block1Pid1}, {block1Pid2, block1Pid2, block1Pid2}});
}

TEST_F(VeloxShuffleWriterMemoryTest, memoryLeak) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LimitedMemoryPool>();
  shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.buffer_size = 4;

  auto shuffleWriter = createShuffleWriter();

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  ASSERT_NOT_OK(shuffleWriter->stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  shuffleWriter.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_F(VeloxShuffleWriterMemoryTest, spillFailWithOutOfMemory) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LimitedMemoryPool>(0);
  shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.buffer_size = 4;

  auto shuffleWriter = createShuffleWriter();

  auto status = splitRowVector(*shuffleWriter, inputVector1_);

  // Should return OOM status because there's no partition buffer to spill.
  ASSERT_TRUE(status.IsOutOfMemory());
}

TEST_F(VeloxShuffleWriterMemoryTest, kInit) {
  shuffleWriterOptions_.buffer_size = 4;
  auto shuffleWriter = createShuffleWriter();

  // Test spill all partition buffers.
  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(payloadSize + bufferSize, &evicted));
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
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(payloadSize + 1, &evicted));
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
      GLUTEN_ASSIGN_OR_THROW(auto payload, shuffleWriter->createPayloadFromBuffer(pid, true));
      if (payload) {
        ASSERT_NOT_OK(shuffleWriter->evictPayload(pid, std::move(payload)));
      }
    }

    auto bufferSize = shuffleWriter->partitionBufferSize();
    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    // Evict payload and shrink min-size buffer.
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(payloadSize + 1, &evicted));
    ASSERT_GT(evicted, payloadSize);
    ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
    // Evict empty partition buffers.
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(bufferSize, &evicted));
    ASSERT_GT(evicted, 0);
    ASSERT_EQ(shuffleWriter->partitionBufferSize(), 0);
    // Evict again. No reclaimable space.
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(1, &evicted));
    ASSERT_EQ(evicted, 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxShuffleWriterMemoryTest, kInitSingle) {
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  shuffleWriterOptions_.buffer_size = 4;
  auto shuffleWriter = createShuffleWriter();

  {
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector2_));
    ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

    auto payloadSize = shuffleWriter->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter->evictFixedSize(payloadSize + 1, &evicted));
    ASSERT_EQ(evicted, payloadSize);
    // No cached payload after evict.
    ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  }

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxShuffleWriterMemoryTest, kSplit) {
  shuffleWriterOptions_.buffer_size = 4;
  auto pool = SelfEvictedMemoryPool(shuffleWriterOptions_.memory_pool);
  shuffleWriterOptions_.memory_pool = &pool;
  auto shuffleWriter = createShuffleWriter();

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
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  auto pool = SelfEvictedMemoryPool(shuffleWriterOptions_.memory_pool);
  shuffleWriterOptions_.memory_pool = &pool;
  auto shuffleWriter = createShuffleWriter();

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // Trigger spill for the next split.
  ASSERT_TRUE(pool.checkEvict(
      shuffleWriter->cachedPayloadSize() * 2, [&] { ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_)); }));

  ASSERT_NOT_OK(shuffleWriter->stop());
}

TEST_F(VeloxShuffleWriterMemoryTest, kStop) {
  auto delegated = shuffleWriterOptions_.memory_pool;
  for (const auto partitioning : {Partitioning::kSingle, Partitioning::kRoundRobin}) {
    shuffleWriterOptions_.partitioning = partitioning;
    shuffleWriterOptions_.buffer_size = 4;
    auto pool = SelfEvictedMemoryPool(delegated);
    shuffleWriterOptions_.memory_pool = &pool;
    auto shuffleWriter = createShuffleWriter();

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

TEST_F(VeloxShuffleWriterMemoryTest, kUnevictable) {
  auto delegated = shuffleWriterOptions_.memory_pool;
  shuffleWriterOptions_.buffer_size = 4;
  auto pool = SelfEvictedMemoryPool(delegated);
  shuffleWriterOptions_.memory_pool = &pool;
  auto shuffleWriter = createShuffleWriter();

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  ASSERT_GT(shuffleWriter->partitionBufferSize(), 0);
  // Set limited capacity.
  pool.setCapacity(0);
  // Evict again. Because no cached payload to evict, it will try to compress and evict partition buffers.
  // Throws OOM during allocating compression buffers.
  auto status = shuffleWriter->evictFixedSize(shuffleWriter->partitionBufferSize(), &evicted);
  ASSERT_TRUE(status.IsOutOfMemory());
}

TEST_F(VeloxShuffleWriterMemoryTest, kUnevictableSingle) {
  auto delegated = shuffleWriterOptions_.memory_pool;
  shuffleWriterOptions_.partitioning = Partitioning::kSingle;
  auto pool = SelfEvictedMemoryPool(delegated);
  shuffleWriterOptions_.memory_pool = &pool;
  auto shuffleWriter = createShuffleWriter();

  pool.setEvictable(shuffleWriter.get());

  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_));

  // First evict cached payloads.
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(shuffleWriter->cachedPayloadSize(), &evicted));
  ASSERT_EQ(shuffleWriter->cachedPayloadSize(), 0);
  // Set limited capacity.
  pool.setCapacity(0);
  // Evict again. Single partitioning doesn't have partition buffers, so the evicted size is 0.
  ASSERT_NOT_OK(shuffleWriter->evictFixedSize(shuffleWriter->partitionBufferSize(), &evicted));
  ASSERT_EQ(evicted, 0);
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
