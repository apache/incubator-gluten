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

#include "config/GlutenConfig.h"
#include "shuffle/VeloxHashShuffleWriter.h"
#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "shuffle/VeloxSortShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "utils/TestAllocationListener.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"

#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

namespace {
struct ShuffleTestParams {
  ShuffleWriterType shuffleWriterType;
  PartitionWriterType partitionWriterType;
  arrow::Compression::type compressionType;
  int32_t compressionThreshold{0};
  int32_t mergeBufferSize{0};
  int32_t diskWriteBufferSize{0};
  bool useRadixSort{false};

  std::string toString() const {
    std::ostringstream out;
    out << "shuffleWriterType = " << ShuffleWriter::typeToString(shuffleWriterType)
        << ", partitionWriterType = " << PartitionWriter::typeToString(partitionWriterType)
        << ", compressionType = " << arrow::util::Codec::GetCodecAsString(compressionType)
        << ", compressionThreshold = " << compressionThreshold << ", mergeBufferSize = " << mergeBufferSize
        << ", compressionBufferSize = " << diskWriteBufferSize
        << ", useRadixSort = " << (useRadixSort ? "true" : "false");
    return out.str();
  }
};

std::vector<RowVectorPtr> takeRows(
    const std::vector<RowVectorPtr>& sources,
    const std::vector<std::vector<int32_t>>& indices) {
  std::vector<RowVectorPtr> result;

  for (size_t i = 0; i < sources.size(); ++i) {
    if (indices[i].empty()) {
      result.push_back(sources[i]);
      continue;
    }

    auto copy = RowVector::createEmpty(sources[0]->type(), sources[0]->pool());
    for (const auto row : indices[i]) {
      GLUTEN_CHECK(row < sources[i]->size(), fmt::format("Index out of bound: {}", row));
      copy->append(sources[i]->slice(row, 1).get());
    }
    result.push_back(std::move(copy));
  }

  return result;
}

RowVectorPtr mergeRowVectors(const std::vector<RowVectorPtr>& sources) {
  RowVectorPtr result = RowVector::createEmpty(sources[0]->type(), sources[0]->pool());

  for (const auto& source : sources) {
    result->append(source.get());
  }

  return result;
}

std::vector<ShuffleTestParams> getTestParams() {
  static std::vector<ShuffleTestParams> params{};

  if (!params.empty()) {
    return params;
  }

  const std::vector<arrow::Compression::type> compressions = {
      arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD};
  const std::vector<int32_t> compressionThresholds = {-1, 0, 3, 4, 10, 4096};
  const std::vector<int32_t> mergeBufferSizes = {0, 3, 4, 10, 4096};

  for (const auto& compression : compressions) {
    // Sort-based shuffle.
    for (const auto partitionWriterType : {PartitionWriterType::kLocal, PartitionWriterType::kRss}) {
      for (const auto diskWriteBufferSize : {4, 56, 32 * 1024}) {
        for (const bool useRadixSort : {true, false}) {
          params.push_back(ShuffleTestParams{
              .shuffleWriterType = ShuffleWriterType::kSortShuffle,
              .partitionWriterType = partitionWriterType,
              .compressionType = compression,
              .diskWriteBufferSize = diskWriteBufferSize,
              .useRadixSort = useRadixSort});
        }
      }
    }

    // Rss sort-based shuffle.
    params.push_back(ShuffleTestParams{
        .shuffleWriterType = ShuffleWriterType::kRssSortShuffle,
        .partitionWriterType = PartitionWriterType::kRss,
        .compressionType = compression});

    // Hash-based shuffle.
    for (const auto compressionThreshold : compressionThresholds) {
      // Local.
      for (const auto mergeBufferSize : mergeBufferSizes) {
        params.push_back(ShuffleTestParams{
            .shuffleWriterType = ShuffleWriterType::kHashShuffle,
            .partitionWriterType = PartitionWriterType::kLocal,
            .compressionType = compression,
            .compressionThreshold = compressionThreshold,
            .mergeBufferSize = mergeBufferSize});
      }

      // Rss.
      params.push_back(ShuffleTestParams{
          .shuffleWriterType = ShuffleWriterType::kHashShuffle,
          .partitionWriterType = PartitionWriterType::kRss,
          .compressionType = compression,
          .compressionThreshold = compressionThreshold});
    }
  }

  return params;
}
} // namespace

class VeloxShuffleWriterTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    VeloxShuffleWriterTestBase::setUpVeloxBackend();
  }

  void TearDown() override {
    VeloxShuffleWriterTestBase::tearDownVeloxBackend();
  }
};

class VeloxShuffleWriterTest : public ::testing::TestWithParam<ShuffleTestParams>, public VeloxShuffleWriterTestBase {
 public:
  arrow::Status initShuffleWriterOptions() {
    ShuffleTestParams params = GetParam();

    shuffleWriterOptions_.useRadixSort = params.useRadixSort;
    shuffleWriterOptions_.diskWriteBufferSize = params.diskWriteBufferSize;

    partitionWriterOptions_.compressionType = params.compressionType;
    partitionWriterOptions_.compressionThreshold = params.compressionThreshold;
    partitionWriterOptions_.mergeBufferSize = params.mergeBufferSize;

    return arrow::Status::OK();
  }

 protected:
  void SetUp() override {
    std::cout << "Running test with param: " << GetParam().toString() << std::endl;
    VeloxShuffleWriterTestBase::setUpTestData();
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  std::shared_ptr<arrow::Schema> getArrowSchema(const facebook::velox::RowVectorPtr& rowVector) const {
    return toArrowSchema(rowVector->type(), getDefaultMemoryManager()->getLeafMemoryPool().get());
  }

  void setReadableFile(const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
    GLUTEN_ASSIGN_OR_THROW(file_, arrow::io::ReadableFile::Open(fileName))
  }

  void getRowVectors(
      arrow::Compression::type compressionType,
      const RowTypePtr& rowType,
      std::vector<facebook::velox::RowVectorPtr>& vectors,
      std::shared_ptr<arrow::io::InputStream> in) {
    const auto veloxCompressionType = arrowCompressionTypeToVelox(compressionType);
    const auto schema = toArrowSchema(rowType, getDefaultMemoryManager()->getLeafMemoryPool().get());

    auto codec = createArrowIpcCodec(compressionType, CodecBackend::NONE);

    // Set batchSize to a large value to make all batches are merged by reader.
    auto deserializerFactory = std::make_unique<gluten::VeloxShuffleReaderDeserializerFactory>(
        schema,
        std::move(codec),
        veloxCompressionType,
        rowType,
        kDefaultBatchSize,
        kDefaultReadBufferSize,
        kDefaultDeserializerBufferSize,
        getDefaultMemoryManager()->getArrowMemoryPool(),
        pool_,
        GetParam().shuffleWriterType);

    const auto reader = std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));

    const auto iter = reader->readStream(in);
    while (iter->hasNext()) {
      auto vector = std::dynamic_pointer_cast<VeloxColumnarBatch>(iter->next())->getRowVector();
      vectors.emplace_back(vector);
    }
  }

  void shuffleWriteReadMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      int32_t expectPartitionLength,
      const std::vector<std::vector<facebook::velox::RowVectorPtr>>& expectedVectors) {
    ASSERT_NOT_OK(shuffleWriter.stop());

    checkFileExists(dataFile_);

    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), expectPartitionLength);

    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0);

    setReadableFile(dataFile_);
    ASSERT_EQ(*file_->GetSize(), lengthSum);
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      if (expectedVectors[i].size() == 0) {
        ASSERT_EQ(lengths[i], 0);
      } else {
        std::vector<facebook::velox::RowVectorPtr> deserializedVectors;
        GLUTEN_ASSIGN_OR_THROW(
            auto in, arrow::io::RandomAccessFile::GetStream(file_, i == 0 ? 0 : lengths[i - 1], lengths[i]));
        getRowVectors(
            partitionWriterOptions_.compressionType, asRowType(expectedVectors[i][0]->type()), deserializedVectors, in);

        const auto expectedVector = mergeRowVectors(expectedVectors[i]);
        const auto deserializedVector = mergeRowVectors(deserializedVectors);
        facebook::velox::test::assertEqualVectors(expectedVector, deserializedVector);
      }
    }
  }

  void testShuffleRoundTrip(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<std::shared_ptr<ColumnarBatch>> batches,
      int32_t expectPartitionLength,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) {
    for (const auto& batch : batches) {
      ASSERT_NOT_OK(shuffleWriter.write(batch, ShuffleWriter::kMinMemLimit));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, expectedVectors);
  }

  void testShuffleRoundTrip(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<RowVectorPtr> inputs,
      int32_t expectPartitionLength,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) {
    std::vector<std::shared_ptr<ColumnarBatch>> batches;
    for (const auto& input : inputs) {
      batches.emplace_back(std::make_shared<VeloxColumnarBatch>(input));
    }
    testShuffleRoundTrip(shuffleWriter, batches, expectPartitionLength, expectedVectors);
  }

  inline static TestAllocationListener* listener_{nullptr};

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

class SinglePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t) override {
    auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();
    auto veloxPool = getDefaultMemoryManager()->getLeafMemoryPool();

    shuffleWriterOptions_.partitioning = Partitioning::kSingle;
    shuffleWriterOptions_.bufferSize = 10;

    auto partitionWriter =
        createPartitionWriter(GetParam().partitionWriterType, 1, dataFile_, localDirs_, partitionWriterOptions_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            GetParam().shuffleWriterType, 1, std::move(partitionWriter), shuffleWriterOptions_, veloxPool, arrowPool));

    return shuffleWriter;
  }
};

class HashPartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void SetUp() override {
    VeloxShuffleWriterTest::SetUp();

    children1_.insert((children1_.begin()), makeFlatVector<int32_t>({1, 2, 2, 2, 2, 1, 1, 1, 2, 1}));
    hashInputVector1_ = makeRowVector(children1_);
    children2_.insert((children2_.begin()), makeFlatVector<int32_t>({2, 2}));
    hashInputVector2_ = makeRowVector(children2_);
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) override {
    auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();
    auto veloxPool = getDefaultMemoryManager()->getLeafMemoryPool();

    shuffleWriterOptions_.partitioning = Partitioning::kHash;
    shuffleWriterOptions_.bufferSize = 4;

    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, numPartitions, dataFile_, localDirs_, partitionWriterOptions_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            GetParam().shuffleWriterType,
            numPartitions,
            std::move(partitionWriter),
            shuffleWriterOptions_,
            veloxPool,
            arrowPool));

    return shuffleWriter;
  }

  std::vector<uint32_t> hashPartitionIds_{1, 2};

  facebook::velox::RowVectorPtr hashInputVector1_;
  facebook::velox::RowVectorPtr hashInputVector2_;
};

class RangePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void SetUp() override {
    VeloxShuffleWriterTest::SetUp();

    auto pid1 = makeRowVector({makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})});
    auto rangeVector1 = makeRowVector(inputVector1_->children());
    compositeBatch1_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid1), std::make_shared<VeloxColumnarBatch>(rangeVector1)});

    auto pid2 = makeRowVector({makeFlatVector<int32_t>({0, 1})});
    auto rangeVector2 = makeRowVector(inputVector2_->children());
    compositeBatch2_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid2), std::make_shared<VeloxColumnarBatch>(rangeVector2)});
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) override {
    auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();
    auto veloxPool = getDefaultMemoryManager()->getLeafMemoryPool();

    shuffleWriterOptions_.partitioning = Partitioning::kRange;
    shuffleWriterOptions_.bufferSize = 4;

    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, numPartitions, dataFile_, localDirs_, partitionWriterOptions_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            GetParam().shuffleWriterType,
            numPartitions,
            std::move(partitionWriter),
            shuffleWriterOptions_,
            veloxPool,
            arrowPool));

    return shuffleWriter;
  }

  std::shared_ptr<ColumnarBatch> compositeBatch1_;
  std::shared_ptr<ColumnarBatch> compositeBatch2_;
};

class RoundRobinPartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) override {
    auto* arrowPool = getDefaultMemoryManager()->getArrowMemoryPool();
    auto veloxPool = getDefaultMemoryManager()->getLeafMemoryPool();

    shuffleWriterOptions_.partitioning = Partitioning::kRoundRobin;
    shuffleWriterOptions_.bufferSize = 4096;

    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, numPartitions, dataFile_, localDirs_, partitionWriterOptions_);

    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            GetParam().shuffleWriterType,
            numPartitions,
            std::move(partitionWriter),
            shuffleWriterOptions_,
            veloxPool,
            arrowPool));

    return shuffleWriter;
  }
};

TEST_P(SinglePartitioningShuffleWriter, single) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }

  ASSERT_NOT_OK(initShuffleWriterOptions());

  // Split 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    testShuffleRoundTrip(*shuffleWriter, {inputVector1_}, 1, {{inputVector1_}});
  }
  // Split > 1 RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    testShuffleRoundTrip(
        *shuffleWriter,
        {inputVector1_, inputVector2_, inputVector1_},
        1,
        {{inputVector1_, inputVector2_, inputVector1_}});
  }
  // Split null RowVector.
  {
    auto shuffleWriter = createShuffleWriter(1);
    auto vector = makeRowVector({
        makeNullableFlatVector<int32_t>({std::nullopt}),
        makeNullableFlatVector<StringView>({std::nullopt}),
    });
    testShuffleRoundTrip(*shuffleWriter, {vector}, 1, {{vector}});
  }
  // Other types.
  {
    auto shuffleWriter = createShuffleWriter(1);
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
            makeNullableFlatVector<StringView>({std::nullopt, "de"}),
        }),
        makeNullableFlatVector<StringView>({std::nullopt, "10 I'm not inline string"}),
        makeArrayVector<int64_t>({
            {1, 2, 3, 4, 5},
            {1, 2, 3},
        }),
        makeMapVector<int32_t, StringView>({{{1, "str1000"}, {2, "str2000"}}, {{3, "str3000"}, {4, "str4000"}}}),
    });
    testShuffleRoundTrip(*shuffleWriter, {vector}, 1, {{vector}});
  }
}

TEST_P(HashPartitioningShuffleWriter, hashPart1Vector) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);
  auto vector = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1, 2}),
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<StringView>({"nn", "re", "fr", "juiu"}),
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

  auto rowType = facebook::velox::asRowType(vector->type());
  auto children = rowType->children();
  auto names = rowType->names();
  children.erase(children.begin());
  names.erase(names.begin());
  auto dataType = facebook::velox::ROW(std::move(names), std::move(children));

  auto firstBlock = makeRowVector({
      makeNullableFlatVector<int8_t>({2, std::nullopt}),
      makeFlatVector<int64_t>({2, 4}),
      makeFlatVector<StringView>({"re", "juiu"}),
      makeFlatVector<int64_t>({34567235, 4567}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({34567235, 4567}, DECIMAL(20, 4)),
      makeFlatVector<int32_t>({1, 1}, DATE()),
      makeFlatVector<Timestamp>({Timestamp(1, 0), Timestamp(1, 0)}),
  });

  auto secondBlock = makeRowVector({
      makeNullableFlatVector<int8_t>({1, 3}),
      makeFlatVector<int64_t>({1, 3}),
      makeFlatVector<StringView>({"nn", "fr"}),
      makeFlatVector<int64_t>({232, 1212}, DECIMAL(12, 4)),
      makeFlatVector<int128_t>({232, 1212}, DECIMAL(20, 4)),
      makeNullableFlatVector<int32_t>({std::nullopt, 0}, DATE()),
      makeNullableFlatVector<Timestamp>({std::nullopt, Timestamp(0, 0)}),
  });

  testShuffleRoundTrip(*shuffleWriter, {vector}, 2, {{firstBlock}, {secondBlock}});
}

TEST_P(HashPartitioningShuffleWriter, hashPart1VectorComplexType) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);
  auto children = childrenComplex_;
  children.insert((children.begin()), makeFlatVector<int32_t>({1, 2}));
  auto vector = makeRowVector(children);
  auto firstBlock = takeRows({inputVectorComplex_}, {{1}});
  auto secondBlock = takeRows({inputVectorComplex_}, {{0}});

  testShuffleRoundTrip(*shuffleWriter, {vector}, 2, {firstBlock, secondBlock});
}

TEST_P(HashPartitioningShuffleWriter, hashPart3Vectors) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {0, 1}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_, inputVector1_}, {{0, 5, 6, 7, 9}, {0, 5, 6, 7, 9}});

  testShuffleRoundTrip(
      *shuffleWriter, {hashInputVector1_, hashInputVector2_, hashInputVector1_}, 2, {blockPid2, blockPid1});
}

TEST_P(HashPartitioningShuffleWriter, hashLargeVectors) {
  const int32_t expectedMaxBatchSize = 8;
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

  // calculate maxBatchSize_
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, hashInputVector1_));
  if (GetParam().shuffleWriterType == ShuffleWriterType::kHashShuffle) {
    VELOX_CHECK_EQ(shuffleWriter->maxBatchSize(), expectedMaxBatchSize);
  }

  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 2, 3, 4, 8}, {}, {1, 2, 3, 4, 8}});
  auto blockPid1 = takeRows({inputVector1_, inputVector1_}, {{0, 5, 6, 7, 9}, {0, 5, 6, 7, 9}});

  VELOX_CHECK(hashInputVector1_->size() > expectedMaxBatchSize);
  testShuffleRoundTrip(*shuffleWriter, {hashInputVector2_, hashInputVector1_}, 2, {blockPid2, blockPid1});
}

TEST_P(RangePartitioningShuffleWriter, range) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleRoundTrip(
      *shuffleWriter, {compositeBatch1_, compositeBatch2_, compositeBatch1_}, 2, {blockPid1, blockPid2});
}

TEST_P(RoundRobinPartitioningShuffleWriter, roundRobin) {
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

  auto blockPid1 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{0, 2, 4, 6, 8}, {0}, {0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_, inputVector2_, inputVector1_}, {{1, 3, 5, 7, 9}, {1}, {1, 3, 5, 7, 9}});

  testShuffleRoundTrip(*shuffleWriter, {inputVector1_, inputVector2_, inputVector1_}, 2, {blockPid1, blockPid2});
}

TEST_P(RoundRobinPartitioningShuffleWriter, preAllocForceRealloc) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferReallocThreshold = 0; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter(2);

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
      makeNullableFlatVector<StringView>({"", "alice"}),
      makeNullableFlatVector<StringView>({"alice", ""}),
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
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }
  ASSERT_NOT_OK(initShuffleWriterOptions());
  shuffleWriterOptions_.bufferReallocThreshold = 1; // Force re-alloc on buffer size changed.
  auto shuffleWriter = createShuffleWriter(2);

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
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
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
  if (GetParam().shuffleWriterType != ShuffleWriterType::kHashShuffle) {
    return;
  }

  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

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

  auto blockPid1 =
      takeRows({inputVector1_, inputVector1_, inputVector1_}, {{0, 2, 4, 6, 8}, {0, 2, 4, 6, 8}, {0, 2, 4, 6, 8}});
  auto blockPid2 =
      takeRows({inputVector1_, inputVector1_, inputVector1_}, {{1, 3, 5, 7, 9}, {1, 3, 5, 7, 9}, {1, 3, 5, 7, 9}});

  // Stop and verify.
  shuffleWriteReadMultiBlocks(*shuffleWriter, 2, {blockPid1, blockPid2});
}

TEST_P(RoundRobinPartitioningShuffleWriter, sortMaxRows) {
  if (GetParam().shuffleWriterType != ShuffleWriterType::kSortShuffle) {
    return;
  }
  ASSERT_NOT_OK(initShuffleWriterOptions());
  auto shuffleWriter = createShuffleWriter(2);

  // Set memLimit to 0 to force allocate a new buffer for each row.
  ASSERT_NOT_OK(splitRowVector(*shuffleWriter, inputVector1_, 0));

  auto blockPid1 = takeRows({inputVector1_}, {{0, 2, 4, 6, 8}});
  auto blockPid2 = takeRows({inputVector1_}, {{1, 3, 5, 7, 9}});
  shuffleWriteReadMultiBlocks(*shuffleWriter, 2, {blockPid1, blockPid2});
}

INSTANTIATE_TEST_SUITE_P(
    SinglePartitioningShuffleWriterGroup,
    SinglePartitioningShuffleWriter,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    RoundRobinPartitioningShuffleWriterGroup,
    RoundRobinPartitioningShuffleWriter,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    HashPartitioningShuffleWriterGroup,
    HashPartitioningShuffleWriter,
    ::testing::ValuesIn(getTestParams()));

INSTANTIATE_TEST_SUITE_P(
    RangePartitioningShuffleWriterGroup,
    RangePartitioningShuffleWriter,
    ::testing::ValuesIn(getTestParams()));

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new gluten::VeloxShuffleWriterTestEnvironment);
  return RUN_ALL_TESTS();
}
