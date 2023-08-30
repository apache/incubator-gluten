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
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <iostream>

#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace arrow;
using namespace arrow::ipc;

namespace gluten {
struct ShuffleTestParams {
  bool prefer_evict;
  arrow::Compression::type compression_type;
  CompressionMode compression_mode;

  std::string toString() const {
    std::ostringstream out;
    out << "prefer_evict = " << prefer_evict << " ; compression_type = " << compression_type;
    return out.str();
  }
};

class VeloxShuffleWriterTest : public ::testing::TestWithParam<ShuffleTestParams>, public velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    const std::string tmpDirPrefix = "columnar-shuffle-test";
    ARROW_ASSIGN_OR_THROW(tmpDir1_, arrow::internal::TemporaryDir::Make(tmpDirPrefix))
    ARROW_ASSIGN_OR_THROW(tmpDir2_, arrow::internal::TemporaryDir::Make(tmpDirPrefix))
    auto configDirs = tmpDir1_->path().ToString() + "," + tmpDir2_->path().ToString();

    setenv(kGlutenSparkLocalDirs.c_str(), configDirs.c_str(), 1);

    shuffleWriterOptions_ = ShuffleWriterOptions::defaults();
    shuffleWriterOptions_.buffer_compress_threshold = 0;
    shuffleWriterOptions_.memory_pool = arrowPool_;
    shuffleWriterOptions_.ipc_memory_pool = shuffleWriterOptions_.memory_pool;

    ShuffleTestParams params = GetParam();
    shuffleWriterOptions_.prefer_evict = params.prefer_evict;
    shuffleWriterOptions_.compression_type = params.compression_type;
    shuffleWriterOptions_.compression_mode = params.compression_mode;

    partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>(shuffleWriterOptions_.prefer_evict);
    std::vector<VectorPtr> children1 = {
        makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}),
        makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2, 2, std::nullopt, std::nullopt, 3, -3}),
        makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 5, 6, 7, 8, std::nullopt}),
        makeNullableFlatVector<int64_t>(
            {std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt}),
        makeNullableFlatVector<float>(
            {-0.1234567,
             std::nullopt,
             0.1234567,
             std::nullopt,
             -0.142857,
             std::nullopt,
             0.142857,
             0.285714,
             0.428617,
             std::nullopt}),
        makeNullableFlatVector<bool>(
            {std::nullopt, true, false, std::nullopt, true, true, false, true, std::nullopt, std::nullopt}),
        makeFlatVector<velox::StringView>(
            {"alice0", "bob1", "alice2", "bob3", "Alice4", "Bob5", "AlicE6", "boB7", "ALICE8", "BOB9"}),
        makeNullableFlatVector<velox::StringView>(
            {"alice", "bob", std::nullopt, std::nullopt, "Alice", "Bob", std::nullopt, "alicE", std::nullopt, "boB"}),
    };
    inputVector1_ = makeRowVector(children1);
    children1.insert((children1.begin()), makeFlatVector<int32_t>({1, 2, 2, 2, 2, 1, 1, 1, 2, 1}));
    hashInputVector1_ = makeRowVector(children1);

    std::vector<VectorPtr> children2 = {
        makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
        makeFlatVector<int8_t>({1, -1}),
        makeNullableFlatVector<int32_t>({100, std::nullopt}),
        makeFlatVector<int64_t>({1, 1}),
        makeFlatVector<float>({0.142857, -0.142857}),
        makeFlatVector<bool>({true, false}),
        makeFlatVector<velox::StringView>(
            {"bob",
             "alicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealice"}),
        makeNullableFlatVector<velox::StringView>({std::nullopt, std::nullopt}),
    };
    inputVector2_ = makeRowVector(children2);
    children2.insert((children2.begin()), makeFlatVector<int32_t>({2, 2}));
    hashInputVector2_ = makeRowVector(children2);
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  void splitRowVector(VeloxShuffleWriter& shuffleWriter, velox::RowVectorPtr vector) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
    GLUTEN_THROW_NOT_OK(shuffleWriter.split(cb));
  }

  RowVectorPtr takeRows(const RowVectorPtr& source, const std::vector<int32_t>& idxs) const {
    RowVectorPtr copy = RowVector::createEmpty(source->type(), source->pool());
    for (int32_t idx : idxs) {
      copy->append(source->slice(idx, 1).get());
    }
    return copy;
  }

  std::shared_ptr<arrow::Schema> getArrowSchema(velox::RowVectorPtr& rowVector) {
    return toArrowSchema(rowVector->type(), pool());
  }

  void setReadableFile(const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
    GLUTEN_ASSIGN_OR_THROW(file_, arrow::io::ReadableFile::Open(fileName))
  }

  void getRowVectors(std::shared_ptr<arrow::Schema> schema, std::vector<velox::RowVectorPtr>& vectors) {
    ReaderOptions options;
    options.compression_type = shuffleWriterOptions_.compression_type;
    options.compression_mode = shuffleWriterOptions_.compression_mode;
    auto reader = std::make_shared<VeloxShuffleReader>(schema, options, arrowPool_, pool_);
    auto iter = reader->readStream(file_);
    while (iter->hasNext()) {
      auto vector = std::dynamic_pointer_cast<VeloxColumnarBatch>(iter->next())->getRowVector();
      vectors.emplace_back(vector);
    }
  }

  // 1 partitionLength
  void testShuffleWrite(VeloxShuffleWriter& shuffleWriter, std::vector<velox::RowVectorPtr> vectors) {
    for (auto& vector : vectors) {
      splitRowVector(shuffleWriter, vector);
    }
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(shuffleWriter.dataFile());
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), 1);

    auto schema = getArrowSchema(vectors[0]);
    std::vector<velox::RowVectorPtr> deserializedVectors;
    setReadableFile(shuffleWriter.dataFile());
    getRowVectors(schema, deserializedVectors);

    ASSERT_EQ(deserializedVectors.size(), vectors.size());
    for (int32_t i = 0; i < deserializedVectors.size(); i++) {
      velox::test::assertEqualVectors(vectors[i], deserializedVectors[i]);
    }
  }

  void shuffleWriteReadMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      int32_t expectPartitionLength,
      TypePtr dataType,
      std::vector<std::vector<velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(shuffleWriter.dataFile());
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), expectPartitionLength);
    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0);
    auto schema = getArrowSchema(expectedVectors[0][0]);
    setReadableFile(shuffleWriter.dataFile());
    ASSERT_EQ(*file_->GetSize(), lengthSum);
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      std::vector<velox::RowVectorPtr> deserializedVectors;
      getRowVectors(schema, deserializedVectors);
      if (i != 0) {
        ASSERT_NOT_OK(file_->Advance(lengths[i - 1]));
      }
      ASSERT_EQ(expectedVectors[i].size(), deserializedVectors.size());
      for (int32_t j = 0; j < expectedVectors[i].size(); j++) {
        velox::test::assertEqualVectors(expectedVectors[i][j], deserializedVectors[j]);
      }
    }
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<std::shared_ptr<ColumnarBatch>> batches,
      int32_t expectPartitionLength,
      TypePtr dataType,
      std::vector<std::vector<velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    for (auto& batch : batches) {
      GLUTEN_THROW_NOT_OK(shuffleWriter.split(batch));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<velox::RowVectorPtr> vectors,
      int32_t expectPartitionLength,
      TypePtr dataType,
      std::vector<std::vector<velox::RowVectorPtr>> expectedVectors) {
    for (auto& vector : vectors) {
      splitRowVector(shuffleWriter, vector);
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }

  std::shared_ptr<arrow::internal::TemporaryDir> tmpDir1_;
  std::shared_ptr<arrow::internal::TemporaryDir> tmpDir2_;

  ShuffleWriterOptions shuffleWriterOptions_;
  std::shared_ptr<VeloxShuffleWriter> shuffleWriter_;

  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator_;

  std::shared_ptr<arrow::io::ReadableFile> file_;

  velox::RowVectorPtr inputVector1_;
  velox::RowVectorPtr inputVector2_;
  velox::RowVectorPtr hashInputVector1_;
  velox::RowVectorPtr hashInputVector2_;

  std::shared_ptr<arrow::MemoryPool> arrowPool_ = defaultArrowMemoryPool();
};

arrow::Status splitRowVectorStatus(VeloxShuffleWriter& shuffleWriter, velox::RowVectorPtr vector) {
  std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
  return shuffleWriter.split(cb);
}

TEST_P(VeloxShuffleWriterTest, singlePart1Vector) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  testShuffleWrite(*shuffleWriter, {inputVector1_});
}

TEST_P(VeloxShuffleWriterTest, singlePart3Vectors) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector2_, inputVector1_});
}

TEST_P(VeloxShuffleWriterTest, singlePartCompressSmallBuffer) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";
  shuffleWriterOptions_.compression_type = arrow::Compression::LZ4_FRAME;
  shuffleWriterOptions_.buffer_compress_threshold = 1024;

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector1_});
}

TEST_P(VeloxShuffleWriterTest, singlePartNullVector) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  auto vector = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt}),
      makeNullableFlatVector<velox::StringView>({std::nullopt}),
  });
  testShuffleWrite(*shuffleWriter, {vector});
}

TEST_P(VeloxShuffleWriterTest, singlePartOtherType) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  auto vector = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt, 1}),
      makeNullableFlatVector<StringView>({std::nullopt, "10"}),
      makeShortDecimalFlatVector({232, 34567235}, DECIMAL(12, 4)),
      makeLongDecimalFlatVector({232, 34567235}, DECIMAL(20, 4)),
      makeFlatVector<Date>(
          2, [](vector_size_t row) { return Date{row % 2}; }, nullEvery(5)),
  });
  testShuffleWrite(*shuffleWriter, {vector});
}

TEST_P(VeloxShuffleWriterTest, singlePartComplexType) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  auto vector = makeRowVector({
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

TEST_P(VeloxShuffleWriterTest, hashPart1Vector) {
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))
  auto vector = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1, 2}),
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<velox::StringView>({"nn", "re", "fr", "juiu"}),
      makeShortDecimalFlatVector({232, 34567235, 1212, 4567}, DECIMAL(12, 4)),
      makeLongDecimalFlatVector({232, 34567235, 1212, 4567}, DECIMAL(20, 4)),
      makeFlatVector<Date>(
          4, [](vector_size_t row) { return Date{row % 2}; }, nullEvery(5)),
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
      makeShortDecimalFlatVector({232, 34567235, 1212, 4567}, DECIMAL(12, 4)),
      makeLongDecimalFlatVector({232, 34567235, 1212, 4567}, DECIMAL(20, 4)),
      makeFlatVector<Date>(
          4, [](vector_size_t row) { return Date{row % 2}; }, nullEvery(5)),
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
      makeShortDecimalFlatVector({34567235, 4567}, DECIMAL(12, 4)),
      makeLongDecimalFlatVector({34567235, 4567}, DECIMAL(20, 4)),
      makeFlatVector<Date>({Date(1), Date(1)}),
      makeFlatVector<Timestamp>({Timestamp(1, 0), Timestamp(1, 0)}),
  });

  auto secondBlock = makeRowVector({
      makeNullableFlatVector<int8_t>({1, 3}),
      makeFlatVector<int64_t>({1, 3}),
      makeFlatVector<velox::StringView>({"nn", "fr"}),
      makeShortDecimalFlatVector({232, 1212}, DECIMAL(12, 4)),
      makeLongDecimalFlatVector({232, 1212}, DECIMAL(20, 4)),
      makeNullableFlatVector<Date>({std::nullopt, Date(0)}),
      makeNullableFlatVector<Timestamp>({std::nullopt, Timestamp(0, 0)}),
  });

  testShuffleWriteMultiBlocks(*shuffleWriter_, {vector}, 2, dataVector->type(), {{firstBlock}, {secondBlock}});
}

TEST_P(VeloxShuffleWriterTest, hashPart1VectorComplexType) {
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))
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

  testShuffleWriteMultiBlocks(*shuffleWriter_, {vector}, 2, dataVector->type(), {{firstBlock}, {secondBlock}});
}

TEST_P(VeloxShuffleWriterTest, hashPart3Vectors) {
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  auto block1Pid1 = takeRows(inputVector1_, {0, 5, 6, 7, 9});
  auto block2Pid1 = takeRows(inputVector2_, {});

  auto block1Pid2 = takeRows(inputVector1_, {1, 2, 3, 4, 8});
  auto block2Pid2 = takeRows(inputVector2_, {0, 1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter_,
      {hashInputVector1_, hashInputVector2_, hashInputVector1_},
      2,
      inputVector1_->type(),
      {{block1Pid2, block2Pid2, block1Pid2}, {block1Pid1, block1Pid1}});
}

TEST_P(VeloxShuffleWriterTest, roundRobin) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block2Pid1 = takeRows(inputVector2_, {0});

  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});
  auto block2Pid2 = takeRows(inputVector2_, {1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter_,
      {inputVector1_, inputVector2_, inputVector1_},
      2,
      inputVector1_->type(),
      {{block1Pid1, block2Pid1, block1Pid1}, {block1Pid2, block2Pid2, block1Pid2}});
}

TEST_P(VeloxShuffleWriterTest, rangePartition) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "range";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_))

  auto pid1 = makeRowVector({makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})});
  auto rangeVector1 = makeRowVector(inputVector1_->children());
  auto compositeBatch1 = CompositeColumnarBatch::create(
      {std::make_shared<VeloxColumnarBatch>(pid1), std::make_shared<VeloxColumnarBatch>(rangeVector1)});

  auto pid2 = makeRowVector({makeFlatVector<int32_t>({0, 1})});
  auto rangeVector2 = makeRowVector(inputVector2_->children());
  auto compositeBatch2 = CompositeColumnarBatch::create(
      {std::make_shared<VeloxColumnarBatch>(pid2), std::make_shared<VeloxColumnarBatch>(rangeVector2)});

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block2Pid1 = takeRows(inputVector2_, {0});

  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});
  auto block2Pid2 = takeRows(inputVector2_, {1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter_,
      {compositeBatch1, compositeBatch2, compositeBatch1},
      2,
      inputVector1_->type(),
      {{block1Pid1, block2Pid1, block1Pid1}, {block1Pid2, block2Pid2, block1Pid2}});
}

TEST_P(VeloxShuffleWriterTest, memoryLeak) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>();

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.partitioning_name = "rr";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  splitRowVector(*shuffleWriter_, inputVector1_);
  splitRowVector(*shuffleWriter_, inputVector2_);
  splitRowVector(*shuffleWriter_, inputVector1_);

  ASSERT_NOT_OK(shuffleWriter_->stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  shuffleWriter_.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_P(VeloxShuffleWriterTest, spillFailWithOutOfMemory) {
  auto pool = std::make_shared<MyMemoryPool>(0);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  auto status = splitRowVectorStatus(*shuffleWriter_, inputVector1_);

  // should return OOM status because there's no partition buffer to spill
  ASSERT_TRUE(status.IsOutOfMemory());
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, TestSpillLargestPartition) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(9 * 1024 * 1024);
  //  pool = std::make_shared<arrow::LoggingMemoryPool>(pool.get());

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  // shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.compression_type = arrow::Compression::UNCOMPRESSED;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, TestStopShrinkAndSpill) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(9 * 1024 * 1024);
  //  pool = std::make_shared<arrow::LoggingMemoryPool>(pool.get());

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  // shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.compression_type = arrow::Compression::UNCOMPRESSED;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }

  auto bufferSize = shuffleWriter_->pool()->bytes_allocated();
  auto payloadSize = shuffleWriter_->totalCachedPayloadSize();

  int64_t evicted;
  shuffleWriter_->setSplitState(SplitState::STOP);
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
  ASSERT_GE(evicted, payloadSize);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    VeloxShuffleWriterTest,
    ::testing::Values(
        ShuffleTestParams{true, arrow::Compression::UNCOMPRESSED, CompressionMode::BUFFER},
        ShuffleTestParams{true, arrow::Compression::LZ4_FRAME, CompressionMode::BUFFER},
        ShuffleTestParams{true, arrow::Compression::ZSTD, CompressionMode::BUFFER},
        ShuffleTestParams{false, arrow::Compression::UNCOMPRESSED, CompressionMode::BUFFER},
        ShuffleTestParams{false, arrow::Compression::LZ4_FRAME, CompressionMode::BUFFER},
        ShuffleTestParams{false, arrow::Compression::ZSTD, CompressionMode::BUFFER},
        ShuffleTestParams{true, arrow::Compression::UNCOMPRESSED, CompressionMode::ROWVECTOR},
        ShuffleTestParams{false, arrow::Compression::LZ4_FRAME, CompressionMode::ROWVECTOR}));

} // namespace gluten
