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

#include "shuffle/VeloxShuffleWriter.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <iostream>

#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleReader.h"

using namespace facebook;
using namespace facebook::velox;
using namespace arrow;
using namespace arrow::ipc;

namespace gluten {

class VeloxShuffleWriterTest : public ::testing::Test, public velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    const std::string tmpDirPrefix = "columnar-shuffle-test";
    ARROW_ASSIGN_OR_THROW(tmpDir1_, arrow::internal::TemporaryDir::Make(tmpDirPrefix))
    ARROW_ASSIGN_OR_THROW(tmpDir2_, arrow::internal::TemporaryDir::Make(tmpDirPrefix))
    auto configDirs = tmpDir1_->path().ToString() + "," + tmpDir2_->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", configDirs.c_str(), 1);

    shuffleWriterOptions_ = ShuffleWriterOptions::defaults();

    partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>();
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

  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> getRecordBatchStreamReader(
      const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      RETURN_NOT_OK(file_->Close());
    }
    ARROW_ASSIGN_OR_RAISE(file_, arrow::io::ReadableFile::Open(fileName))
    ARROW_ASSIGN_OR_RAISE(auto fileReader, arrow::ipc::RecordBatchStreamReader::Open(file_))
    return fileReader;
  }

  void splitRowVector(VeloxShuffleWriter& shuffle_writer, velox::RowVectorPtr vector) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
    GLUTEN_THROW_NOT_OK(shuffle_writer.split(cb.get()));
  }

  RowVectorPtr takeRows(const RowVectorPtr& source, const std::vector<int32_t>& idxs) const {
    RowVectorPtr copy = RowVector::createEmpty(source->type(), source->pool());
    for (int32_t idx : idxs) {
      copy->append(source->slice(idx, 1).get());
    }
    return copy;
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

    std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
    ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter.dataFile()));
    auto expectedSchema = shuffleWriter.writeSchema();
    // verify schema
    ASSERT_TRUE(fileReader->schema()->Equals(expectedSchema, true));
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    ASSERT_NOT_OK(fileReader->ReadAll(&batches));
    ASSERT_EQ(batches.size(), vectors.size());
    for (int32_t i = 0; i < batches.size(); i++) {
      auto deserialized = VeloxShuffleReader::readRowVector(*batches[i], asRowType(vectors[i]->type()), pool_.get());
      velox::test::assertEqualVectors(deserialized, vectors[i]);
    }
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<velox::RowVectorPtr> vectors,
      int32_t expectPartitionLength,
      TypePtr dataType,
      std::vector<std::vector<velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    for (auto& vector : vectors) {
      splitRowVector(shuffleWriter, vector);
    }
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(shuffleWriter.dataFile());
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    auto expectedSchema = shuffleWriter.writeSchema();
    ASSERT_EQ(lengths.size(), expectPartitionLength);
    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0);

    // verify schema
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      GLUTEN_ASSIGN_OR_THROW(auto fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
      if (i == 0) {
        ASSERT_EQ(*file_->GetSize(), lengthSum);
      }
      ASSERT_TRUE(fileReader->schema()->Equals(expectedSchema, true));
      if (i != 0) {
        ASSERT_NOT_OK(file_->Advance(lengths[i - 1]));
      }
      ASSERT_NOT_OK(fileReader->ReadAll(&batches));
      // ASSERT_EQ(batches.size(), vectors.size()); //input batch may be empty
      ASSERT_EQ(expectedVectors[i].size(), batches.size());
      auto partitionVectors = expectedVectors[i];
      ASSERT_EQ(partitionVectors.size(), batches.size());
      for (int32_t i = 0; i < batches.size(); i++) {
        auto deserialized = VeloxShuffleReader::readRowVector(*batches[i], asRowType(dataType), pool_.get());
        velox::test::assertEqualVectors(partitionVectors[i], deserialized);
      }
    }
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

  std::shared_ptr<velox::memory::MemoryPool> pool_ = getDefaultVeloxLeafMemoryPool();
};

arrow::Status splitRowVectorStatus(VeloxShuffleWriter& shuffleWriter, velox::RowVectorPtr vector) {
  std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
  return shuffleWriter.split(cb.get());
}

TEST_F(VeloxShuffleWriterTest, singlePart1Vector) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

  testShuffleWrite(*shuffleWriter, {inputVector1_});
}

TEST_F(VeloxShuffleWriterTest, singlePart3Vectors) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

  testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector2_, inputVector1_});
}

TEST_F(VeloxShuffleWriterTest, singlePartCompress) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";
  shuffleWriterOptions_.compression_type = arrow::Compression::LZ4_FRAME;
  shuffleWriterOptions_.batch_compress_threshold = 1;

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

  testShuffleWrite(*shuffleWriter, {inputVector1_, inputVector1_});
}

TEST_F(VeloxShuffleWriterTest, singlePartNullVector) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

  auto vector = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt}),
      makeNullableFlatVector<velox::StringView>({std::nullopt}),
  });
  testShuffleWrite(*shuffleWriter, {vector});
}

TEST_F(VeloxShuffleWriterTest, singlePartOtherType) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

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

TEST_F(VeloxShuffleWriterTest, hashPart1Vector) {
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(shuffleWriter_, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_))
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

TEST_F(VeloxShuffleWriterTest, hashPart3Vectors) {
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(shuffleWriter_, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_))

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

TEST_F(VeloxShuffleWriterTest, roundRobin) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

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

TEST_F(VeloxShuffleWriterTest, rangePartition) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "range";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_))

  auto children1 = inputVector1_->children();
  std::vector<VectorPtr> rangeVectorChildren1 = {makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})};
  rangeVectorChildren1.insert(rangeVectorChildren1.end(), children1.begin(), children1.end());
  auto rangeVector1 = makeRowVector(rangeVectorChildren1);

  auto children2 = inputVector2_->children();
  std::vector<VectorPtr> rangeVectorChildren2 = {makeFlatVector<int32_t>({0, 1})};
  rangeVectorChildren2.insert(rangeVectorChildren2.end(), children2.begin(), children2.end());
  auto rangeVector2 = makeRowVector(rangeVectorChildren2);

  auto block1Pid1 = takeRows(inputVector1_, {0, 2, 4, 6, 8});
  auto block2Pid1 = takeRows(inputVector2_, {0});

  auto block1Pid2 = takeRows(inputVector1_, {1, 3, 5, 7, 9});
  auto block2Pid2 = takeRows(inputVector2_, {1});

  testShuffleWriteMultiBlocks(
      *shuffleWriter_,
      {rangeVector1, rangeVector2, rangeVector1},
      2,
      inputVector1_->type(),
      {{block1Pid1, block2Pid1, block1Pid1}, {block1Pid2, block2Pid2, block1Pid2}});
}

TEST_F(VeloxShuffleWriterTest, memoryLeak) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(17 * 1024 * 1024);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.write_schema = false;
  shuffleWriterOptions_.partitioning_name = "rr";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  splitRowVector(*shuffleWriter_, inputVector1_);
  splitRowVector(*shuffleWriter_, inputVector2_);
  splitRowVector(*shuffleWriter_, inputVector1_);

  ASSERT_NOT_OK(shuffleWriter_->stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  shuffleWriter_.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_F(VeloxShuffleWriterTest, spillFailWithOutOfMemory) {
  auto pool = std::make_shared<MyMemoryPool>(0);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  auto status = splitRowVectorStatus(*shuffleWriter_, inputVector1_);

  // should return OOM status because there's no partition buffer to spill
  ASSERT_TRUE(status.IsOutOfMemory());
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_F(VeloxShuffleWriterTest, TestSpillLargestPartition) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(9 * 1024 * 1024);
  //  pool = std::make_shared<arrow::LoggingMemoryPool>(pool.get());

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  // shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.compression_type = arrow::Compression::UNCOMPRESSED;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

} // namespace gluten
