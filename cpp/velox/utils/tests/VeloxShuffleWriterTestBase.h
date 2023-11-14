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

#pragma once

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include "LocalRssClient.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "utils/Compression.h"
#include "velox/vector/tests/VectorTestUtils.h"

namespace gluten {

struct ShuffleTestParams {
  PartitionWriterType partition_writer_type;
  arrow::Compression::type compression_type;
  CompressionMode compression_mode;

  std::string toString() const {
    std::ostringstream out;
    out << "partition_writer_type = " << partition_writer_type << "compression_type = " << compression_type
        << ", compression_mode = " << compression_mode;
    return out.str();
  }
};

class VeloxShuffleWriterTestBase : public facebook::velox::test::VectorTestBase {
 protected:
  void setUp() {
    shuffleWriterOptions_ = ShuffleWriterOptions::defaults();
    shuffleWriterOptions_.compression_threshold = 0;
    shuffleWriterOptions_.memory_pool = defaultArrowMemoryPool().get();
    GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFile());

    // Set up test data.
    children1_ = {
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
        makeFlatVector<facebook::velox::StringView>(
            {"alice0", "bob1", "alice2", "bob3", "Alice4", "Bob5", "AlicE6", "boB7", "ALICE8", "BOB9"}),
        makeNullableFlatVector<facebook::velox::StringView>(
            {"alice", "bob", std::nullopt, std::nullopt, "Alice", "Bob", std::nullopt, "alicE", std::nullopt, "boB"}),
    };

    children2_ = {
        makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
        makeFlatVector<int8_t>({1, -1}),
        makeNullableFlatVector<int32_t>({100, std::nullopt}),
        makeFlatVector<int64_t>({1, 1}),
        makeFlatVector<float>({0.142857, -0.142857}),
        makeFlatVector<bool>({true, false}),
        makeFlatVector<facebook::velox::StringView>(
            {"bob",
             "alicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealice"}),
        makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, std::nullopt}),
    };

    childrenNoNull_ = {
        makeFlatVector<int8_t>({0, 1}),
        makeFlatVector<int8_t>({0, -1}),
        makeFlatVector<int32_t>({0, 100}),
        makeFlatVector<int64_t>({0, 1}),
        makeFlatVector<float>({0, 0.142857}),
        makeFlatVector<bool>({false, true}),
        makeFlatVector<facebook::velox::StringView>({"", "alice"}),
        makeFlatVector<facebook::velox::StringView>({"alice", ""}),
    };

    inputVector1_ = makeRowVector(children1_);
    inputVector2_ = makeRowVector(children2_);
    inputVectorNoNull_ = makeRowVector(childrenNoNull_);
  }

  arrow::Status splitRowVector(VeloxShuffleWriter& shuffleWriter, facebook::velox::RowVectorPtr vector) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
    return shuffleWriter.split(cb, ShuffleWriter::kMinMemLimit);
  }

  // Create multiple local dirs and join with comma.
  arrow::Status setLocalDirsAndDataFile() {
    auto& localDirs = shuffleWriterOptions_.local_dirs;
    static const std::string kTestLocalDirsPrefix = "columnar-shuffle-test-";

    // Create first tmp dir and create data file.
    // To prevent tmpDirs from being deleted in the dtor, we need to store them.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    ARROW_ASSIGN_OR_RAISE(shuffleWriterOptions_.data_file, createTempShuffleFile(tmpDirs_.back()->path().ToString()));
    localDirs += tmpDirs_.back()->path().ToString();
    localDirs.push_back(',');

    // Create second tmp dir.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    localDirs += tmpDirs_.back()->path().ToString();
    return arrow::Status::OK();
  }

  virtual std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() = 0;

  ShuffleWriterOptions shuffleWriterOptions_;

  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator_;

  std::vector<std::unique_ptr<arrow::internal::TemporaryDir>> tmpDirs_;

  std::vector<facebook::velox::VectorPtr> children1_;
  std::vector<facebook::velox::VectorPtr> children2_;
  std::vector<facebook::velox::VectorPtr> childrenNoNull_;

  facebook::velox::RowVectorPtr inputVector1_;
  facebook::velox::RowVectorPtr inputVector2_;
  facebook::velox::RowVectorPtr inputVectorNoNull_;
};

class VeloxShuffleWriterTest : public ::testing::TestWithParam<ShuffleTestParams>, public VeloxShuffleWriterTestBase {
 protected:
  virtual void SetUp() override {
    VeloxShuffleWriterTestBase::setUp();

    ShuffleTestParams params = GetParam();
    if (params.partition_writer_type == PartitionWriterType::kCeleborn) {
      partitionWriterCreator_ = std::make_shared<CelebornPartitionWriterCreator>(
          std::make_shared<LocalRssClient>(shuffleWriterOptions_.data_file));
      shuffleWriterOptions_.partition_writer_type = kCeleborn;
    } else {
      partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>();
    }
    shuffleWriterOptions_.compression_type = params.compression_type;
    shuffleWriterOptions_.compression_mode = params.compression_mode;
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  std::shared_ptr<arrow::Schema> getArrowSchema(facebook::velox::RowVectorPtr& rowVector) {
    return toArrowSchema(rowVector->type(), pool());
  }

  void setReadableFile(const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
    GLUTEN_ASSIGN_OR_THROW(file_, arrow::io::ReadableFile::Open(fileName))
  }

  void getRowVectors(std::shared_ptr<arrow::Schema> schema, std::vector<facebook::velox::RowVectorPtr>& vectors) {
    ShuffleReaderOptions options;
    options.compression_type = shuffleWriterOptions_.compression_type;
    auto reader = std::make_shared<VeloxShuffleReader>(schema, options, defaultArrowMemoryPool().get(), pool_);
    auto iter = reader->readStream(file_);
    while (iter->hasNext()) {
      auto vector = std::dynamic_pointer_cast<VeloxColumnarBatch>(iter->next())->getRowVector();
      vectors.emplace_back(vector);
    }
  }

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

class SinglePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void testShuffleWrite(VeloxShuffleWriter& shuffleWriter, std::vector<facebook::velox::RowVectorPtr> vectors) {
    for (auto& vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
      // No partition buffers for single partitioner.
      ASSERT_EQ(shuffleWriter.partitionBufferSize(), 0);
    }
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(shuffleWriter.dataFile());
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), 1);

    auto schema = getArrowSchema(vectors[0]);
    std::vector<facebook::velox::RowVectorPtr> deserializedVectors;
    setReadableFile(shuffleWriter.dataFile());
    getRowVectors(schema, deserializedVectors);

    ASSERT_EQ(deserializedVectors.size(), vectors.size());
    for (int32_t i = 0; i < deserializedVectors.size(); i++) {
      facebook::velox::test::assertEqualVectors(vectors[i], deserializedVectors[i]);
    }
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() override {
    shuffleWriterOptions_.buffer_size = 10;
    shuffleWriterOptions_.partitioning = Partitioning::kSingle;
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_))
    return shuffleWriter;
  }
};

class MultiplePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void shuffleWriteReadMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(shuffleWriter.dataFile());
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), expectPartitionLength);
    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0);
    auto schema = toArrowSchema(dataType, pool());
    setReadableFile(shuffleWriter.dataFile());
    ASSERT_EQ(*file_->GetSize(), lengthSum);
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      if (expectedVectors[i].size() == 0) {
        ASSERT_EQ(lengths[i], 0);
      } else {
        std::vector<facebook::velox::RowVectorPtr> deserializedVectors;
        getRowVectors(schema, deserializedVectors);
        if (i != 0) {
          ASSERT_NOT_OK(file_->Advance(lengths[i - 1]));
        }
        ASSERT_EQ(expectedVectors[i].size(), deserializedVectors.size());
        for (int32_t j = 0; j < expectedVectors[i].size(); j++) {
          facebook::velox::test::assertEqualVectors(expectedVectors[i][j], deserializedVectors[j]);
        }
      }
    }
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<facebook::velox::RowVectorPtr> vectors,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) {
    for (auto& vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }
};

class HashPartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  void SetUp() override {
    MultiplePartitioningShuffleWriter::SetUp();

    children1_.insert((children1_.begin()), makeFlatVector<int32_t>({1, 2, 2, 2, 2, 1, 1, 1, 2, 1}));
    hashInputVector1_ = makeRowVector(children1_);
    children2_.insert((children2_.begin()), makeFlatVector<int32_t>({2, 2}));
    hashInputVector2_ = makeRowVector(children2_);
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() override {
    shuffleWriterOptions_.buffer_size = 4;
    shuffleWriterOptions_.partitioning = Partitioning::kHash;
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))
    return shuffleWriter;
  }

  std::vector<uint32_t> hashPartitionIds_{1, 2};

  facebook::velox::RowVectorPtr hashInputVector1_;
  facebook::velox::RowVectorPtr hashInputVector2_;
};

class RangePartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  void SetUp() override {
    MultiplePartitioningShuffleWriter::SetUp();

    auto pid1 = makeRowVector({makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})});
    auto rangeVector1 = makeRowVector(inputVector1_->children());
    compositeBatch1_ = CompositeColumnarBatch::create(
        {std::make_shared<VeloxColumnarBatch>(pid1), std::make_shared<VeloxColumnarBatch>(rangeVector1)});

    auto pid2 = makeRowVector({makeFlatVector<int32_t>({0, 1})});
    auto rangeVector2 = makeRowVector(inputVector2_->children());
    compositeBatch2_ = CompositeColumnarBatch::create(
        {std::make_shared<VeloxColumnarBatch>(pid2), std::make_shared<VeloxColumnarBatch>(rangeVector2)});
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() override {
    shuffleWriterOptions_.buffer_size = 4;
    shuffleWriterOptions_.partitioning = Partitioning::kRange;
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))
    return shuffleWriter;
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<std::shared_ptr<ColumnarBatch>> batches,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    for (auto& batch : batches) {
      ASSERT_NOT_OK(shuffleWriter.split(batch, ShuffleWriter::kMinMemLimit));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }

  std::shared_ptr<ColumnarBatch> compositeBatch1_;
  std::shared_ptr<ColumnarBatch> compositeBatch2_;
};

class RoundRobinPartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() override {
    shuffleWriterOptions_.buffer_size = 4;
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter, VeloxShuffleWriter::create(2, partitionWriterCreator_, shuffleWriterOptions_, pool_))
    return shuffleWriter;
  }
};

class VeloxShuffleWriterMemoryTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  void SetUp() override {
    VeloxShuffleWriterTestBase::setUp();
    // Use LocalPartitionWriter to test OOM and spill.
    partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) {
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_))
    return shuffleWriter;
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter() override {
    return createShuffleWriter(kDefaultShufflePartitions);
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
    ASSERT_NOT_OK(shuffleWriter.evictFixedSize(targetEvicted, &evicted));

    return evicted;
  };

  static constexpr uint32_t kDefaultShufflePartitions = 2;
};

} // namespace gluten
