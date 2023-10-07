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
#include "shuffle/Utils.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "shuffle/rss/CelebornPartitionWriter.h"
#include "shuffle/rss/RssClient.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace arrow;
using namespace arrow::ipc;

namespace gluten {

namespace {
arrow::Status splitRowVectorStatus(VeloxShuffleWriter& shuffleWriter, velox::RowVectorPtr vector) {
  std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
  return shuffleWriter.split(cb, ShuffleWriter::kMinMemLimit);
}
} // namespace

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

class LocalRssClient : public RssClient {
 public:
  LocalRssClient(std::string dataFile) : dataFile_(dataFile) {}

  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size) {
    auto idx = -1;
    auto maybeIdx = partitionIdx_.find(partitionId);
    auto returnSize = size;
    if (maybeIdx == partitionIdx_.end()) {
      idx = partitionIdx_.size();
      partitionIdx_[partitionId] = idx;
      auto buffer = arrow::AllocateResizableBuffer(0).ValueOrDie();
      partitionBuffers_.push_back(std::move(buffer));
      // Add EOS length.
      returnSize += sizeof(int32_t) * 2;
    } else {
      idx = maybeIdx->second;
    }

    auto& buffer = partitionBuffers_[idx];
    auto newSize = buffer->size() + size;
    if (buffer->capacity() < newSize) {
      ASSERT_NOT_OK(buffer->Reserve(newSize));
    }
    memcpy(buffer->mutable_data() + buffer->size(), bytes, size);
    ASSERT_NOT_OK(buffer->Resize(newSize));
    return returnSize;
  }

  void stop() {
    std::shared_ptr<arrow::io::FileOutputStream> fout;
    ARROW_ASSIGN_OR_THROW(fout, arrow::io::FileOutputStream::Open(dataFile_, true));

    for (auto item : partitionIdx_) {
      auto idx = item.second;
      ASSERT_NOT_OK(fout->Write(partitionBuffers_[idx]->data(), partitionBuffers_[idx]->size()));
      ASSERT_NOT_OK(writeEos(fout.get()));
      ASSERT_NOT_OK(fout->Flush());
    }
    ASSERT_NOT_OK(fout->Close());
  }

 private:
  std::string dataFile_;
  std::vector<std::unique_ptr<arrow::ResizableBuffer>> partitionBuffers_;
  std::map<uint32_t, uint32_t> partitionIdx_;
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
    shuffleWriterOptions_.memory_pool = arrowPool_.get();

    ShuffleTestParams params = GetParam();
    if (params.partition_writer_type == PartitionWriterType::kCeleborn) {
      auto configuredDirs = getConfiguredLocalDirs().ValueOrDie();
      auto dataFile = createTempShuffleFile(configuredDirs[0]).ValueOrDie();
      shuffleWriterOptions_.data_file = dataFile;
      partitionWriterCreator_ =
          std::make_shared<CelebornPartitionWriterCreator>(std::make_shared<LocalRssClient>(dataFile));
      shuffleWriterOptions_.partition_writer_type = kCeleborn;
    } else {
      partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>();
    }
    shuffleWriterOptions_.compression_type = params.compression_type;
    shuffleWriterOptions_.compression_mode = params.compression_mode;

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
    GLUTEN_THROW_NOT_OK(shuffleWriter.split(cb, ShuffleWriter::kMinMemLimit));
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
    auto reader = std::make_shared<VeloxShuffleReader>(schema, options, arrowPool_.get(), pool_);
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
    auto schema = toArrowSchema(dataType, pool());
    setReadableFile(shuffleWriter.dataFile());
    ASSERT_EQ(*file_->GetSize(), lengthSum);
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      if (expectedVectors[i].size() == 0) {
        ASSERT_EQ(lengths[i], 0);
      } else {
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
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<std::shared_ptr<ColumnarBatch>> batches,
      int32_t expectPartitionLength,
      TypePtr dataType,
      std::vector<std::vector<velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    for (auto& batch : batches) {
      GLUTEN_THROW_NOT_OK(shuffleWriter.split(batch, ShuffleWriter::kMinMemLimit));
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

  int64_t splitRowVectorAndSpill(std::vector<velox::RowVectorPtr> vectors, bool shrink) {
    for (auto vector : vectors) {
      ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, vector));
    }

    auto targetEvicted = shuffleWriter_->cachedPayloadSize();
    if (shrink) {
      targetEvicted += shuffleWriter_->partitionBufferSize();
    }
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(targetEvicted, &evicted));

    return evicted;
  };

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
  std::vector<uint32_t> hashPartitionIds_{1, 2};

  std::shared_ptr<arrow::MemoryPool> arrowPool_ = defaultArrowMemoryPool();
};

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
  shuffleWriterOptions_.memory_pool = pool.get();
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
  shuffleWriterOptions_.memory_pool = pool.get();
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

TEST_P(VeloxShuffleWriterTest, TestSplitSpillAndShrink) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 100; // Set a large buffer size to make sure there are spaces to shrink.
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));

    auto bufferSize = shuffleWriter_->partitionBufferSize();
    auto payloadSize = shuffleWriter_->cachedPayloadSize();
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
    ASSERT_GT(evicted, 0);
  }

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, TestStopShrinkAndSpill) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 100; // Set a large buffer size to make sure there are spaces to shrink.
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }

  auto bufferSize = shuffleWriter_->partitionBufferSize();
  auto payloadSize = shuffleWriter_->cachedPayloadSize();
  if (shuffleWriterOptions_.partition_writer_type == PartitionWriterType::kLocal) {
    // No evict triggered, the cached payload should not be empty.
    ASSERT_GT(payloadSize, 0);
  }

  int64_t evicted;
  shuffleWriter_->setSplitState(SplitState::kStop);
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
  // Total evicted should be greater than payloadSize, to test shrinking has been triggered.
  ASSERT_GT(evicted, payloadSize);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, TestSpillOnStop) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 100; // Set a large buffer size to make sure there are spaces to shrink.
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  auto evicted = splitRowVectorAndSpill({inputVector1_, inputVector2_, inputVector1_}, true);
  ASSERT_GT(evicted, 0);
  ASSERT_EQ(shuffleWriter_->cachedPayloadSize(), 0);

  // Split multiple times, to get non-empty partition buffers and cached payloads.
  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }

  if (shuffleWriterOptions_.partition_writer_type == PartitionWriterType::kLocal) {
    // No evict triggered, the cached payload should not be empty.
    ASSERT_GT(shuffleWriter_->cachedPayloadSize(), 0);
  }

  // Spill on stop.
  shuffleWriter_->setSplitState(SplitState::kStop);

  auto payloadSize = shuffleWriter_->cachedPayloadSize();
  auto bufferSize = shuffleWriter_->partitionBufferSize();
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
  // Total evicted should be greater than payloadSize, to test shrinking has been triggered.
  ASSERT_GT(evicted, payloadSize);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, TestSpill) {
  int32_t numPartitions = 4;
  shuffleWriterOptions_.buffer_size = 1; // Set a small buffer size to force clear and cache buffers for each split.
  shuffleWriterOptions_.partitioning_name = "hash";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, hashInputVector1_));

  // Clear buffers and evict payloads.
  for (auto pid : hashPartitionIds_) {
    std::unique_ptr<arrow::ipc::IpcPayload> payload;
    ARROW_ASSIGN_OR_THROW(payload, shuffleWriter_->createPayloadFromBuffer(pid, true));
    if (payload) {
      ASSERT_NOT_OK(shuffleWriter_->evictPayload(pid, std::move(payload)));
    }
  }

  // Evict all payloads.
  auto evicted = splitRowVectorAndSpill({hashInputVector1_}, true);
  if (shuffleWriterOptions_.partition_writer_type == PartitionWriterType::kLocal) {
    ASSERT_GT(evicted, 0);
  }
  // No more cached payloads after spill.
  ASSERT_EQ(shuffleWriter_->cachedPayloadSize(), 0);

  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, hashInputVector1_));

  auto block1Pid1 = takeRows(inputVector1_, {0, 5, 6, 7, 9});
  auto block1Pid2 = takeRows(inputVector1_, {1, 2, 3, 4, 8});

  shuffleWriteReadMultiBlocks(
      *shuffleWriter_,
      numPartitions,
      inputVector1_->type(),
      {{}, {block1Pid1, block1Pid1, block1Pid1}, {block1Pid2, block1Pid2, block1Pid2}, {}});
}

TEST_P(VeloxShuffleWriterTest, TestShrinkZeroSizeBuffer) {
  // Test 2 cases:
  // 1. partition buffer size before shrink is 0.
  // 2. partition buffer size after shrink is 0.
  int32_t numPartitions = 200; // Set a large number of partitions to create empty partition buffers.
  shuffleWriterOptions_.buffer_size = 100; // Set a large buffer size to make sure there are spaces to shrink.
  shuffleWriterOptions_.partitioning_name = "hash";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));
  auto evicted = splitRowVectorAndSpill({hashInputVector1_, hashInputVector2_, hashInputVector1_}, true);
  ASSERT_GT(evicted, 0);

  // Clear buffers then the size after shrink will be 0.
  for (auto pid : hashPartitionIds_) {
    std::unique_ptr<arrow::ipc::IpcPayload> payload;
    ARROW_ASSIGN_OR_THROW(payload, shuffleWriter_->createPayloadFromBuffer(pid, true));
    if (payload) {
      ASSERT_NOT_OK(shuffleWriter_->evictPayload(pid, std::move(payload)));
    }
  }

  auto bufferSize = shuffleWriter_->partitionBufferSize();
  auto payloadSize = shuffleWriter_->cachedPayloadSize();
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
  // All cached payloads and partition buffer memory should be evicted.
  ASSERT_EQ(evicted, payloadSize + bufferSize);
  // All buffers should be released.
  ASSERT_EQ(shuffleWriter_->partitionBufferSize(), 0);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, SmallBufferSizeNoShrink) {
  int32_t numPartitions = 4; // Set a large number of partitions to create empty partition buffers.
  shuffleWriterOptions_.buffer_size = 1; // Set a small buffer size to test no space to shrink.
  shuffleWriterOptions_.partitioning_name = "hash";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, hashInputVector1_));

  int64_t evicted = 0;
  auto bufferSize = shuffleWriter_->partitionBufferSize();
  auto payloadSize = shuffleWriter_->cachedPayloadSize();
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(payloadSize + bufferSize, &evicted));
  ASSERT_EQ(evicted, 0);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, SinglePartitioningNoShrink) {
  shuffleWriterOptions_.partitioning_name = "single";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  // Split multiple times, to get non-empty partition buffers and cached payloads.
  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));
    ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  }

  ASSERT_EQ(shuffleWriter_->getSplitState(), SplitState::kInit);

  // No partition buffers for single partitioner.
  ASSERT_EQ(shuffleWriter_->partitionBufferSize(), 0);

  int64_t evicted = 0;
  auto cachedPayloadSize = shuffleWriter_->cachedPayloadSize();
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(cachedPayloadSize + 1, &evicted));
  // No shrink.
  ASSERT_EQ(evicted, cachedPayloadSize);
  // No more cached payloads after spill.
  ASSERT_EQ(shuffleWriter_->cachedPayloadSize(), 0);

  // No more space to spill or shrink.
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(1, &evicted));
  ASSERT_EQ(evicted, 0);
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, PreAllocPartitionBuffer1) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 2; // Set a small buffer size.
  shuffleWriterOptions_.buffer_realloc_threshold = 0; // Force re-alloc on buffer size changed.
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  // First spilt no null.
  std::vector<VectorPtr> noNull = {
      makeFlatVector<int8_t>({0, 1}),
      makeFlatVector<int8_t>({0, -1}),
      makeFlatVector<int32_t>({0, 100}),
      makeFlatVector<int64_t>({0, 1}),
      makeFlatVector<float>({0, 0.142857}),
      makeFlatVector<bool>({false, true}),
      makeFlatVector<velox::StringView>({"", "alice"}),
      makeFlatVector<velox::StringView>({"alice", ""}),
  };
  auto inputNoNull = makeRowVector(noNull);

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
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputNoNull));
  // Split second input, continue filling but update null.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputHasNull));

  // Split first input again.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputNoNull));
  // Check when buffer is full, evict current buffers and reuse.
  auto cachedPayloadSize = shuffleWriter_->cachedPayloadSize();
  auto partitionBufferBeforeEvict = shuffleWriter_->partitionBufferSize();
  int64_t evicted;
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(cachedPayloadSize, &evicted));
  // Check only cached data being spilled.
  ASSERT_EQ(evicted, cachedPayloadSize);
  ARROW_CHECK_EQ(shuffleWriter_->partitionBufferSize(), partitionBufferBeforeEvict);

  // Split more data with null. New buffer size is larger.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));

  // Split more data with null. New buffer size is smaller.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector2_));

  // Split more data with null. New buffer size is larger and current data is preserved.
  // Evict cached data first.
  ASSERT_NOT_OK(shuffleWriter_->evictFixedSize(shuffleWriter_->cachedPayloadSize(), &evicted));
  // Set a large buffer size.
  shuffleWriter_->options().buffer_size = 100;
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputVector1_));
  // No data got evicted so the cached size is 0.
  ASSERT_EQ(shuffleWriter_->cachedPayloadSize(), 0);

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_P(VeloxShuffleWriterTest, PreAllocPartitionBuffer2) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 2; // Set a small buffer size.
  shuffleWriterOptions_.buffer_realloc_threshold = 100; // Set a large threshold to force buffer reused.
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, VeloxShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_, pool_));

  // First spilt no null.
  std::vector<VectorPtr> noNull = {
      makeFlatVector<int8_t>({0, 1}),
      makeFlatVector<int8_t>({0, -1}),
      makeFlatVector<int32_t>({0, 100}),
      makeFlatVector<int64_t>({0, 1}),
      makeFlatVector<float>({0, 0.142857}),
      makeFlatVector<bool>({false, true}),
      makeFlatVector<velox::StringView>({"", "alice"}),
      makeFlatVector<velox::StringView>({"alice", ""}),
  };
  auto inputNoNull = makeRowVector(noNull);

  // Second split has null int.
  std::vector<VectorPtr> fixedWithdHasNull = {
      makeNullableFlatVector<int8_t>({0, 1, std::nullopt, std::nullopt}),
      makeNullableFlatVector<int8_t>({0, -1, std::nullopt, std::nullopt}),
      makeNullableFlatVector<int32_t>({0, 100, std::nullopt, std::nullopt}),
      makeNullableFlatVector<int64_t>({0, 1, std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({0, 0.142857, std::nullopt, std::nullopt}),
      makeNullableFlatVector<bool>({false, true, std::nullopt, std::nullopt}),
      makeNullableFlatVector<velox::StringView>({"", "alice", "", ""}),
      makeNullableFlatVector<velox::StringView>({"alice", "", "", ""}),
  };
  auto inputFixedWidthHasNull = makeRowVector(fixedWithdHasNull);

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

  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputNoNull));
  // Split more data with null. Already filled + to be filled > buffer size, Buffer is resized larger.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputFixedWidthHasNull));
  // Split more data with null. Already filled + to be filled > buffer size, newSize is smaller so buffer is not
  // resized.
  ASSERT_NOT_OK(splitRowVectorStatus(*shuffleWriter_, inputStringHasNull));

  ASSERT_NOT_OK(shuffleWriter_->stop());
}

INSTANTIATE_TEST_SUITE_P(
    VeloxShuffleWriteParam,
    VeloxShuffleWriterTest,
    ::testing::Values(
        ShuffleTestParams{PartitionWriterType::kLocal, arrow::Compression::UNCOMPRESSED, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kLocal, arrow::Compression::LZ4_FRAME, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kLocal, arrow::Compression::ZSTD, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kCeleborn, arrow::Compression::UNCOMPRESSED, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kCeleborn, arrow::Compression::LZ4_FRAME, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kCeleborn, arrow::Compression::ZSTD, CompressionMode::BUFFER},
        ShuffleTestParams{PartitionWriterType::kLocal, arrow::Compression::UNCOMPRESSED, CompressionMode::ROWVECTOR},
        ShuffleTestParams{PartitionWriterType::kCeleborn, arrow::Compression::LZ4_FRAME, CompressionMode::ROWVECTOR}));

} // namespace gluten
