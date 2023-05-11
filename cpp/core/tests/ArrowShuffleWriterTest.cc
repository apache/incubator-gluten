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

void printTrace(void) {
  char** strings;
  size_t i, size;
  enum Constexpr { kMaxSize = 1024 };
  void* array[kMaxSize];
  size = backtrace(array, kMaxSize);
  strings = backtrace_symbols(array, size);
  for (i = 0; i < size; i++)
    printf("    %s\n", strings[i]);
  puts("");
  free(strings);
}

#include "TestUtils.h"
#include "memory/ColumnarBatch.h"
#include "shuffle/ArrowShuffleWriter.h"

namespace gluten {
namespace shuffle {

class MyMemoryPool final : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool(int64_t capacity) : capacity_(capacity) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    if (bytes_allocated() + size > capacity_) {
      return arrow::Status::OutOfMemory("malloc of size ", size, " failed");
    }
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    // std::cout << "Allocate: size = " << size << " addr = " << std::hex <<
    //(uint64_t)*out << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override {
    if (newSize > capacity_) {
      return arrow::Status::OutOfMemory("malloc of size ", newSize, " failed");
    }
    // auto old_ptr = *ptr;
    RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, ptr));
    stats_.UpdateAllocatedBytes(newSize - oldSize);
    // std::cout << "Reallocate: old_size = " << old_size << " old_ptr = " <<
    // std::hex <<
    //(uint64_t)old_ptr << std::dec << " new_size = " << new_size << " addr = "
    //<<
    // std::hex << (uint64_t)*ptr << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
    // std::cout << "Free: size = " << size << " addr = " << std::hex <<
    // (uint64_t)buffer
    //<< std::dec << std::endl;
    // print_trace();
  }

  int64_t bytes_allocated() const override {
    return stats_.bytes_allocated();
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return pool_->backend_name();
  }

 private:
  MemoryPool* pool_ = arrow::default_memory_pool();
  int64_t capacity_;
  arrow::internal::MemoryPoolStats stats_;
};

class ArrowShuffleWriterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto hashPartitionKey = field("hash_partition_key", arrow::int32());
    auto fNa = field("f_na", arrow::null());
    auto fInt8A = field("f_int8_a", arrow::int8());
    auto fInt8B = field("f_int8_b", arrow::int8());
    auto fInt32 = field("f_int32", arrow::int32());
    auto fUint64 = field("f_uint64", arrow::uint64());
    auto fDouble = field("f_double", arrow::float64());
    auto fBool = field("f_bool", arrow::boolean());
    auto fString = field("f_string", arrow::utf8());
    auto fNullableString = field("f_nullable_string", arrow::utf8());
    auto fDecimal = field("f_decimal128", arrow::decimal(10, 2));

    ARROW_ASSIGN_OR_THROW(tmpDir1_, arrow::internal::TemporaryDir::Make(kTmpDirPrefix))
    ARROW_ASSIGN_OR_THROW(tmpDir2_, arrow::internal::TemporaryDir::Make(kTmpDirPrefix))
    auto configDirs = tmpDir1_->path().ToString() + "," + tmpDir2_->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", configDirs.c_str(), 1);

    schema_ = arrow::schema({fNa, fInt8A, fInt8B, fInt32, fUint64, fDouble, fBool, fString, fNullableString, fDecimal});

    makeInputBatch(kInputData1, schema_, &inputBatch1_);
    makeInputBatch(kInputData2, schema_, &inputBatch2_);

    std::merge(
        kHashKey1.begin(), kHashKey1.end(), kInputData1.begin(), kInputData1.end(), back_inserter(hashInputData1_));
    std::merge(
        kHashKey2.begin(), kHashKey2.end(), kInputData2.begin(), kInputData2.end(), back_inserter(hashInputData2_));
    hashSchema_ = arrow::schema(
        {hashPartitionKey, fNa, fInt8A, fInt8B, fInt32, fUint64, fDouble, fBool, fString, fNullableString, fDecimal});
    makeInputBatch(hashInputData1_, hashSchema_, &hashInputBatch1_);
    makeInputBatch(hashInputData2_, hashSchema_, &hashInputBatch2_);
    shuffleWriterOptions_ = ShuffleWriterOptions::defaults();
    partitionWriterCreator_ = std::make_shared<LocalPartitionWriterCreator>();
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExsists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> takeRows(
      const std::shared_ptr<arrow::RecordBatch>& inputBatch,
      const std::string& jsonIdx) {
    std::shared_ptr<arrow::Array> takeIdx;
    ARROW_ASSIGN_OR_THROW(takeIdx, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), jsonIdx));

    auto cntx = arrow::compute::ExecContext();
    std::shared_ptr<arrow::RecordBatch> res;
    ARROW_ASSIGN_OR_RAISE(
        arrow::Datum result,
        arrow::compute::Take(arrow::Datum(inputBatch), arrow::Datum(takeIdx), arrow::compute::TakeOptions{}, &cntx));
    return result.record_batch();
  }

  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> getRecordBatchStreamReader(
      const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      RETURN_NOT_OK(file_->Close());
    }
    ARROW_ASSIGN_OR_RAISE(file_, arrow::io::ReadableFile::Open(fileName))
    ARROW_ASSIGN_OR_RAISE(auto file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_))
    return file_reader;
  }

  static const std::string kTmpDirPrefix;
  static const std::vector<std::string> kInputData1;
  static const std::vector<std::string> kInputData2;

  std::shared_ptr<arrow::internal::TemporaryDir> tmpDir1_;
  std::shared_ptr<arrow::internal::TemporaryDir> tmpDir2_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<ArrowShuffleWriter> shuffleWriter_;
  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator_;
  ShuffleWriterOptions shuffleWriterOptions_;

  std::shared_ptr<arrow::RecordBatch> inputBatch1_;
  std::shared_ptr<arrow::RecordBatch> inputBatch2_;

  // hash batch first column is partition key hash value named
  // hash_partition_key
  static const std::vector<std::string> kHashKey1;
  static const std::vector<std::string> kHashKey2;
  std::vector<std::string> hashInputData1_;
  std::vector<std::string> hashInputData2_;
  std::shared_ptr<arrow::Schema> hashSchema_;
  std::shared_ptr<arrow::RecordBatch> hashInputBatch1_;
  std::shared_ptr<arrow::RecordBatch> hashInputBatch2_;

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

const std::string ArrowShuffleWriterTest::kTmpDirPrefix = "columnar-shuffle-test";
const std::vector<std::string> ArrowShuffleWriterTest::kInputData1 = {
    "[null, null, null, null, null, null, null, null, null, null]",
    "[1, 2, 3, null, 4, null, 5, 6, null, 7]",
    "[1, -1, null, null, -2, 2, null, null, 3, -3]",
    "[1, 2, 3, 4, null, 5, 6, 7, 8, null]",
    "[null, null, null, null, null, null, null, null, null, null]",
    R"([-0.1234567, null, 0.1234567, null, -0.142857, null, 0.142857, 0.285714, 0.428617, null])",
    "[null, true, false, null, true, true, false, true, null, null]",
    R"(["alice0", "bob1", "alice2", "bob3", "Alice4", "Bob5", "AlicE6", "boB7", "ALICE8", "BOB9"])",
    R"(["alice", "bob", null, null, "Alice", "Bob", null, "alicE", null, "boB"])",
    R"(["-1.01", "2.01", "-3.01", null, "0.11", "3.14", "2.27", null, "-3.14", null])"};

const std::vector<std::string> ArrowShuffleWriterTest::kInputData2 = {
    "[null, null]",
    "[null, null]",
    "[1, -1]",
    "[100, null]",
    "[1, 1]",
    R"([0.142857, -0.142857])",
    "[true, false]",
    R"(["bob", "alicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealice"])",
    R"([null, null])",
    R"([null, null])"};

const std::vector<std::string> ArrowShuffleWriterTest::kHashKey1 = {"[1, 2, 2, 2, 2, 1, 1, 1, 2, 1]"};
const std::vector<std::string> ArrowShuffleWriterTest::kHashKey2 = {"[2, 2]"};

std::shared_ptr<ColumnarBatch> recordBatchToColumnarBatch(std::shared_ptr<arrow::RecordBatch> rb) {
  std::unique_ptr<ArrowSchema> cSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> cArray = std::make_unique<ArrowArray>();
  ASSERT_NOT_OK(arrow::ExportRecordBatch(*rb, cArray.get(), cSchema.get()));
  return std::make_shared<ArrowCStructColumnarBatch>(std::move(cSchema), std::move(cArray));
}

TEST_F(ArrowShuffleWriterTest, TestSinglePartPartitioner) {
  shuffleWriterOptions_.buffer_size = 10;
  shuffleWriterOptions_.partitioning_name = "single";

  ARROW_ASSIGN_OR_THROW(shuffleWriter_, ArrowShuffleWriter::create(1, partitionWriterCreator_, shuffleWriterOptions_))

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch2_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  // verify data file
  checkFileExsists(shuffleWriter_->dataFile());

  // verify output temporary files
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 1);

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify schema
  ASSERT_EQ(*fileReader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);

  std::vector<arrow::RecordBatch*> expected = {inputBatch1_.get(), inputBatch2_.get(), inputBatch1_.get()};
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
      //      std::cout << " result " << rb->column(j)->ToString() << std::endl;
      //      std::cout << " expected " << expected[i]->column(j)->ToString() <<
      //      std::endl;
      ASSERT_TRUE(
          rb->column(j)->Equals(*expected[i]->column(j), arrow::EqualOptions::Defaults().diff_sink(&std::cout)));
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinPartitioner) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch2_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *schema_);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  std::shared_ptr<arrow::RecordBatch> resBatch1;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatch1_, "[0, 2, 4, 6, 8]"))
  ARROW_ASSIGN_OR_THROW(resBatch1, takeRows(inputBatch2_, "[0]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get(), resBatch1.get(), resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatch1_, "[1, 3, 5, 7, 9]"))
  ARROW_ASSIGN_OR_THROW(resBatch1, takeRows(inputBatch2_, "[1]"))
  expected = {resBatch0.get(), resBatch1.get(), resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestShuffleWriterMemoryLeak) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(17 * 1024 * 1024);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.write_schema = false;
  shuffleWriterOptions_.partitioning_name = "rr";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch2_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  shuffleWriter_.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_F(ArrowShuffleWriterTest, TestHashPartitioner) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_))

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(hashInputBatch1_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(hashInputBatch2_).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(hashInputBatch1_).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);

  // verify data file
  checkFileExsists(shuffleWriter_->dataFile());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify schema
  ASSERT_EQ(*fileReader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));

  for (const auto& rb : batches) {
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto i = 0; i < rb->num_columns(); ++i) {
      ASSERT_EQ(rb->column(i)->length(), rb->num_rows());
    }
  }
}

TEST_F(ArrowShuffleWriterTest, TestFallbackRangePartitioner) {
  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "range";

  std::shared_ptr<arrow::Array> pidArr0;
  ARROW_ASSIGN_OR_THROW(
      pidArr0, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 0, 1, 0, 1, 0, 1, 0, 1]"));
  std::shared_ptr<arrow::Array> pidArr1;
  ARROW_ASSIGN_OR_THROW(pidArr1, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1]"));

  std::shared_ptr<arrow::Schema> schemaWPid;
  std::shared_ptr<arrow::RecordBatch> inputBatch1WPid;
  std::shared_ptr<arrow::RecordBatch> inputBatch2WPid;
  ARROW_ASSIGN_OR_THROW(schemaWPid, schema_->AddField(0, arrow::field("pid", arrow::int32())));
  ARROW_ASSIGN_OR_THROW(inputBatch1WPid, inputBatch1_->AddColumn(0, "pid", pidArr0));
  ARROW_ASSIGN_OR_THROW(inputBatch2WPid, inputBatch2_->AddColumn(0, "pid", pidArr1));

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_))

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1WPid).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch2WPid).get()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1WPid).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *schema_);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  std::shared_ptr<arrow::RecordBatch> resBatch1;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatch1_, "[0, 2, 4, 6, 8]"))
  ARROW_ASSIGN_OR_THROW(resBatch1, takeRows(inputBatch2_, "[0]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get(), resBatch1.get(), resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatch1_, "[1, 3, 5, 7, 9]"))
  ARROW_ASSIGN_OR_THROW(resBatch1, takeRows(inputBatch2_, "[1]"))
  expected = {resBatch0.get(), resBatch1.get(), resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestSpillFailWithOutOfMemory) {
  auto pool = std::make_shared<MyMemoryPool>(0);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.memory_pool = pool;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  auto status = shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get());
  // should return OOM status because there's no partition buffer to spill
  ASSERT_TRUE(status.IsOutOfMemory());
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_F(ArrowShuffleWriterTest, TestSpillLargestPartition) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(9 * 1024 * 1024);
  //  pool = std::make_shared<arrow::LoggingMemoryPool>(pool.get());

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  // shuffleWriterOptions_.memory_pool = pool.get();
  shuffleWriterOptions_.compression_type = arrow::Compression::UNCOMPRESSED;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));
    ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch2_).get()));
    ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatch1_).get()));
  }
  ASSERT_NOT_OK(shuffleWriter_->stop());
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinListArrayShuffleWriter) {
  auto fArrStr = arrow::field("f_arr", arrow::list(arrow::utf8()));
  auto fArrBool = arrow::field("f_bool", arrow::list(arrow::boolean()));
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::int32()));
  auto fArrDouble = arrow::field("f_double", arrow::list(arrow::float64()));
  auto fArrDecimal = arrow::field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto rbSchema = arrow::schema({fArrStr, fArrBool, fArrInt32, fArrDouble, fArrDecimal});

  const std::vector<std::string> inputDataArr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], [null], [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], [null]])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], [null]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  std::shared_ptr<arrow::RecordBatch> resBatch1;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2, 4]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3, 5]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinNestListArrayShuffleWriter) {
  auto fArrStr = arrow::field("f_str", arrow::list(arrow::list(arrow::utf8())));
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::list(arrow::int32())));

  auto rbSchema = arrow::schema({fArrStr, fArrInt32});

  const std::vector<std::string> inputDataArr = {
      R"([[["alice0", "bob1"]], [["alice2"], ["bob3"]], [["Alice4", "Bob5", "AlicE6"]], [["boB7"], ["ALICE8", "BOB9"]]])",
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinNestLargeListArrayShuffleWriter) {
  auto fArrStr = arrow::field("f_str", arrow::large_list(arrow::list(arrow::utf8())));
  auto fArrInt32 = arrow::field("f_int32", arrow::large_list(arrow::list(arrow::int32())));

  auto rbSchema = arrow::schema({fArrStr, fArrInt32});

  const std::vector<std::string> inputDataArr = {
      R"([[["alice0", "bob1"]], [["alice2"], ["bob3"]], [["Alice4", "Bob5", "AlicE6"]], [["boB7"], ["ALICE8", "BOB9"]]])",
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinListStructArrayShuffleWriter) {
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto fArrListStruct = arrow::field(
      "f_list_struct",
      arrow::list(arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())})));

  auto rbSchema = arrow::schema({fArrInt32, fArrListStruct});

  const std::vector<std::string> inputDataArr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[{"a": 4, "b": null}], [{"a": 42, "b": null}, {"a": null, "b": "foo2"}], [{"a": 43, "b": "foo3"}], [{"a": 44, "b": "foo4"}]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinListMapArrayShuffleWriter) {
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto fArrListMap = arrow::field("f_list_map", arrow::list(arrow::map(arrow::utf8(), arrow::utf8())));

  auto rbSchema = arrow::schema({fArrInt32, fArrListMap});

  const std::vector<std::string> inputDataArr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[[["key1", "val_aa1"]]], [[["key1", "val_bb1"]], [["key2", "val_bb2"]]], [[["key1", "val_cc1"]]], [[["key1", "val_dd1"]]]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinStructArrayShuffleWriter) {
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto fArrStructList = arrow::field(
      "f_struct_list",
      arrow::struct_({arrow::field("a", arrow::list(arrow::int32())), arrow::field("b", arrow::utf8())}));

  auto rbSchema = arrow::schema({fArrInt32, fArrStructList});

  const std::vector<std::string> inputDataArr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([{"a": [1,1,1,1], "b": null}, {"a": null, "b": "foo2"}, {"a": [3,3,3,3], "b": "foo3"}, {"a": [4,4,4,4], "b": "foo4"}])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinMapArrayShuffleWriter) {
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto fArrMap = arrow::field("f_map", arrow::map(arrow::utf8(), arrow::utf8()));

  auto rbSchema = arrow::schema({fArrInt32, fArrMap});

  const std::vector<std::string> inputDataArr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[["key1", "val_aa1"]], [["key1", "val_bb1"], ["key2", "val_bb2"]], [["key1", "val_cc1"]], [["key1", "val_dd1"]]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(ArrowShuffleWriterTest, TestHashListArrayShuffleWriterWithMorePartitions) {
  int32_t numPartitions = 5;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "hash";

  auto hashPartitionKey = arrow::field("hash_partition_key", arrow::int32());
  auto fUint64 = arrow::field("f_uint64", arrow::uint64());
  auto fArrStr = arrow::field("f_arr", arrow::list(arrow::utf8()));

  auto rbSchema = arrow::schema({hashPartitionKey, fUint64, fArrStr});
  auto dataSchema = arrow::schema({fUint64, fArrStr});
  const std::vector<std::string> inputBatch1Data = {R"([1, 2])", R"([1, 2])", R"([["alice0", "bob1"], ["alice2"]])"};
  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputBatch1Data, rbSchema, &inputBatchArr);

  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));

  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));

  ASSERT_NOT_OK(shuffleWriter_->stop());

  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 5);

  checkFileExsists(shuffleWriter_->dataFile());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  ASSERT_EQ(*fileReader->schema(), *dataSchema);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));

  for (const auto& rb : batches) {
    ASSERT_EQ(rb->num_columns(), dataSchema->num_fields());
    for (auto i = 0; i < rb->num_columns(); ++i) {
      ASSERT_EQ(rb->column(i)->length(), rb->num_rows());
    }
  }
}

TEST_F(ArrowShuffleWriterTest, TestRoundRobinListArrayShuffleWriterwithCompression) {
  auto fArrStr = arrow::field("f_arr", arrow::list(arrow::utf8()));
  auto fArrBool = arrow::field("f_bool", arrow::list(arrow::boolean()));
  auto fArrInt32 = arrow::field("f_int32", arrow::list(arrow::int32()));
  auto fArrDouble = arrow::field("f_double", arrow::list(arrow::float64()));
  auto fArrDecimal = arrow::field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto rbSchema = arrow::schema({fArrStr, fArrBool, fArrInt32, fArrDouble, fArrDecimal});

  const std::vector<std::string> inputDataArr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], [null], [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], [null]])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], [null]])"};

  std::shared_ptr<arrow::RecordBatch> inputBatchArr;
  makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);

  int32_t numPartitions = 2;
  shuffleWriterOptions_.buffer_size = 4;
  shuffleWriterOptions_.partitioning_name = "rr";
  ARROW_ASSIGN_OR_THROW(
      shuffleWriter_, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator_, shuffleWriterOptions_));
  auto compressionType = arrow::util::Codec::GetCompressionType("lz4");
  ASSERT_NOT_OK(shuffleWriter_->setCompressType(compressionType.MoveValueUnsafe()));
  ASSERT_NOT_OK(shuffleWriter_->split(recordBatchToColumnarBatch(inputBatchArr).get()));
  ASSERT_NOT_OK(shuffleWriter_->stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> fileReader;
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));

  // verify partition lengths
  const auto& lengths = shuffleWriter_->partitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*fileReader->schema(), *rbSchema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> resBatch0;
  std::shared_ptr<arrow::RecordBatch> resBatch1;
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[0, 2, 4]"))
  std::vector<arrow::RecordBatch*> expected = {resBatch0.get()};

  // verify first block
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(resBatch0, takeRows(inputBatchArr, "[1, 3, 5]"))
  expected = {resBatch0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(fileReader, getRecordBatchStreamReader(shuffleWriter_->dataFile()));
  ASSERT_EQ(*fileReader->schema(), *rbSchema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(fileReader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (size_t i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rbSchema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

} // namespace shuffle
} // namespace gluten
