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
void print_trace(void) {
  char** strings;
  size_t i, size;
  enum Constexpr { MAX_SIZE = 1024 };
  void* array[MAX_SIZE];
  size = backtrace(array, MAX_SIZE);
  strings = backtrace_symbols(array, size);
  for (i = 0; i < size; i++)
    printf("    %s\n", strings[i]);
  puts("");
  free(strings);
}

#include "TestUtils.h"
#include "operators/shuffle/Splitter.h"

namespace gluten {
namespace shuffle {

class MyMemoryPool : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool(int64_t capacity) : capacity_(capacity) {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (bytes_allocated() + size > capacity_) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    // std::cout << "Allocate: size = " << size << " addr = " << std::hex <<
    //(uint64_t)*out << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size > capacity_) {
      return Status::OutOfMemory("malloc of size ", new_size, " failed");
    }
    auto old_ptr = *ptr;
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    // std::cout << "Reallocate: old_size = " << old_size << " old_ptr = " <<
    // std::hex <<
    //(uint64_t)old_ptr << std::dec << " new_size = " << new_size << " addr = "
    //<<
    // std::hex << (uint64_t)*ptr << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
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

class SplitterTest : public ::testing::Test {
 protected:
  void SetUp() {
    auto hash_partition_key = field("hash_partition_key", arrow::int32());
    auto f_na = field("f_na", arrow::null());
    auto f_int8_a = field("f_int8_a", arrow::int8());
    auto f_int8_b = field("f_int8_b", arrow::int8());
    auto f_int32 = field("f_int32", arrow::int32());
    auto f_uint64 = field("f_uint64", arrow::uint64());
    auto f_double = field("f_double", arrow::float64());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());
    auto f_nullable_string = field("f_nullable_string", arrow::utf8());
    auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

    ARROW_ASSIGN_OR_THROW(tmp_dir_1_, std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    ARROW_ASSIGN_OR_THROW(tmp_dir_2_, std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    auto config_dirs = tmp_dir_1_->path().ToString() + "," + tmp_dir_2_->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", config_dirs.c_str(), 1);

    schema_ = arrow::schema(
        {f_na, f_int8_a, f_int8_b, f_int32, f_uint64, f_double, f_bool, f_string, f_nullable_string, f_decimal});

    MakeInputBatch(input_data_1, schema_, &input_batch_1_);
    MakeInputBatch(input_data_2, schema_, &input_batch_2_);

    std::merge(
        hash_key_1.begin(),
        hash_key_1.end(),
        input_data_1.begin(),
        input_data_1.end(),
        back_inserter(hash_input_data_1));
    std::merge(
        hash_key_2.begin(),
        hash_key_2.end(),
        input_data_2.begin(),
        input_data_2.end(),
        back_inserter(hash_input_data_2));
    hash_schema_ = arrow::schema(
        {hash_partition_key,
         f_na,
         f_int8_a,
         f_int8_b,
         f_int32,
         f_uint64,
         f_double,
         f_bool,
         f_string,
         f_nullable_string,
         f_decimal});
    MakeInputBatch(hash_input_data_1, hash_schema_, &hash_input_batch_1_);
    MakeInputBatch(hash_input_data_2, hash_schema_, &hash_input_batch_2_);
    split_options_ = SplitOptions::Defaults();
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      file_->Close();
    }
  }

  static void CheckFileExsists(const std::string& file_name) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(file_name)), true);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> TakeRows(
      const std::shared_ptr<arrow::RecordBatch>& input_batch,
      const std::string& json_idx) {
    std::shared_ptr<arrow::Array> take_idx;
    ARROW_ASSIGN_OR_THROW(take_idx, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), json_idx));

    auto cntx = arrow::compute::ExecContext();
    std::shared_ptr<arrow::RecordBatch> res;
    ARROW_ASSIGN_OR_RAISE(
        arrow::Datum result,
        arrow::compute::Take(arrow::Datum(input_batch), arrow::Datum(take_idx), arrow::compute::TakeOptions{}, &cntx));
    return result.record_batch();
  }

  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> GetRecordBatchStreamReader(
      const std::string& file_name) {
    if (file_ != nullptr && !file_->closed()) {
      RETURN_NOT_OK(file_->Close());
    }
    ARROW_ASSIGN_OR_RAISE(file_, arrow::io::ReadableFile::Open(file_name))
    ARROW_ASSIGN_OR_RAISE(auto file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_))
    return file_reader;
  }

  static const std::string tmp_dir_prefix;
  static const std::vector<std::string> input_data_1;
  static const std::vector<std::string> input_data_2;

  std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir_1_;
  std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir_2_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Splitter> splitter_;
  SplitOptions split_options_;

  std::shared_ptr<arrow::RecordBatch> input_batch_1_;
  std::shared_ptr<arrow::RecordBatch> input_batch_2_;

  // hash batch first column is partition key hash value named
  // hash_partition_key
  static const std::vector<std::string> hash_key_1;
  static const std::vector<std::string> hash_key_2;
  std::vector<std::string> hash_input_data_1;
  std::vector<std::string> hash_input_data_2;
  std::shared_ptr<arrow::Schema> hash_schema_;
  std::shared_ptr<arrow::RecordBatch> hash_input_batch_1_;
  std::shared_ptr<arrow::RecordBatch> hash_input_batch_2_;

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

const std::string SplitterTest::tmp_dir_prefix = "columnar-shuffle-test";
const std::vector<std::string> SplitterTest::input_data_1 = {
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

const std::vector<std::string> SplitterTest::input_data_2 = {
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

const std::vector<std::string> SplitterTest::hash_key_1 = {"[1, 2, 2, 2, 2, 1, 1, 1, 2, 1]"};
const std::vector<std::string> SplitterTest::hash_key_2 = {"[2, 2]"};

TEST_F(SplitterTest, TestSingleSplitter) {
  split_options_.buffer_size = 10;

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, 1, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  // verify data file
  CheckFileExsists(splitter_->DataFile());

  // verify output temporary files
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 1);

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify schema
  ASSERT_EQ(*file_reader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);

  std::vector<arrow::RecordBatch*> expected = {input_batch_1_.get(), input_batch_2_.get(), input_batch_1_.get()};
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
      //      std::cout << " result " << rb->column(j)->ToString() << std::endl;
      //      std::cout << " expected " << expected[i]->column(j)->ToString() <<
      //      std::endl;
      ASSERT_TRUE(rb->column(j)->Equals(*expected[i]->column(j), EqualOptions::Defaults().diff_sink(&std::cout)));
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *schema_);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  std::shared_ptr<arrow::RecordBatch> res_batch_1;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_1_, "[0, 2, 4, 6, 8]"))
  ARROW_ASSIGN_OR_THROW(res_batch_1, TakeRows(input_batch_2_, "[0]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get(), res_batch_1.get(), res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_1_, "[1, 3, 5, 7, 9]"))
  ARROW_ASSIGN_OR_THROW(res_batch_1, TakeRows(input_batch_2_, "[1]"))
  expected = {res_batch_0.get(), res_batch_1.get(), res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestSplitterMemoryLeak) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(17 * 1024 * 1024);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  split_options_.memory_pool = pool;
  split_options_.write_schema = false;

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  ASSERT_TRUE(pool->bytes_allocated() == 0);
  splitter_.reset();
  ASSERT_TRUE(pool->bytes_allocated() == 0);
}

TEST_F(SplitterTest, TestHashSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("hash", hash_schema_, num_partitions, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*hash_input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*hash_input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*hash_input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);

  // verify data file
  CheckFileExsists(splitter_->DataFile());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify schema
  ASSERT_EQ(*file_reader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));

  for (const auto& rb : batches) {
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto i = 0; i < rb->num_columns(); ++i) {
      ASSERT_EQ(rb->column(i)->length(), rb->num_rows());
    }
  }
}

TEST_F(SplitterTest, TestFallbackRangeSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;

  std::shared_ptr<arrow::Array> pid_arr_0;
  ARROW_ASSIGN_OR_THROW(
      pid_arr_0, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 0, 1, 0, 1, 0, 1, 0, 1]"));
  std::shared_ptr<arrow::Array> pid_arr_1;
  ARROW_ASSIGN_OR_THROW(pid_arr_1, arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1]"));

  std::shared_ptr<arrow::Schema> schema_w_pid;
  std::shared_ptr<arrow::RecordBatch> input_batch_1_w_pid;
  std::shared_ptr<arrow::RecordBatch> input_batch_2_w_pid;
  ARROW_ASSIGN_OR_THROW(schema_w_pid, schema_->AddField(0, arrow::field("pid", arrow::int32())));
  ARROW_ASSIGN_OR_THROW(input_batch_1_w_pid, input_batch_1_->AddColumn(0, "pid", pid_arr_0));
  ARROW_ASSIGN_OR_THROW(input_batch_2_w_pid, input_batch_2_->AddColumn(0, "pid", pid_arr_1));

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("range", std::move(schema_w_pid), num_partitions, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_w_pid));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_w_pid));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_w_pid));

  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *schema_);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  std::shared_ptr<arrow::RecordBatch> res_batch_1;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_1_, "[0, 2, 4, 6, 8]"))
  ARROW_ASSIGN_OR_THROW(res_batch_1, TakeRows(input_batch_2_, "[0]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get(), res_batch_1.get(), res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_1_, "[1, 3, 5, 7, 9]"))
  ARROW_ASSIGN_OR_THROW(res_batch_1, TakeRows(input_batch_2_, "[1]"))
  expected = {res_batch_0.get(), res_batch_1.get(), res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestSpillFailWithOutOfMemory) {
  auto pool = std::make_shared<MyMemoryPool>(0);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  split_options_.memory_pool = pool;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, num_partitions, split_options_));

  auto status = splitter_->Split(*input_batch_1_);
  // should return OOM status because there's no partition buffer to spill
  ASSERT_TRUE(status.IsOutOfMemory());
  ASSERT_NOT_OK(splitter_->Stop());
}

TEST_F(SplitterTest, TestSpillLargestPartition) {
  std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>(9 * 1024 * 1024);
  //  pool = std::make_shared<arrow::LoggingMemoryPool>(pool.get());

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  // split_options_.memory_pool = pool.get();
  split_options_.compression_type = arrow::Compression::UNCOMPRESSED;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, num_partitions, split_options_));

  for (int i = 0; i < 100; ++i) {
    ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
    ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
    ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  }
  ASSERT_NOT_OK(splitter_->Stop());
}

TEST_F(SplitterTest, TestRoundRobinListArraySplitter) {
  auto f_arr_str = field("f_arr", arrow::list(arrow::utf8()));
  auto f_arr_bool = field("f_bool", arrow::list(arrow::boolean()));
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::int32()));
  auto f_arr_double = field("f_double", arrow::list(arrow::float64()));
  auto f_arr_decimal = field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto rb_schema = arrow::schema({f_arr_str, f_arr_bool, f_arr_int32, f_arr_double, f_arr_decimal});

  const std::vector<std::string> input_data_arr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], [null], [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], [null]])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], [null]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  std::shared_ptr<arrow::RecordBatch> res_batch_1;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2, 4]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3, 5]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinNestListArraySplitter) {
  auto f_arr_str = field("f_str", arrow::list(arrow::list(arrow::utf8())));
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));

  auto rb_schema = arrow::schema({f_arr_str, f_arr_int32});

  const std::vector<std::string> input_data_arr = {
      R"([[["alice0", "bob1"]], [["alice2"], ["bob3"]], [["Alice4", "Bob5", "AlicE6"]], [["boB7"], ["ALICE8", "BOB9"]]])",
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinNestLargeListArraySplitter) {
  auto f_arr_str = field("f_str", arrow::large_list(arrow::list(arrow::utf8())));
  auto f_arr_int32 = field("f_int32", arrow::large_list(arrow::list(arrow::int32())));

  auto rb_schema = arrow::schema({f_arr_str, f_arr_int32});

  const std::vector<std::string> input_data_arr = {
      R"([[["alice0", "bob1"]], [["alice2"], ["bob3"]], [["Alice4", "Bob5", "AlicE6"]], [["boB7"], ["ALICE8", "BOB9"]]])",
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinListStructArraySplitter) {
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto f_arr_list_struct = field("f_list_struct", list(struct_({field("a", int32()), field("b", utf8())})));

  auto rb_schema = arrow::schema({f_arr_int32, f_arr_list_struct});

  const std::vector<std::string> input_data_arr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[{"a": 4, "b": null}], [{"a": 42, "b": null}, {"a": null, "b": "foo2"}], [{"a": 43, "b": "foo3"}], [{"a": 44, "b": "foo4"}]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinListMapArraySplitter) {
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto f_arr_list_map = field("f_list_map", list(map(utf8(), utf8())));

  auto rb_schema = arrow::schema({f_arr_int32, f_arr_list_map});

  const std::vector<std::string> input_data_arr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[[["key1", "val_aa1"]]], [[["key1", "val_bb1"]], [["key2", "val_bb2"]]], [[["key1", "val_cc1"]]], [[["key1", "val_dd1"]]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinStructArraySplitter) {
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto f_arr_struct_list = field("f_struct_list", struct_({field("a", list(int32())), field("b", utf8())}));

  auto rb_schema = arrow::schema({f_arr_int32, f_arr_struct_list});

  const std::vector<std::string> input_data_arr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([{"a": [1,1,1,1], "b": null}, {"a": null, "b": "foo2"}, {"a": [3,3,3,3], "b": "foo3"}, {"a": [4,4,4,4], "b": "foo4"}])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestRoundRobinMapArraySplitter) {
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto f_arr_map = field("f_map", map(utf8(), utf8()));

  auto rb_schema = arrow::schema({f_arr_int32, f_arr_map});

  const std::vector<std::string> input_data_arr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[["key1", "val_aa1"]], [["key1", "val_bb1"], ["key2", "val_bb2"]], [["key1", "val_cc1"]], [["key1", "val_dd1"]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

TEST_F(SplitterTest, TestHashListArraySplitterWithMorePartitions) {
  int32_t num_partitions = 5;
  split_options_.buffer_size = 4;

  auto hash_partition_key = field("hash_partition_key", arrow::int32());
  auto f_uint64 = field("f_uint64", arrow::uint64());
  auto f_arr_str = field("f_arr", arrow::list(arrow::utf8()));

  auto rb_schema = arrow::schema({hash_partition_key, f_uint64, f_arr_str});
  auto data_schema = arrow::schema({f_uint64, f_arr_str});
  const std::vector<std::string> input_batch_1_data = {R"([1, 2])", R"([1, 2])", R"([["alice0", "bob1"], ["alice2"]])"};
  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_batch_1_data, rb_schema, &input_batch_arr);

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("hash", rb_schema, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));

  ASSERT_NOT_OK(splitter_->Stop());

  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 5);

  CheckFileExsists(splitter_->DataFile());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  ASSERT_EQ(*file_reader->schema(), *data_schema);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));

  for (const auto& rb : batches) {
    ASSERT_EQ(rb->num_columns(), data_schema->num_fields());
    for (auto i = 0; i < rb->num_columns(); ++i) {
      ASSERT_EQ(rb->column(i)->length(), rb->num_rows());
    }
  }
}

TEST_F(SplitterTest, TestRoundRobinListArraySplitterwithCompression) {
  auto f_arr_str = field("f_arr", arrow::list(arrow::utf8()));
  auto f_arr_bool = field("f_bool", arrow::list(arrow::boolean()));
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::int32()));
  auto f_arr_double = field("f_double", arrow::list(arrow::float64()));
  auto f_arr_decimal = field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto rb_schema = arrow::schema({f_arr_str, f_arr_bool, f_arr_int32, f_arr_double, f_arr_decimal});

  const std::vector<std::string> input_data_arr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], [null], [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], [null]])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], [null]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  int32_t num_partitions = 2;
  split_options_.buffer_size = 4;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", rb_schema, num_partitions, split_options_));
  auto compression_type = arrow::util::Codec::GetCompressionType("lz4");
  ASSERT_NOT_OK(splitter_->SetCompressType(compression_type.MoveValueUnsafe()));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_arr));
  ASSERT_NOT_OK(splitter_->Stop());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *rb_schema);

  // prepare first block expected result
  std::shared_ptr<arrow::RecordBatch> res_batch_0;
  std::shared_ptr<arrow::RecordBatch> res_batch_1;
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[0, 2, 4]"))
  std::vector<arrow::RecordBatch*> expected = {res_batch_0.get()};

  // verify first block
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }

  // prepare second block expected result
  ARROW_ASSIGN_OR_THROW(res_batch_0, TakeRows(input_batch_arr, "[1, 3, 5]"))
  expected = {res_batch_0.get()};

  // verify second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *rb_schema);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    ASSERT_TRUE(rb->Equals(*expected[i]));
  }
}

} // namespace shuffle
} // namespace gluten
