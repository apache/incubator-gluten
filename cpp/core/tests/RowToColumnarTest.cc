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

#include "operators/r2c/RowToArrowColumnarConverter.h"

#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/util.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>

#include "memory/ArrowMemoryPool.h"
#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "tests/TestUtils.h"

namespace gluten {

class Row2ColumnarTest : public ::testing::Test {
 public:
  void testRecordBatchEqual(
      std::shared_ptr<arrow::RecordBatch> input_batch,
      uint8_t* expect_arr = nullptr,
      size_t expect_arr_length = 0) {
    auto columnarToRowConverter =
        std::make_shared<ArrowColumnarToRowConverter>(input_batch, GetDefaultWrappedArrowMemoryPool());

    GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
    GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

    int64_t num_rows = input_batch->num_rows();
    uint8_t* address = columnarToRowConverter->GetBufferAddress();
    auto length_vec = columnarToRowConverter->GetLengths();
    if (expect_arr_length != 0) {
      for (int i = 0; i < expect_arr_length; i++) {
        ASSERT_EQ(*(address + i), *(expect_arr + i));
      }
    }
    long arr[length_vec.size()];
    for (int i = 0; i < length_vec.size(); i++) {
      arr[i] = length_vec[i];
    }
    long* lengthPtr = arr;

    std::shared_ptr<RowToColumnarConverter> row_to_columnar_converter =
        std::make_shared<RowToColumnarConverter>(input_batch->schema());

    auto rb = row_to_columnar_converter->convert(num_rows, lengthPtr, address);
    ASSERT_TRUE(rb->Equals(*input_batch));
  }
};

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultForInt_64) {
  const std::vector<std::string> input_data = {"[1, 2, 3]"};

  auto f_int64 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResult) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());
  auto f_double = field("f_double", arrow::float64());
  auto f_float = field("f_float", arrow::float32());
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_string = field("f_string", arrow::utf8());
  auto f_binary = field("f_binary", arrow::binary());
  auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({f_int8, f_int16, f_int32, f_int64, f_float, f_double, f_binary});

  const std::vector<std::string> input_data = {
      "[1, 1]", "[1, 1]", "[1, 1]", "[1, 1]", "[3.5, 3.5]", "[1, 1]", R"(["abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultForInt_64_twoColumn) {
  const std::vector<std::string> input_data = {
      "[1, 2]",
      "[1, 2]",
  };

  auto f_int64_col0 = field("f_int64", arrow::int64());
  auto f_int64_col1 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({f_int64_col0, f_int64_col1});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_int8_int16) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());

  const std::vector<std::string> input_data = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({f_int8, f_int16});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };

  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_int32_int64) {
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());

  const std::vector<std::string> input_data = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({f_int32, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_float_double) {
  auto f_float = field("f_float", arrow::float32());
  auto f_double = field("f_double", arrow::float64());

  const std::vector<std::string> input_data = {
      "[1.0, 2.0]",
      "[1.0, 2.0]",
  };

  auto schema = arrow::schema({f_float, f_double});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  uint8_t expect_arr[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64};
  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_bool_binary) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());

  const std::vector<std::string> input_data = {
      "[false, true]",
      R"(["aa", "bb"])",
  };

  auto schema = arrow::schema({f_bool, f_binary});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_decimal_string) {
  auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));
  auto f_string = field("f_string", arrow::utf8());

  const std::vector<std::string> input_data = {R"(["1.00", "2.00"])", R"(["aa", "bb"])"};

  auto schema = arrow::schema({f_decimal, f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };

  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_int64_int64_with_null) {
  auto f_int64 = field("f_int64", arrow::int64());

  const std::vector<std::string> input_data = {
      "[null,2]",
      "[null,2]",
  };

  auto schema = arrow::schema({f_int64, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  uint8_t expect_arr[] = {
      3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_string) {
  auto f_binary = field("f_binary", arrow::binary());
  auto f_string = field("f_string", arrow::utf8());

  const std::vector<std::string> input_data = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };

  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, TestArrowColumnarToRowConverterResultBuffer_bool) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());

  const std::vector<std::string> input_data = {
      "[false, true]",

  };

  auto schema = arrow::schema({f_bool});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  };
  testRecordBatchEqual(input_batch, expect_arr, sizeof(expect_arr));
}

TEST_F(Row2ColumnarTest, allTypes) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());
  auto f_double = field("f_double", arrow::float64());
  auto f_float = field("f_float", arrow::float32());
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_string = field("f_string", arrow::utf8());
  auto f_binary = field("f_binary", arrow::binary());
  auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double, f_binary, f_decimal});

  const std::vector<std::string> input_data = {
      "[true, true]",
      "[1, 1]",
      "[1, 1]",
      "[1, 1]",
      "[1, 1]",
      "[3.5, 3.5]",
      "[1, 1]",
      R"(["abc", "abc"])",
      R"(["100.00", "100.00"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

TEST_F(Row2ColumnarTest, allTypes_18rows) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());
  auto f_double = field("f_double", arrow::float64());
  auto f_float = field("f_float", arrow::float32());
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_string = field("f_string", arrow::utf8());
  auto f_binary = field("f_binary", arrow::binary());
  auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double, f_binary, f_decimal});

  const std::vector<std::string> input_data = {
      "[true, "
      "true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,"
      "true]",
      "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, "
      "3.5, 3.5]",
      "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      R"(["abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc"])",
      R"(["100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

} // namespace gluten
