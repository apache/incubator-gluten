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

#include "compute/VeloxColumnarToRowConverter.h"
#include "compute/VeloxRowToColumnarConverter.h"
#include "jni/JniErrors.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "tests/TestUtils.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

using namespace facebook;
using namespace facebook::velox;

namespace gluten {
class VeloxColumnarToRowTest : public ::testing::Test, public test::VectorTestBase {
 public:
  RowVectorPtr RecordBatch2VeloxRowVector(const arrow::RecordBatch& rb) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    ASSERT_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
    auto vp = velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::GetDefaultWrappedVeloxMemoryPool().get());
    return std::dynamic_pointer_cast<velox::RowVector>(vp);
  }

  void testRecordBatchEqual(std::shared_ptr<arrow::RecordBatch> input_batch) {
    auto row = RecordBatch2VeloxRowVector(*input_batch);
    auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool_, veloxPool_);
    GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
    GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

    int64_t num_rows = input_batch->num_rows();

    uint8_t* address = columnarToRowConverter->GetBufferAddress();
    auto length_vec = columnarToRowConverter->GetLengths();

    long arr[length_vec.size()];
    for (int i = 0; i < length_vec.size(); i++) {
      arr[i] = length_vec[i];
    }
    long* lengthPtr = arr;

    ArrowSchema cSchema;
    GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*input_batch->schema(), &cSchema));
    auto row_to_columnar_converter = std::make_shared<VeloxRowToColumnarConverter>(&cSchema, veloxPool_);

    auto cb = row_to_columnar_converter->convert(num_rows, lengthPtr, address);
    auto vp = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb)->getRowVector();
    // std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
    velox::test::assertEqualVectors(row, vp);
  }

 private:
  std::shared_ptr<velox::memory::MemoryPool> veloxPool_ = GetDefaultWrappedVeloxMemoryPool();
  std::shared_ptr<arrow::MemoryPool> arrowPool_ = GetDefaultWrappedArrowMemoryPool();
};

TEST_F(VeloxColumnarToRowTest, timestamp) {
  // only RowVector can convert to RecordBatch
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0}, Timestamp{12, 0}, Timestamp{0, 17'123'456}, Timestamp{1, 17'123'456}, Timestamp{-1, 17'123'456}};
  auto row = makeRowVector({
      makeFlatVector<Timestamp>(timeValues),
  });
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto converter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

  JniAssertOkOrThrow(
      converter->Init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  JniAssertOkOrThrow(converter->Write(), "Native convert columnar to row: ColumnarToRowConverter write failed");
}

TEST_F(VeloxColumnarToRowTest, Int_64) {
  const std::vector<std::string> input_data = {"[1, 2, 3]"};

  auto f_int64 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}

TEST_F(VeloxColumnarToRowTest, basic) {
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

TEST_F(VeloxColumnarToRowTest, Int_64_twoColumn) {
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

TEST_F(VeloxColumnarToRowTest, Buffer_int8_int16) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());

  std::cout << "---------verify f_int8, f_int16---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({f_int8, f_int16});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_int32_int64) {
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int32, f_int64---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({f_int32, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_float_double) {
  auto f_float = field("f_float", arrow::float32());
  auto f_double = field("f_double", arrow::float64());

  std::cout << "---------verify f_float, f_double---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[1.0, 2.0]",
      "[1.0, 2.0]",
  };

  auto schema = arrow::schema({f_float, f_double});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64};
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_bool_binary) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());

  std::cout << "---------verify f_bool, f_binary---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[false, true]",
      R"(["aa", "bb"])",
  };

  auto schema = arrow::schema({f_bool, f_binary});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_decimal_string) {
  auto f_string = field("f_string", arrow::utf8());

  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> input_data = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,  0,  0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0,
                          97, 97, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0};
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_int64_int64_with_null) {
  auto f_int64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int64, f_int64 with null ---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[null,2]",
      "[null,2]",
  };

  auto schema = arrow::schema({f_int64, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_string) {
  auto f_binary = field("f_binary", arrow::binary());
  auto f_string = field("f_string", arrow::utf8());

  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> input_data = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_bool) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());

  std::cout << "---------verify f_bool---------" << std::endl;
  const std::vector<std::string> input_data = {
      "[false, true]",

  };

  auto schema = arrow::schema({f_bool});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  auto row = RecordBatch2VeloxRowVector(*input_batch);
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = GetDefaultWrappedVeloxMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->Write());

  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expect_arr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expect_arr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, _allTypes) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());
  auto f_double = field("f_double", arrow::float64());
  auto f_float = field("f_float", arrow::float32());
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_string = field("f_string", arrow::utf8());
  auto f_binary = field("f_binary", arrow::binary());

  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double, f_binary});

  const std::vector<std::string> input_data = {
      "[true, true]", "[1, 1]", "[1, 1]", "[1, 1]", "[1, 1]", "[3.5, 3.5]", "[1, 1]", R"(["abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  testRecordBatchEqual(input_batch);
}

TEST_F(VeloxColumnarToRowTest, _allTypes_18rows) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());
  auto f_double = field("f_double", arrow::float64());
  auto f_float = field("f_float", arrow::float32());
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_string = field("f_string", arrow::utf8());
  auto f_binary = field("f_binary", arrow::binary());

  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double, f_binary});

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
      R"(["abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);
  testRecordBatchEqual(input_batch);
}
} // namespace gluten
