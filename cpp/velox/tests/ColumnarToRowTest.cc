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

#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "compute/VeloxRowToColumnarConverter.h"
#include "jni/JniErrors.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "utils/TestUtils.h"
#include "velox/vector/ComplexVector.h"

using namespace arrow;
using namespace facebook;
using namespace facebook::velox;

namespace gluten {
class ColumnarToRowTest : public ::testing::Test {
 public:
  void testRecordBatchEqual(std::shared_ptr<arrow::RecordBatch> inputBatch) {
    std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
        std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
    GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
    GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

    int64_t numRows = inputBatch->num_rows();

    uint8_t* address = columnarToRowConverter->getBufferAddress();

    auto lengthVec = columnarToRowConverter->getLengths();

    long arr[lengthVec.size()];
    for (int i = 0; i < lengthVec.size(); i++) {
      arr[i] = lengthVec[i];
    }
    long* lengthPtr = arr;

    ArrowSchema cSchema;
    GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*inputBatch->schema(), &cSchema));
    auto rowToColumnarConverter = std::make_shared<VeloxRowToColumnarConverter>(&cSchema, veloxPool_);

    auto cb = rowToColumnarConverter->convert(numRows, lengthPtr, address);
    auto vp = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb)->getRowVector();
    auto rb = veloxVectorToRecordBatch(vp);
    // std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
    // std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
    ASSERT_TRUE(rb->Equals(*inputBatch));
  }

  void testConvertBatch(std::shared_ptr<RecordBatch> inputBatch) {
    auto converter = std::make_shared<ArrowColumnarToRowConverter>(inputBatch, arrowPool_);
    jniAssertOkOrThrow(
        converter->init(),
        "Native convert columnar to row: Init "
        "ColumnarToRowConverter failed");
    jniAssertOkOrThrow(converter->write(), "Native convert columnar to row: ColumnarToRowConverter write failed");
  }

 private:
  std::shared_ptr<arrow::RecordBatch> veloxVectorToRecordBatch(VectorPtr vector) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    velox::exportToArrow(vector, arrowArray, veloxPool_.get());
    velox::exportToArrow(vector, arrowSchema);
    GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::RecordBatch> rb, arrow::ImportRecordBatch(&arrowArray, &arrowSchema));
    return rb;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_ = getDefaultVeloxLeafMemoryPool();
  std::shared_ptr<arrow::MemoryPool> arrowPool_ = getDefaultArrowMemoryPool();
};

TEST_F(ColumnarToRowTest, decimal) {
  {
    std::vector<std::shared_ptr<Field>> fields = {field("f_decimal128", decimal(10, 2))};

    auto schema = arrow::schema(fields);
    std::shared_ptr<RecordBatch> inputBatch;
    const std::vector<std::string> inputData = {R"(["-1.01", "2.95"])"};
    makeInputBatch(inputData, schema, &inputBatch);
    testConvertBatch(inputBatch);
  }

  {
    auto f2 = {field("f2", timestamp(TimeUnit::MICRO))};
    auto schema = arrow::schema(f2);
    std::shared_ptr<RecordBatch> inputBatch;
    const std::vector<std::string> inputData = {R"(["1970-01-01","2000-02-29","3989-07-14","1900-02-28"])"};
    makeInputBatch(inputData, schema, &inputBatch);
    testConvertBatch(inputBatch);
  }
}

TEST_F(ColumnarToRowTest, Int_64) {
  const std::vector<std::string> inputData = {"[1, 2, 3]"};

  auto fInt64 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);
  testRecordBatchEqual(inputBatch);
}

TEST_F(ColumnarToRowTest, basic) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());
  auto fDouble = field("f_double", arrow::float64());
  auto fFloat = field("f_float", arrow::float32());
  auto fBool = field("f_bool", arrow::boolean());
  auto fString = field("f_string", arrow::utf8());
  auto fBinary = field("f_binary", arrow::binary());
  auto fDecimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({fInt8, fInt16, fInt32, fInt64, fFloat, fDouble, fBinary});

  const std::vector<std::string> inputData = {
      "[1, 1]", "[1, 1]", "[1, 1]", "[1, 1]", "[3.5, 3.5]", "[1, 1]", R"(["abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(ColumnarToRowTest, Int_64_twoColumn) {
  const std::vector<std::string> inputData = {
      "[1, 2]",
      "[1, 2]",
  };

  auto fInt64Col0 = field("f_int64", arrow::int64());
  auto fInt64Col1 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({fInt64Col0, fInt64Col1});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(ColumnarToRowTest, Buffer_int8_int16) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());

  std::cout << "---------verify f_int8, f_int16---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({fInt8, fInt16});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_int32_int64) {
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int32, f_int64---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({fInt32, fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_float_double) {
  auto fFloat = field("f_float", arrow::float32());
  auto fDouble = field("f_double", arrow::float64());

  std::cout << "---------verify f_float, f_double---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[1.0, 2.0]",
      "[1.0, 2.0]",
  };

  auto schema = arrow::schema({fFloat, fDouble});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   64};
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_bool_binary) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fBinary = field("f_binary", arrow::binary());

  std::cout << "---------verify f_bool, f_binary---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[false, true]",
      R"(["aa", "bb"])",
  };

  auto schema = arrow::schema({fBool, fBinary});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_decimal_string) {
  auto fDecimal = field("f_decimal128", arrow::decimal(10, 2));
  auto fString = field("f_string", arrow::utf8());

  std::cout << "---------verify f_decimal, f_string---------" << std::endl;
  const std::vector<std::string> inputData = {R"(["1.00", "2.00"])", R"(["aa", "bb"])"};

  auto schema = arrow::schema({fDecimal, fString});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 24, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_int64_int64_with_null) {
  auto fInt64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int64, f_int64 with null ---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[null,2]",
      "[null,2]",
  };

  auto schema = arrow::schema({fInt64, fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_string) {
  auto fBinary = field("f_binary", arrow::binary());
  auto fString = field("f_string", arrow::utf8());

  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> inputData = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({fString});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 97, 97, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 98, 98, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, Buffer_bool) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fBinary = field("f_binary", arrow::binary());

  std::cout << "---------verify f_bool---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[false, true]",

  };

  auto schema = arrow::schema({fBool});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  std::shared_ptr<ArrowColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ArrowColumnarToRowConverter>(inputBatch, getDefaultArrowMemoryPool());

  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  };
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(ColumnarToRowTest, _allTypes) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());
  auto fDouble = field("f_double", arrow::float64());
  auto fFloat = field("f_float", arrow::float32());
  auto fBool = field("f_bool", arrow::boolean());
  auto fString = field("f_string", arrow::utf8());
  auto fBinary = field("f_binary", arrow::binary());
  auto fDecimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({fBool, fInt8, fInt16, fInt32, fInt64, fFloat, fDouble, fBinary, fDecimal});

  const std::vector<std::string> inputData = {
      "[true, true]",
      "[1, 1]",
      "[1, 1]",
      "[1, 1]",
      "[1, 1]",
      "[3.5, 3.5]",
      "[1, 1]",
      R"(["abc", "abc"])",
      R"(["100.00", "100.00"])"};

  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(ColumnarToRowTest, _allTypes_18rows) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());
  auto fDouble = field("f_double", arrow::float64());
  auto fFloat = field("f_float", arrow::float32());
  auto fBool = field("f_bool", arrow::boolean());
  auto fString = field("f_string", arrow::utf8());
  auto fBinary = field("f_binary", arrow::binary());
  auto fDecimal = field("f_decimal128", arrow::decimal(10, 2));

  auto schema = arrow::schema({fBool, fInt8, fInt16, fInt32, fInt64, fFloat, fDouble, fBinary, fDecimal});

  const std::vector<std::string> inputData = {
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

  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(ColumnarToRowTest, tooMuchColumn) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());

  std::vector<std::shared_ptr<Field>> fields;
  std::vector<std::string> data;
  for (int i = 0; i < 300; i++) {
    fields.emplace_back(fInt64);
    data.emplace_back("[" + std::to_string(i) + "]");
  }

  auto schema = arrow::schema(fields);
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(data, schema, &inputBatch);
  testRecordBatchEqual(inputBatch);
}
} // namespace gluten
