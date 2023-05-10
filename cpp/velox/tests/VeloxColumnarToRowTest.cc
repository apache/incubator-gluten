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
  RowVectorPtr recordBatch2VeloxRowVector(const arrow::RecordBatch& rb) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    ASSERT_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
    auto vp = velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::getDefaultVeloxLeafMemoryPool().get());
    return std::dynamic_pointer_cast<velox::RowVector>(vp);
  }

  void testRecordBatchEqual(std::shared_ptr<arrow::RecordBatch> inputBatch) {
    auto row = recordBatch2VeloxRowVector(*inputBatch);
    auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool_, veloxPool_);
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
    // std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
    velox::test::assertEqualVectors(row, vp);
  }

 private:
  std::shared_ptr<velox::memory::MemoryPool> veloxPool_ = getDefaultVeloxLeafMemoryPool();
  std::shared_ptr<arrow::MemoryPool> arrowPool_ = getDefaultArrowMemoryPool();
};

TEST_F(VeloxColumnarToRowTest, timestamp) {
  // only RowVector can convert to RecordBatch
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0}, Timestamp{12, 0}, Timestamp{0, 17'123'456}, Timestamp{1, 17'123'456}, Timestamp{-1, 17'123'456}};
  auto row = makeRowVector({
      makeFlatVector<Timestamp>(timeValues),
  });
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto converter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

  jniAssertOkOrThrow(
      converter->init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  jniAssertOkOrThrow(converter->write(), "Native convert columnar to row: ColumnarToRowConverter write failed");
}

TEST_F(VeloxColumnarToRowTest, Int_64) {
  const std::vector<std::string> inputData = {"[1, 2, 3]"};

  auto fInt64 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);
  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxColumnarToRowTest, basic) {
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

TEST_F(VeloxColumnarToRowTest, Int_64_twoColumn) {
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

TEST_F(VeloxColumnarToRowTest, Buffer_int8_int16) {
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

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

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

TEST_F(VeloxColumnarToRowTest, Buffer_int32_int64) {
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

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
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

TEST_F(VeloxColumnarToRowTest, Buffer_float_double) {
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

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
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

TEST_F(VeloxColumnarToRowTest, Buffer_bool_binary) {
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

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
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

TEST_F(VeloxColumnarToRowTest, Buffer_decimal_string) {
  auto fString = field("f_string", arrow::utf8());

  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> inputData = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({fString});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->init());
  GLUTEN_THROW_NOT_OK(columnarToRowConverter->write());

  uint8_t* address = columnarToRowConverter->getBufferAddress();

  uint8_t expectArr[] = {0,  0,  0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0,
                         97, 97, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0};
  for (int i = 0; i < sizeof(expectArr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t) * (address + i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t) * (expectArr + i) << std::endl;
    ASSERT_EQ(*(address + i), *(expectArr + i));
  }
}

TEST_F(VeloxColumnarToRowTest, Buffer_int64_int64_with_null) {
  auto fInt64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int64, f_int64 with null ---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[null,2]",
      "[null,2]",
  };

  auto schema = arrow::schema({fInt64, fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
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

TEST_F(VeloxColumnarToRowTest, Buffer_string) {
  auto fBinary = field("f_binary", arrow::binary());
  auto fString = field("f_string", arrow::utf8());

  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> inputData = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({fString});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);
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

TEST_F(VeloxColumnarToRowTest, Buffer_bool) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fBinary = field("f_binary", arrow::binary());

  std::cout << "---------verify f_bool---------" << std::endl;
  const std::vector<std::string> inputData = {
      "[false, true]",

  };

  auto schema = arrow::schema({fBool});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  auto row = recordBatch2VeloxRowVector(*inputBatch);
  auto arrowPool = getDefaultArrowMemoryPool();
  auto veloxPool = getDefaultVeloxLeafMemoryPool();
  auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

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

TEST_F(VeloxColumnarToRowTest, _allTypes) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());
  auto fDouble = field("f_double", arrow::float64());
  auto fFloat = field("f_float", arrow::float32());
  auto fBool = field("f_bool", arrow::boolean());
  auto fString = field("f_string", arrow::utf8());
  auto fBinary = field("f_binary", arrow::binary());

  auto schema = arrow::schema({fBool, fInt8, fInt16, fInt32, fInt64, fFloat, fDouble, fBinary});

  const std::vector<std::string> inputData = {
      "[true, true]", "[1, 1]", "[1, 1]", "[1, 1]", "[1, 1]", "[3.5, 3.5]", "[1, 1]", R"(["abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxColumnarToRowTest, _allTypes_18rows) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());
  auto fDouble = field("f_double", arrow::float64());
  auto fFloat = field("f_float", arrow::float32());
  auto fBool = field("f_bool", arrow::boolean());
  auto fString = field("f_string", arrow::utf8());
  auto fBinary = field("f_binary", arrow::binary());

  auto schema = arrow::schema({fBool, fInt8, fInt16, fInt32, fInt64, fFloat, fDouble, fBinary});

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
      R"(["abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);
  testRecordBatchEqual(inputBatch);
}
} // namespace gluten
