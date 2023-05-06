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
#include "operators/c2r/ArrowColumnarToRowConverter.h"
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
class VeloxRowToColumnarTest : public ::testing::Test, public test::VectorTestBase {
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
    // std::cout << vp->toString() << std::endl;
    // std::cout << vp->toString(0, 10) << std::endl;
    velox::test::assertEqualVectors(row, vp);
  }

 private:
  std::shared_ptr<velox::memory::MemoryPool> veloxPool_ = getDefaultVeloxLeafMemoryPool();
  std::shared_ptr<arrow::MemoryPool> arrowPool_ = getDefaultArrowMemoryPool();
};

TEST_F(VeloxRowToColumnarTest, Int_64) {
  const std::vector<std::string> inputData = {"[1, 2, 3]"};

  auto fInt64 = field("f_int64", arrow::int64());
  auto schema = arrow::schema({fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);
  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, basic) {
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

TEST_F(VeloxRowToColumnarTest, Int_64_twoColumn) {
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

TEST_F(VeloxRowToColumnarTest, Buffer_int8_int16) {
  auto fInt8 = field("f_int8_a", arrow::int8());
  auto fInt16 = field("f_int16", arrow::int16());

  const std::vector<std::string> inputData = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({fInt8, fInt16});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_int32_int64) {
  auto fInt32 = field("f_int32", arrow::int32());
  auto fInt64 = field("f_int64", arrow::int64());

  const std::vector<std::string> inputData = {
      "[1, 2]",
      "[1, 2]",
  };

  auto schema = arrow::schema({fInt32, fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_float_double) {
  auto fFloat = field("f_float", arrow::float32());
  auto fDouble = field("f_double", arrow::float64());

  const std::vector<std::string> inputData = {
      "[1.0, 2.0]",
      "[1.0, 2.0]",
  };

  auto schema = arrow::schema({fFloat, fDouble});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_bool_binary) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fBinary = field("f_binary", arrow::binary());

  const std::vector<std::string> inputData = {
      "[false, true]",
      R"(["aa", "bb"])",
  };

  auto schema = arrow::schema({fBool, fBinary});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, decimalTest) {
  auto fString = field("f_string", arrow::utf8());
  auto fShortDecimal = field("f_decimal_short_128", arrow::decimal(10, 2));
  auto fLongDecimal = field("f_decimal_long_128", arrow::decimal(20, 2));
  const std::vector<std::string> inputData = {R"(["aa", "bb"])", R"(["-1.01", "2.95"])", R"(["-1.01", "2.95"])"};

  auto schema = arrow::schema({fString, fShortDecimal, fLongDecimal});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_int64_int64_with_null) {
  auto fInt64 = field("f_int64", arrow::int64());

  const std::vector<std::string> inputData = {
      "[null,2]",
      "[null,2]",
  };

  auto schema = arrow::schema({fInt64, fInt64});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_string) {
  auto fBinary = field("f_binary", arrow::binary());
  auto fString = field("f_string", arrow::utf8());

  const std::vector<std::string> inputData = {R"(["aa", "bb"])"};

  auto schema = arrow::schema({fString});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, Buffer_bool) {
  auto fBool = field("f_bool", arrow::boolean());
  auto fBinary = field("f_binary", arrow::binary());

  const std::vector<std::string> inputData = {
      "[false, true]",

  };

  auto schema = arrow::schema({fBool});
  std::shared_ptr<arrow::RecordBatch> inputBatch;
  makeInputBatch(inputData, schema, &inputBatch);

  testRecordBatchEqual(inputBatch);
}

TEST_F(VeloxRowToColumnarTest, _allTypes) {
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

TEST_F(VeloxRowToColumnarTest, _allTypes_18rows) {
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
